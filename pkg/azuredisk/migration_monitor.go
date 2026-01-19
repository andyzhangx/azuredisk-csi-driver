/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package azuredisk

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
)

const (
	// Migration monitoring constants
	progressReportingThreshold = 20 // Report every 20% completion

	ReasonSKUMigrationStarted   = "SKUMigrationStarted"
	ReasonSKUMigrationProgress  = "SKUMigrationProgress"
	ReasonSKUMigrationCompleted = "SKUMigrationCompleted"
	ReasonSKUMigrationTimeout   = "SKUMigrationTimeout"

	// Label for migration status on PVC
	LabelMigrationInProgress = "disk.csi.azure.com/migration-in-progress"

	// Volume size thresholds (in bytes)
	volumeSize2TB  = 2 * 1024 * 1024 * 1024 * 1024  // 2TB
	volumeSize4TB  = 4 * 1024 * 1024 * 1024 * 1024  // 4TB
	volumeSize16TB = 16 * 1024 * 1024 * 1024 * 1024 // 16TB
	volumeSize64TB = 64 * 1024 * 1024 * 1024 * 1024 // 64TB

	// Timeout durations based on volume size
	migrationTimeoutBelowTwoTB       = 10 * time.Hour // Below 2TB
	migrationTimeoutBelowFourTB      = 12 * time.Hour // 2TB to 4TB
	migrationTimeoutBelowSixteenTB   = 16 * time.Hour // 4TB to 16TB
	migrationTimeoutBelowSixtyFourTB = 19 * time.Hour // 16TB to 64TB
)

var (
	migrationCheckInterval = 60 * time.Second
	// Migration timeout map
	// Note: This is a global variable to allow overriding via environment variable
	// It is initialized in init() function
	// This allows for testing and flexibility in production environments
	migrationTimeouts = map[int64]time.Duration{
		volumeSize2TB:  migrationTimeoutBelowTwoTB,
		volumeSize4TB:  migrationTimeoutBelowFourTB,
		volumeSize16TB: migrationTimeoutBelowSixteenTB,
		volumeSize64TB: migrationTimeoutBelowSixtyFourTB,
	}

	// timeout array for small, medium and large volumes
	sortedMigrationSlabArray = []int64{
		volumeSize2TB,  // 2TB
		volumeSize4TB,  // 4TB
		volumeSize16TB, // 16TB
		volumeSize64TB, // 64TB
	}

	// Maximum migration timeout
	maxMigrationTimeout = 24 * time.Hour // Maximum allowed timeout for any migration
)

// getMigrationTimeout returns the appropriate timeout based on volume size
func getMigrationTimeout(volumeSize int64) time.Duration {
	for _, slab := range sortedMigrationSlabArray {
		if volumeSize < slab {
			if timeout, exists := migrationTimeouts[slab]; exists {
				return timeout
			}
			break // No more slabs to check
		}
	}
	return migrationTimeoutBelowSixteenTB
}

func initializeTimeouts() {
	// Use env variable if exists to override the maximum migration timeout mainly for testing purposes

	// overrides migrationTimeouts map in form of comma separated values like "2TB=5h,4TB=9h"
	if migrationTimeoutsEnv := os.Getenv("MIGRATION_TIMEOUTS"); migrationTimeoutsEnv != "" {
		// migration size array
		for _, pair := range strings.Split(migrationTimeoutsEnv, ",") {
			parts := strings.Split(pair, "=")
			if len(parts) != 2 {
				klog.Warningf("Invalid migration timeout format: %s", pair)
				continue
			}

			sizeStr := parts[0]
			timeoutStr := parts[1]

			var size resource.Quantity
			var timeout time.Duration
			var err error

			if size, err = resource.ParseQuantity(sizeStr); err != nil {
				klog.Warningf("Invalid migration timeout size: %s", sizeStr)
				continue
			}

			if timeout, err = time.ParseDuration(timeoutStr); err != nil {
				klog.Warningf("Invalid migration timeout duration: %s", timeoutStr)
				continue
			}

			migrationTimeouts[size.Value()] = timeout
			sortedMigrationSlabArray = append(sortedMigrationSlabArray, size.Value())
		}
		// sort the migration size array
		sort.Slice(sortedMigrationSlabArray, func(i, j int) bool {
			return sortedMigrationSlabArray[i] < sortedMigrationSlabArray[j]
		})

		klog.V(4).Infof("Sorted migration slab array: %v", sortedMigrationSlabArray)
	}

	if maxMigrationTimeoutEnv := os.Getenv("MAX_MIGRATION_TIMEOUT"); maxMigrationTimeoutEnv != "" {
		if duration, err := time.ParseDuration(maxMigrationTimeoutEnv); err == nil {
			maxMigrationTimeout = duration
		}
	}
}

func init() {
	initializeTimeouts()
}

// MigrationTask represents an active disk migration monitoring task
type MigrationTask struct {
	DiskURI              string
	PVName               string
	PVCName              string
	PVCNamespace         string
	FromSKU              string
	ToSKU                armcompute.DiskStorageAccountTypes
	StartTime            time.Time
	LastReportedProgress float32
	Context              context.Context
	CancelFunc           context.CancelFunc
	VolumeSize           int64         // Volume size in bytes
	MigrationTimeout     time.Duration // Calculated timeout based on volume size
	Timedout             bool          // Indicates if the task has timed out
	Cancelled            atomic.Bool   // Indicates if the task was cancelled
	PVLabeled            bool          // Indicates if the PV has been labelled for migration
	mutex                sync.RWMutex  // Ensures disk level migration details are accessed safely
}

// MigrationProgressMonitor monitors disk migration progress
type MigrationProgressMonitor struct {
	kubeClient     kubernetes.Interface
	eventRecorder  record.EventRecorder
	diskController *ManagedDiskController
	activeTasks    map[string]*MigrationTask
	mutex          sync.RWMutex // Ensures the activeTasks map is accessed safely
}

// NewMigrationProgressMonitor creates a new migration progress monitor
func NewMigrationProgressMonitor(kubeClient kubernetes.Interface, eventRecorder record.EventRecorder, diskController *ManagedDiskController) *MigrationProgressMonitor {
	return &MigrationProgressMonitor{
		kubeClient:     kubeClient,
		eventRecorder:  eventRecorder,
		diskController: diskController,
		activeTasks:    make(map[string]*MigrationTask),
	}
}

// StartMigrationMonitoring starts monitoring a disk migration with progress updates
func (m *MigrationProgressMonitor) StartMigrationMonitoring(ctx context.Context, isProvisioningFlow bool, diskURI, pvName string, fromSKU string, toSKU armcompute.DiskStorageAccountTypes, volumeSize int64) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Check if already monitoring this disk
	if _, exists := m.activeTasks[diskURI]; exists {
		klog.V(2).Infof("Migration monitoring already active for disk %s", diskURI)
		return nil
	}

	var pvcName, pvcNamespace string

	if !isProvisioningFlow && pvName != "" {
		// Get PV to find associated PVC
		pv, err := m.kubeClient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get PV %s: %v", pvName, err)
		}

		// Check if PV has a claim reference
		if pv.Spec.ClaimRef == nil {
			klog.V(2).Infof("PV %s has no claim reference, skipping migration monitoring", pvName)
			return nil
		}

		pvcName = pv.Spec.ClaimRef.Name
		pvcNamespace = pv.Spec.ClaimRef.Namespace
	}

	// Calculate timeout based on volume size
	migrationTimeout := getMigrationTimeout(volumeSize)

	// Create task context with calculated timeout
	taskCtx, cancelFunc := context.WithTimeout(context.Background(), migrationTimeout)

	task := &MigrationTask{
		DiskURI:              diskURI,
		PVName:               pvName,
		PVCName:              pvcName,
		PVCNamespace:         pvcNamespace,
		FromSKU:              fromSKU,
		ToSKU:                toSKU,
		StartTime:            time.Now(),
		LastReportedProgress: 0,
		Context:              taskCtx,
		CancelFunc:           cancelFunc,
		VolumeSize:           volumeSize,
		MigrationTimeout:     migrationTimeout,
		PVLabeled:            false,
	}

	task.mutex.Lock()
	defer task.mutex.Unlock()

	m.activeTasks[diskURI] = task

	// Add label to PV to track migration state and check if it was already present
	labelExisted, err := m.addMigrationLabelIfNotExists(ctx, pvName, fromSKU, toSKU)
	if err != nil {
		klog.Warningf("Failed to add migration label to PV %s: %v", pvName, err)
	} else {
		task.PVLabeled = true
	}

	// Start async monitoring
	go m.monitorMigrationProgress(task)

	// Log the timeout being used
	klog.V(2).Infof("Using migration timeout of %v for volume %s", migrationTimeout, pvName)

	// Only emit start event if label was not already present (new migration)
	if !isProvisioningFlow && !labelExisted {
		_ = m.emitMigrationEvent(task, corev1.EventTypeNormal, ReasonSKUMigrationStarted,
			fmt.Sprintf("Started monitoring SKU migration from %s to %s for volume %s (timeout: %v)", fromSKU, toSKU, pvName, migrationTimeout))
		klog.V(2).Infof("Started migration monitoring for disk %s (%s -> %s)", pvName, fromSKU, toSKU)
	} else {
		klog.V(2).Infof("Resumed migration monitoring for disk %s (%s -> %s)", pvName, fromSKU, toSKU)
	}

	return nil
}

// monitorMigrationProgress monitors the progress of a disk migration using PollUntilContextTimeout
func (m *MigrationProgressMonitor) monitorMigrationProgress(task *MigrationTask) {

	isPvcInfoEmpty := func() bool {
		task.mutex.RLock()
		defer task.mutex.RUnlock()
		return (task.PVCName == "" || task.PVCNamespace == "")
	}

	// Poll condition function
	pollCondition := func(_ context.Context) (bool, error) {
		if task.Cancelled.Load() {
			klog.Infof("Migration monitoring for disk %s cancelled", task.DiskURI)
			return true, nil // Stop polling if cancelled
		}

		// Check if we've exceeded max migration timeout
		if time.Since(task.StartTime) >= maxMigrationTimeout {
			_ = m.emitMigrationEvent(task, corev1.EventTypeWarning, ReasonSKUMigrationTimeout,
				fmt.Sprintf("Stopping monitoring the migration for the disk %s after waiting for %vh", task.DiskURI, maxMigrationTimeout.Hours()))
			klog.Warningf("Migration monitoring for disk %s cancelling after waiting for %vh", task.DiskURI, maxMigrationTimeout.Hours())
			return true, fmt.Errorf("maximum migration timeout exceeded")
		}

		// when a volume is created from a snapshot, the PV will be created by external provisioner
		// & so we could not have added the label from StartMigrationMonitoring(), so we will try here
		if !task.PVLabeled {
			_, err := m.addMigrationLabelIfNotExists(task.Context, task.PVName, task.FromSKU, task.ToSKU)
			if err != nil {
				klog.Warningf("Failed to add migration label to PV %s: %v", task.PVName, err)
			} else {
				task.mutex.Lock()
				task.PVLabeled = true
				task.mutex.Unlock()
			}
		}

		// During provisioning flow, we may not have PVC name and namespace in provisioning context,
		// so we would not have emitted the events
		if task.PVLabeled && isPvcInfoEmpty() {
			// Get PV to find associated PVC
			pv, err := m.kubeClient.CoreV1().PersistentVolumes().Get(task.Context, task.PVName, metav1.GetOptions{})
			if err == nil && pv.Spec.ClaimRef != nil {
				task.mutex.Lock()
				task.PVCName = pv.Spec.ClaimRef.Name
				task.PVCNamespace = pv.Spec.ClaimRef.Namespace
				task.mutex.Unlock()
				_ = m.emitMigrationEvent(task, corev1.EventTypeNormal, ReasonSKUMigrationStarted,
					fmt.Sprintf(
						"Started monitoring SKU migration from %s to %s for volume %s (timeout: %v)",
						task.FromSKU, task.ToSKU, task.PVName, task.MigrationTimeout))
				klog.V(2).Infof("Started migration monitoring for disk %s (%s -> %s)", task.PVName, task.FromSKU, task.ToSKU)
			}
		}

		// Check migration progress
		completed, err := m.checkMigrationProgress(task)
		if err != nil {
			klog.Warningf("Progress check error for disk %s (will retry): %v", task.DiskURI, err)
			return false, nil // Continue polling on transient errors
		}

		if completed {
			// Migration completed successfully
			if err := m.emitMigrationEvent(task, corev1.EventTypeNormal, ReasonSKUMigrationCompleted,
				fmt.Sprintf("Successfully completed SKU migration from %s to %s for volume %s (duration: %v)",
					task.FromSKU, task.ToSKU, task.PVName, time.Since(task.StartTime))); err != nil {
				klog.Errorf("Failed to emit completion event for disk %s: %v", task.DiskURI, err)
			}
			klog.V(2).Infof("Migration completed for disk %s in %v", task.DiskURI, time.Since(task.StartTime))

			// Remove label when migration completes
			if err := m.removeMigrationLabel(task.Context, task.PVName); err != nil {
				klog.Warningf("Failed to remove migration label from PV %s: %v", task.PVName, err)
			}
			return true, nil // Stop polling - migration completed
		}

		return false, nil // Continue polling
	}

	// Start polling with the migration timeout
	pollErr := wait.PollUntilContextTimeout(task.Context, migrationCheckInterval, task.MigrationTimeout, true, pollCondition)

	// Handle different timeout/error scenarios
	if pollErr != nil {
		// Check if this is the initial migration timeout (not max timeout)
		if (pollErr == context.DeadlineExceeded ||
			task.Context.Err() == context.DeadlineExceeded) && !task.Timedout {
			// Initial migration timeout exceeded, but continue monitoring until max timeout
			_ = m.emitMigrationEvent(task, corev1.EventTypeWarning, ReasonSKUMigrationTimeout,
				fmt.Sprintf("Migration taking too long (running %v hours) for volume %s", task.MigrationTimeout.Hours(), task.PVName))
			klog.Warningf("Migration taking too long (running %v hours) for disk %s", task.MigrationTimeout.Hours(), task.DiskURI)
			task.mutex.Lock()
			task.Timedout = true
			task.mutex.Unlock()

			// Continue monitoring until max timeout with buffer of 5 minutes to ensure the task is not prematurely cancelled
			remaining := maxMigrationTimeout - time.Since(task.StartTime) + (migrationCheckInterval * 5)
			if remaining > (migrationCheckInterval * 5) {
				// Create extended context and continue polling
				extendedCtx, extendedCancel := context.WithTimeout(context.Background(), remaining)
				defer extendedCancel()

				task.mutex.Lock()
				task.Context = extendedCtx
				task.CancelFunc = extendedCancel
				task.mutex.Unlock()

				// Continue polling with extended timeout
				extendedPollErr := wait.PollUntilContextTimeout(extendedCtx, migrationCheckInterval, remaining, true, pollCondition)
				if extendedPollErr != nil {
					if wait.Interrupted(extendedPollErr) {
						klog.Infof("Extended migration monitoring for disk %s cancelled", task.DiskURI)
					} else {
						klog.Errorf("Extended migration monitoring for disk %s failed: %v", task.DiskURI, extendedPollErr)
					}
				}
			}
		} else {
			// Other error scenarios
			klog.Errorf("Migration monitoring for disk %s failed with error: %v", task.DiskURI, pollErr)
		}
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.activeTasks, task.DiskURI)
	task.CancelFunc()
}

// checkMigrationProgress checks the current progress of a disk migration
func (m *MigrationProgressMonitor) checkMigrationProgress(task *MigrationTask) (bool, error) {
	// Get current disk state
	disk, err := m.diskController.GetDiskByURI(task.Context, task.DiskURI)
	if err != nil {
		return false, fmt.Errorf("failed to get disk %s: %v", task.DiskURI, err)
	}

	// Check completion percentage if available
	var completionPercent float32
	if disk.Properties != nil && disk.Properties.CompletionPercent != nil {
		completionPercent = *disk.Properties.CompletionPercent
	}

	// Report progress if significant milestone reached
	if m.shouldReportProgress(completionPercent, task.LastReportedProgress) {
		_ = m.emitMigrationEvent(task, corev1.EventTypeNormal, ReasonSKUMigrationProgress,
			fmt.Sprintf("Migration progress: %.1f%% complete for volume %s (elapsed: %v)",
				completionPercent, task.PVName, time.Since(task.StartTime)))
		task.mutex.Lock()
		task.LastReportedProgress = completionPercent
		task.mutex.Unlock()
		klog.V(2).Infof("Migration progress for disk %s: %.1f%% complete", task.DiskURI, completionPercent)
	}

	return completionPercent >= 100, nil
}

// shouldReportProgress determines if progress should be reported based on milestones
func (m *MigrationProgressMonitor) shouldReportProgress(current, last float32) bool {
	// Report at every 20% milestone
	currentMilestone := int(current/progressReportingThreshold) * progressReportingThreshold
	lastMilestone := int(last/progressReportingThreshold) * progressReportingThreshold

	return current < 100 && (currentMilestone > lastMilestone && currentMilestone > 0)
}

// emitMigrationEvent emits a Kubernetes event for the PersistentVolumeClaim
func (m *MigrationProgressMonitor) emitMigrationEvent(task *MigrationTask, eventType, reason, message string) error {
	if m.eventRecorder == nil || m.kubeClient == nil {
		klog.Warningf("Event recorder or kube client not available, skipping event: %s", message)
		return fmt.Errorf("event recorder or kube client not available")
	}

	// Get PersistentVolumeClaim object
	pvc, err := m.kubeClient.CoreV1().PersistentVolumeClaims(task.PVCNamespace).Get(task.Context, task.PVCName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("Failed to get PersistentVolumeClaim %s/%s for event emission: %v", task.PVCNamespace, task.PVCName, err)
		// if error is not found, lets exit the migration go routine
		if apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	// Emit event on the PersistentVolumeClaim
	m.eventRecorder.Event(pvc, eventType, reason, message)
	klog.V(4).Infof("Emitted event for PVC %s/%s: %s - %s", task.PVCNamespace, task.PVCName, reason, message)
	return nil
}

// addMigrationLabelIfNotExists adds migration label to the PersistentVolume if it doesn't already exist
// Returns true if label already existed, false if it was newly added
func (m *MigrationProgressMonitor) addMigrationLabelIfNotExists(ctx context.Context, pvName string, fromSKU string, toSKU armcompute.DiskStorageAccountTypes) (bool, error) {
	pv, err := m.kubeClient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}

	// Check if migration label already exists
	if pv.Labels != nil {
		if value, exists := pv.Labels[LabelMigrationInProgress]; exists && value == "true" {
			klog.V(2).Infof("Migration label already exists for PV %s (%s -> %s)", pvName, fromSKU, toSKU)
			return true, nil
		}
	}

	// Label doesn't exist, add it
	if pv.Labels == nil {
		pv.Labels = make(map[string]string)
	}

	pv.Labels[LabelMigrationInProgress] = "true"

	_, err = m.kubeClient.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	return false, err
}

// removeMigrationLabel removes migration label from the PersistentVolume
func (m *MigrationProgressMonitor) removeMigrationLabel(ctx context.Context, pvName string) error {
	pv, err := m.kubeClient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	delete(pv.Labels, LabelMigrationInProgress)

	_, err = m.kubeClient.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
	return err
}

// GetActiveMigrations returns currently active migration tasks
// GetActiveMigrations returns currently active migration tasks
func (m *MigrationProgressMonitor) GetActiveMigrations() map[string]*MigrationTask {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	result := make(map[string]*MigrationTask)
	for k, v := range m.activeTasks {
		// Create a new task with copied values instead of copying the entire struct
		v.mutex.RLock()
		taskCopy := &MigrationTask{
			DiskURI:              v.DiskURI,
			PVName:               v.PVName,
			PVCName:              v.PVCName,
			PVCNamespace:         v.PVCNamespace,
			FromSKU:              v.FromSKU,
			ToSKU:                v.ToSKU,
			StartTime:            v.StartTime,
			LastReportedProgress: v.LastReportedProgress,
			Context:              v.Context,
			CancelFunc:           v.CancelFunc,
			VolumeSize:           v.VolumeSize,
			MigrationTimeout:     v.MigrationTimeout,
			Timedout:             v.Timedout,
			// For atomic.Bool, create a new one with the current value
		}
		taskCopy.Cancelled.Store(v.Cancelled.Load())
		v.mutex.RUnlock()
		result[k] = taskCopy
	}
	return result
}

// IsMigrationActive checks if migration is currently being monitored for a disk
func (m *MigrationProgressMonitor) IsMigrationActive(diskURI string) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	_, exists := m.activeTasks[diskURI]
	return exists
}

// Stop stops all active migration monitoring tasks
func (m *MigrationProgressMonitor) Stop() {
	func() {
		m.mutex.RLock()
		tasks := m.activeTasks
		defer m.mutex.RUnlock()

		for _, task := range tasks {
			task.mutex.Lock()
			if task.CancelFunc != nil && !task.Cancelled.Load() {
				task.Cancelled.Store(true) // Mark task as cancelled
				task.CancelFunc()
				task.Context.Done() // Ensure context is cancelled
			}
			task.mutex.Unlock()
		}
	}()

	// Wait for up to 2 minutes for all tasks to be cleaned up
	ctx, cancel := context.WithTimeout(context.Background(), 2*migrationCheckInterval)
	defer cancel()

	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*migrationCheckInterval, true, func(ctx context.Context) (bool, error) {
		if ctx.Err() != nil {
			klog.V(2).Infof("Migration monitoring stop cancelled")
			return true, nil // This WILL stop the polling
		}
		m.mutex.RLock()
		tasksRemaining := len(m.activeTasks)
		m.mutex.RUnlock()

		if tasksRemaining == 0 {
			klog.V(2).Infof("All migration tasks have been cancelled")
			return true, nil // This WILL stop the polling
		}

		klog.V(4).Infof("Still waiting for %d migration tasks to finish", tasksRemaining)
		return false, nil // Continue polling
	})

	if err != nil {
		m.mutex.RLock()
		remaining := len(m.activeTasks)
		m.mutex.RUnlock()

		if remaining > 0 {
			klog.Warningf("Stop timeout: %d migration tasks still active", remaining)
		}
	}

	klog.V(2).Infof("Stopped all active migration monitoring tasks")
}

// Recovery function using labels
func (d *Driver) recoverMigrationMonitorsFromLabels(ctx context.Context) error {
	// List PVCs with migration label
	labelSelector := fmt.Sprintf("%s=true", LabelMigrationInProgress)
	pvList, err := d.cloud.KubeClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return err
	}

	recoveredCount := 0
	for _, pv := range pvList.Items {
		// Get the associated PVC
		if pv.Spec.ClaimRef == nil {
			klog.V(2).Infof("PV %s has no claim reference, skipping recovery", pv.Name)
			continue
		}

		// Check if it's an Azure disk CSI volume
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == d.Name {
			diskURI := pv.Spec.CSI.VolumeHandle

			// For recovery, we need to determine fromSKU and toSKU from the disk properties
			klog.V(3).Infof("Recovering migration monitor for PV: %s", pv.Name)

			// For now, we'll use placeholders - when more SKUs involve for migration, need to get this from disk properties
			fromSKU := string(armcompute.DiskStorageAccountTypesPremiumLRS) // we may incorrectly say PremiumLRS instead of StandardZRS, lets keep it simple for now
			toSKU := armcompute.DiskStorageAccountTypesPremiumV2LRS

			if pv.Spec.CSI.VolumeAttributes != nil {
				if sku, exists := azureutils.ParseDiskParametersForKey(pv.Spec.CSI.VolumeAttributes, "storageAccountType"); exists {
					if !strings.EqualFold(sku, string(fromSKU)) && !strings.EqualFold(sku, string(toSKU)) {
						continue
					}
				}
				if sku, exists := azureutils.ParseDiskParametersForKey(pv.Spec.CSI.VolumeAttributes, "skuName"); exists {
					if !strings.EqualFold(sku, string(fromSKU)) && !strings.EqualFold(sku, string(toSKU)) {
						continue
					}
				}
			}

			storageQtyVal := pv.Spec.Capacity[corev1.ResourceStorage]
			storageQty := &storageQtyVal
			volumeSizeInBytes := storageQty.Value()

			if err := d.migrationMonitor.StartMigrationMonitoring(ctx, false, diskURI, pv.Name, fromSKU, toSKU, volumeSizeInBytes); err != nil {
				klog.Errorf("Failed to recover migration for PV %s: %v", pv.Name, err)
			} else {
				recoveredCount++
			}
		}
	}

	klog.V(4).Infof("Recovered %d migration monitors from labels", recoveredCount)
	return nil
}
