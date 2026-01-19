/*
Copyright 2020 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"

	"k8s.io/apimachinery/pkg/types"
	kwait "k8s.io/apimachinery/pkg/util/wait"
	cloudprovider "k8s.io/cloud-provider"
	volerr "k8s.io/cloud-provider/volume/errors"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/util"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient"
	azcache "sigs.k8s.io/cloud-provider-azure/pkg/cache"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

const (
	// Disk Caching is not supported for disks 4 TiB and larger
	// https://docs.microsoft.com/en-us/azure/virtual-machines/premium-storage-performance#disk-caching
	diskCachingLimit = 4096 // GiB

	maxLUN                 = 64 // max number of LUNs per VM
	errStatusCode400       = "statuscode=400"
	errInvalidParameter    = `code="invalidparameter"`
	errTargetInstanceIDs   = `target="instanceids"`
	sourceSnapshot         = "snapshot"
	sourceVolume           = "volume"
	attachDiskMapKeySuffix = "attachdiskmap"
	detachDiskMapKeySuffix = "detachdiskmap"
	detachInProgress       = "detach in progress"

	// default initial delay in milliseconds for batch disk attach/detach
	defaultAttachDetachInitialDelayInMs = 1000

	// default detach operation timeout
	defaultDetachOperationMinTimeoutInSeconds = 240

	// default timeout for VMSS detach operation before polling on GET VM to verify detach status
	defaultVMSSDetachTimeoutInSeconds = 20

	// WriteAcceleratorEnabled support for Azure Write Accelerator on Azure Disks
	// https://docs.microsoft.com/azure/virtual-machines/windows/how-to-enable-write-accelerator
	WriteAcceleratorEnabled = "writeacceleratorenabled"

	// see https://docs.microsoft.com/en-us/rest/api/compute/disks/createorupdate#create-a-managed-disk-by-copying-a-snapshot.
	diskSnapshotPath = "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/snapshots/%s"

	// see https://docs.microsoft.com/en-us/rest/api/compute/disks/createorupdate#create-a-managed-disk-from-an-existing-managed-disk-in-the-same-or-different-subscription.
	managedDiskPath = "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s"
)

var defaultBackOff = kwait.Backoff{
	Steps:    7,
	Duration: 2 * time.Second,
	Factor:   1.5,
	Jitter:   0.0,
}

type controllerCommon struct {
	diskStateMap  sync.Map // <diskURI, attaching/detaching state>
	lockMap       *lockMap
	cloud         *provider.Cloud
	clientFactory azclient.ClientFactory
	// disk queue that is waiting for attach or detach on specific node
	// <nodeName, map<diskURI, *provider.AttachDiskOptions/DetachDiskOptions>>
	attachDiskMap sync.Map
	detachDiskMap sync.Map
	// DisableDiskLunCheck whether disable disk lun check after disk attach/detach
	DisableDiskLunCheck bool
	// DetachOperationMinTimeoutInSeconds determines timeout for waiting on detach operation before a force detach
	DetachOperationMinTimeoutInSeconds int
	// AttachDetachInitialDelayInMs determines initial delay in milliseconds for batch disk attach/detach
	AttachDetachInitialDelayInMs int
	// VMSSDetachTimeoutInSeconds determines timeout for polling on VMSS detach operation before polling on GET VM to verify detach status
	// This timeout should be lower than DetachOperationMinTimeoutInSeconds and cover most detach operations
	VMSSDetachTimeoutInSeconds int
	ForceDetachBackoff         bool
	WaitForDetach              bool
	CheckDiskCountForBatching  bool
	// a timed cache for disk attach hitting max data disk count, <nodeName, "">
	hitMaxDataDiskCountCache azcache.Resource
}

// ExtendedLocation contains additional info about the location of resources.
type ExtendedLocation struct {
	// Name - The name of the extended location.
	Name string `json:"name,omitempty"`
	// Type - The type of the extended location.
	Type string `json:"type,omitempty"`
}

// AttachDisk attaches a disk to vm
// occupiedLuns is used to avoid conflict with other disk attach in k8s VolumeAttachments
// return (lun, error)
func (c *controllerCommon) AttachDisk(ctx context.Context, diskName, diskURI string, nodeName types.NodeName,
	cachingMode armcompute.CachingTypes, disk *armcompute.Disk, occupiedLuns []int) (int32, error) {
	diskEncryptionSetID := ""
	writeAcceleratorEnabled := false

	// there is possibility that disk is nil when GetDisk is throttled
	// don't check disk state when GetDisk is throttled
	if disk != nil {
		if disk.ManagedBy != nil && (disk.Properties == nil || disk.Properties.MaxShares == nil || *disk.Properties.MaxShares <= 1) {
			vmset, err := c.cloud.GetNodeVMSet(ctx, nodeName, azcache.CacheReadTypeUnsafe)
			if err != nil {
				return -1, err
			}
			klog.V(4).Infof("found disk(%s) is already attached to node %s", diskURI, *disk.ManagedBy)
			attachedNode, err := vmset.GetNodeNameByProviderID(ctx, *disk.ManagedBy)
			if err != nil {
				return -1, err
			}
			if strings.EqualFold(string(nodeName), string(attachedNode)) {
				klog.Warningf("volume %s is actually attached to current node %s, invalidate vm cache and return disk lun", diskURI, nodeName)
				// update VM(invalidate vm cache)
				if errUpdate := c.UpdateVM(ctx, nodeName); errUpdate != nil {
					return -1, errUpdate
				}
				lun, _, err := c.GetDiskLun(ctx, diskName, diskURI, nodeName)
				return lun, err
			}

			attachErr := fmt.Sprintf(
				"disk(%s) already attached to node(%s), could not be attached to node(%s)",
				diskURI, *disk.ManagedBy, nodeName)
			klog.V(2).Infof("found dangling volume %s attached to node %s, could not be attached to node(%s)", diskURI, attachedNode, nodeName)
			return -1, volerr.NewDanglingError(attachErr, attachedNode, "")
		}

		if disk.Properties != nil {
			if disk.Properties.DiskSizeGB != nil && *disk.Properties.DiskSizeGB >= diskCachingLimit && cachingMode != armcompute.CachingTypesNone {
				// Disk Caching is not supported for disks 4 TiB and larger
				// https://docs.microsoft.com/en-us/azure/virtual-machines/premium-storage-performance#disk-caching
				cachingMode = armcompute.CachingTypesNone
				klog.Warningf("size of disk(%s) is %dGB which is bigger than limit(%dGB), set cacheMode as None",
					diskURI, *disk.Properties.DiskSizeGB, diskCachingLimit)
			}

			if disk.Properties.Encryption != nil &&
				disk.Properties.Encryption.DiskEncryptionSetID != nil {
				diskEncryptionSetID = *disk.Properties.Encryption.DiskEncryptionSetID
			}

			if disk.Properties.DiskState != nil && *disk.Properties.DiskState != armcompute.DiskStateUnattached && (disk.Properties.MaxShares == nil || *disk.Properties.MaxShares <= 1) {
				return -1, fmt.Errorf("state of disk(%s) is %s, not in expected %s state", diskURI, *disk.Properties.DiskState, armcompute.DiskStateUnattached)
			}
		}
		if disk.SKU != nil && disk.SKU.Name != nil && *disk.SKU.Name == armcompute.DiskStorageAccountTypesPremiumV2LRS {
			klog.V(2).Infof("disk(%s) is PremiumV2LRS and only supports None caching mode", diskURI)
			cachingMode = armcompute.CachingTypesNone
		}

		if v, ok := disk.Tags[WriteAcceleratorEnabled]; ok {
			if v != nil && strings.EqualFold(*v, "true") {
				writeAcceleratorEnabled = true
			}
		}
	}

	options := provider.AttachDiskOptions{
		Lun:                     -1,
		DiskName:                diskName,
		CachingMode:             armcompute.CachingTypes(cachingMode),
		DiskEncryptionSetID:     diskEncryptionSetID,
		WriteAcceleratorEnabled: writeAcceleratorEnabled,
	}
	node := strings.ToLower(string(nodeName))
	diskuri := strings.ToLower(diskURI)
	requestNum, err := c.batchAttachDiskRequest(diskuri, node, &options)
	if err != nil {
		return -1, err
	}

	var waitForDetachHappened bool
	if c.WaitForDetach && c.isMaxDataDiskCountExceeded(ctx, string(nodeName)) {
		// wait for disk detach to finish first on the same node
		if err = kwait.PollUntilContextTimeout(ctx, 2*time.Second, 30*time.Second, true, func(context.Context) (bool, error) {
			detachDiskReqeustNum, err := c.getDetachDiskRequestNum(node)
			if err != nil {
				return false, err
			}
			if detachDiskReqeustNum == 0 {
				return true, nil
			}
			klog.V(4).Infof("there are still %d detach disk requests on node %s, wait for detach to finish, current disk: %s", detachDiskReqeustNum, node, diskName)
			waitForDetachHappened = true
			return false, nil
		}); err != nil {
			klog.Errorf("current disk: %s, wait for detach disk requests on node %s failed: %v", diskName, node, err)
		}
	}

	c.lockMap.LockEntry(node)
	defer c.lockMap.UnlockEntry(node)

	if !waitForDetachHappened && c.AttachDetachInitialDelayInMs > 0 && requestNum == 1 {
		klog.V(2).Infof("wait %dms for more requests on node %s, current disk attach: %s", c.AttachDetachInitialDelayInMs, node, diskURI)
		time.Sleep(time.Duration(c.AttachDetachInitialDelayInMs) * time.Millisecond)
	}

	numDisksAllowed := math.MaxInt
	if c.CheckDiskCountForBatching {
		_, instanceType, err := GetNodeInfoFromLabels(ctx, string(nodeName), c.cloud.KubeClient)
		if err != nil {
			klog.Errorf("failed to get node info from labels: %v", err)
		} else if instanceType != "" {
			maxNumDisks, instanceExists := GetMaxDataDiskCount(instanceType)
			if instanceExists {
				attachedDisks, _, err := c.GetNodeDataDisks(ctx, nodeName, azcache.CacheReadTypeDefault)
				if err != nil {
					return -1, err
				}
				numDisksAttached := len(attachedDisks)
				if int(maxNumDisks) > numDisksAttached {
					numDisksAllowed = int(maxNumDisks) - numDisksAttached
				} else {
					numDisksAllowed = 0
				}
			}
		}
	}

	diskMap, err := c.retrieveAttachBatchedDiskRequests(node, diskuri)
	if err != nil {
		return -1, err
	}

	if len(diskMap) == 0 {
		// disk was already processed in the batch, return the result
		return c.verifyAttach(ctx, diskName, diskURI, nodeName)
	}

	// Remove some disks from the batch if the number is more than the max number of disks allowed
	removeDisks := len(diskMap) - numDisksAllowed
	if removeDisks > 0 {
		klog.V(2).Infof("too many disks to attach, remove %d disks from the request", removeDisks)
		for diskURI, options := range diskMap {
			if removeDisks == 0 {
				break
			}
			if options != nil {
				klog.V(2).Infof("remove disk(%s) from attach request from node(%s)", diskURI, nodeName)
				delete(diskMap, diskURI)
			}
			removeDisks--
		}
	}

	lun, setLunErr := c.SetDiskLun(ctx, nodeName, diskuri, diskMap, occupiedLuns)
	if setLunErr != nil {
		return -1, setLunErr
	}

	vmset, err := c.cloud.GetNodeVMSet(ctx, nodeName, azcache.CacheReadTypeUnsafe)
	if err != nil {
		return -1, err
	}
	c.diskStateMap.Store(disk, "attaching")
	defer c.diskStateMap.Delete(disk)

	defer func() {
		// invalidate the cache if there is error in disk attach
		if err != nil {
			_ = vmset.DeleteCacheForNode(ctx, string(nodeName))
		}
	}()

	klog.V(2).Infof("Trying to attach volume %s lun %d to node %s, diskMap len:%d, %+v", diskURI, lun, nodeName, len(diskMap), diskMap)
	err = vmset.AttachDisk(ctx, nodeName, diskMap)
	if err != nil {
		if strings.Contains(err.Error(), util.MaximumDataDiskExceededMsg) {
			klog.Warningf("hit max data disk count when attaching disk %s, set cache for node(%s)", diskName, nodeName)
			c.hitMaxDataDiskCountCache.Set(node, "")
		}
		if strings.Contains(err.Error(), "OperationPreempted") {
			klog.Errorf("Retry VM Update on node (%s) due to error (%v)", nodeName, err)
			err = vmset.UpdateVM(ctx, nodeName)
		}
		if err != nil {
			return -1, err
		}
	}

	if !c.DisableDiskLunCheck {
		// always check disk lun after disk attach complete
		return c.verifyAttach(ctx, diskName, diskURI, nodeName)
	}
	return lun, nil
}

// insertAttachDiskRequest return (attachDiskRequestQueueLength, error)
func (c *controllerCommon) batchAttachDiskRequest(diskURI, nodeName string, options *provider.AttachDiskOptions) (int, error) {
	var diskMap map[string]*provider.AttachDiskOptions
	attachDiskMapKey := nodeName + attachDiskMapKeySuffix
	c.lockMap.LockEntry(attachDiskMapKey)
	defer c.lockMap.UnlockEntry(attachDiskMapKey)
	v, ok := c.attachDiskMap.Load(nodeName)
	if ok {
		if diskMap, ok = v.(map[string]*provider.AttachDiskOptions); !ok {
			return -1, fmt.Errorf("convert attachDiskMap failure on node(%s)", nodeName)
		}
	} else {
		diskMap = make(map[string]*provider.AttachDiskOptions)
		c.attachDiskMap.Store(nodeName, diskMap)
	}
	// insert attach disk request to queue
	_, ok = diskMap[diskURI]
	if ok {
		klog.V(2).Infof("azureDisk - duplicated attach disk(%s) request on node(%s)", diskURI, nodeName)
	} else {
		diskMap[diskURI] = options
	}
	return len(diskMap), nil
}

// clean up attach disk requests
// return original attach disk requests
func (c *controllerCommon) retrieveAttachBatchedDiskRequests(nodeName, diskURI string) (map[string]*provider.AttachDiskOptions, error) {
	var diskMap map[string]*provider.AttachDiskOptions

	attachDiskMapKey := nodeName + attachDiskMapKeySuffix
	c.lockMap.LockEntry(attachDiskMapKey)
	defer c.lockMap.UnlockEntry(attachDiskMapKey)
	nodeAttachRequests, ok := c.attachDiskMap.Load(nodeName)
	if !ok {
		return diskMap, nil
	}
	if diskMap, ok = nodeAttachRequests.(map[string]*provider.AttachDiskOptions); !ok {
		return diskMap, fmt.Errorf("convert attachDiskMap failure on node(%s)", nodeName)
	}
	if _, ok = diskMap[diskURI]; !ok {
		klog.V(2).Infof("no attach disk(%s) request on node(%s), diskMap len:%d, %+v", diskURI, nodeName, len(diskMap), diskMap)
		return nil, nil
	}
	c.attachDiskMap.Store(nodeName, make(map[string]*provider.AttachDiskOptions))
	return diskMap, nil
}

// DetachDisk detaches a disk from VM
func (c *controllerCommon) DetachDisk(ctx context.Context, diskName, diskURI string, nodeName types.NodeName) error {
	if _, err := c.cloud.InstanceID(ctx, nodeName); err != nil {
		if errors.Is(err, cloudprovider.InstanceNotFound) {
			// if host doesn't exist, no need to detach
			klog.Warningf("azureDisk - failed to get azure instance id(%s), DetachDisk(%s) will assume disk is already detached",
				nodeName, diskURI)
			return nil
		}
		klog.Warningf("failed to get azure instance id (%v)", err)
		return fmt.Errorf("failed to get azure instance id for node %q: %w", nodeName, err)
	}

	vmset, err := c.cloud.GetNodeVMSet(ctx, nodeName, azcache.CacheReadTypeUnsafe)
	if err != nil {
		return err
	}

	node := strings.ToLower(string(nodeName))
	formattedDiskURI := strings.ToLower(diskURI)
	batchCount, err := c.batchDetachDiskRequest(diskName, formattedDiskURI, node)
	if err != nil {
		return err
	}

	c.lockMap.LockEntry(node)
	defer c.lockMap.UnlockEntry(node)

	if c.AttachDetachInitialDelayInMs > 0 && batchCount == 1 {
		klog.V(2).Infof("wait %dms for more requests on node %s, current disk detach: %s", c.AttachDetachInitialDelayInMs, node, diskURI)
		time.Sleep(time.Duration(c.AttachDetachInitialDelayInMs) * time.Millisecond)
	}

	diskMap, err := c.retrieveDetachBatchedDiskRequests(node, formattedDiskURI)
	if err != nil {
		return err
	}

	if len(diskMap) == 0 {
		// disk was already processed in the batch, return the result
		// returns success when the disk has toBeDetached flag set to true
		// we could consider issuing a force detach if the disk is stuck in ToBeDetached state for too long
		return c.verifyDetach(ctx, diskName, diskURI, nodeName)
	}

	klog.V(2).Infof("Trying to detach volume %s from node %s, diskMap len:%d, %s", diskURI, nodeName, len(diskMap), diskMap)
	c.diskStateMap.Store(formattedDiskURI, "detaching")
	defer c.diskStateMap.Delete(formattedDiskURI)

	// Calculate the timeout for the regular detach operation. If the operation fails or times out,
	// we need an active context to issue a force detach if necessary.
	// This timeout determines how long the CSI driver will wait before attempting a force detach.
	detachContextTimeout := time.Duration(c.DetachOperationMinTimeoutInSeconds) * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		halfRemaining := remaining / 2
		if halfRemaining > detachContextTimeout {
			detachContextTimeout = halfRemaining
		}
	}
	detachCtx, cancel := context.WithTimeout(ctx, detachContextTimeout)
	defer cancel()

	// vmssDetachTimeout is the timeout where we expect most VMSS detach operations to finish within
	vmssDetachTimeout := time.Duration(c.VMSSDetachTimeoutInSeconds) * time.Second
	if vmssDetachTimeout == 0 || vmssDetachTimeout > detachContextTimeout {
		vmssDetachTimeout = detachContextTimeout
		klog.V(2).Infof("reset vmssDetachTimeout as detachContextTimeout %v", detachContextTimeout)
	}

	// earlyExitCtx provides an initial timeout for VMSS detach operations that covers most cases.
	// If the detach doesn't complete within this timeout, we stop waiting for the operation result
	// and instead switch to polling the VM's data disk list directly via GET requests.
	// This approach is critical during zone outages or other scenarios where disks may detach
	// outside the CSI driver's control. By polling the actual VM state, we can confirm if the
	// disk was successfully detached and avoid unnecessary force detach operations that could impact VM stability.
	earlyExitCtx, earlyCancel := context.WithTimeout(detachCtx, vmssDetachTimeout)
	defer earlyCancel()

	if err = vmset.DetachDisk(earlyExitCtx, nodeName, diskMap, false); err != nil {
		if isInstanceNotFoundError(err) {
			// if host doesn't exist, no need to detach
			klog.Warningf("azureDisk - got InstanceNotFoundError(%v), DetachDisk(%s) will assume disk is already detached",
				err, diskURI)
			return nil
		}

		// if operation is preempted by a force operation on VM, check if the disk got detached before returning error
		if preemptedByForceOperation(err) {
			klog.Errorf("azureDisk - DetachDisk(%s) from node %s was preempted by a concurrent force detach operation", diskName, nodeName)
			return c.verifyDetach(ctx, diskName, diskURI, nodeName)
		}

		// continue polling on GET VM to verify detach status
		if errors.Is(err, context.DeadlineExceeded) {
			klog.Errorf("azureDisk - DetachDisk(%s) from node %s timed out after %v", diskName, nodeName, vmssDetachTimeout)
			err = c.pollForDetachCompletion(detachCtx, diskName, diskURI, nodeName, vmset)
		}

		if err != nil && c.ForceDetachBackoff {
			klog.Errorf("azureDisk - DetachDisk(%s) from node %s failed with error: %v, retry with force detach", diskName, nodeName, err)
			err = vmset.DetachDisk(ctx, nodeName, diskMap, true)
		}
	}

	// return the detach result directly if the lun cannot be checked
	if err != nil {
		klog.Errorf("azureDisk - detach disk(%s) failed, err: %v", diskName, err)
		return err
	}

	if !c.DisableDiskLunCheck {
		// always check disk lun after disk detach complete
		return c.verifyDetach(ctx, diskName, diskURI, nodeName)
	}

	klog.V(2).Infof("azureDisk - detach disk(%s) succeeded", diskName)
	return nil
}

// UpdateVM updates a vm
func (c *controllerCommon) UpdateVM(ctx context.Context, nodeName types.NodeName) error {
	vmset, err := c.cloud.GetNodeVMSet(ctx, nodeName, azcache.CacheReadTypeUnsafe)
	if err != nil {
		return err
	}
	node := strings.ToLower(string(nodeName))
	c.lockMap.LockEntry(node)
	defer c.lockMap.UnlockEntry(node)

	defer func() {
		_ = vmset.DeleteCacheForNode(ctx, string(nodeName))
	}()

	klog.V(2).Infof("azureDisk - update: vm(%s)", nodeName)
	return vmset.UpdateVM(ctx, nodeName)
}

// verifyDetach verifies if the disk is detached from the node by checking the disk lun.
func (c *controllerCommon) verifyDetach(ctx context.Context, diskName, diskURI string, nodeName types.NodeName) error {
	// verify if the disk is detached
	lun, vmState, errGetLun := c.GetDiskLun(ctx, diskName, diskURI, nodeName)
	if errGetLun != nil {
		if strings.Contains(errGetLun.Error(), consts.CannotFindDiskLUN) {
			klog.V(2).Infof("azureDisk - detach disk(%s, %s) succeeded", diskName, diskURI)
			return nil
		}
		klog.Errorf("azureDisk - detach disk(%s, %s) failed, error: %v", diskName, diskURI, errGetLun)
		return errGetLun
	}
	return fmt.Errorf("disk(%s) is still attached to node(%s) on lun(%d), vmState: %s", diskURI, nodeName, lun, ptr.Deref(vmState, ""))
}

// pollForDetachCompletion polls on GET VM to verify detach status
func (c *controllerCommon) pollForDetachCompletion(detachCtx context.Context, diskName, diskURI string, nodeName types.NodeName, vmset provider.VMSet) error {
	// poll on GET VM to verify detach status
	return kwait.ExponentialBackoffWithContext(detachCtx, defaultBackOff, func(_ context.Context) (bool, error) {
		_ = vmset.DeleteCacheForNode(detachCtx, string(nodeName))
		_, _, errGetLun := c.GetDiskLun(detachCtx, diskName, diskURI, nodeName)
		if errGetLun != nil {
			if strings.Contains(errGetLun.Error(), consts.CannotFindDiskLUN) {
				klog.V(2).Infof("polling on GET VM: detach disk(%s) succeeded", diskName)
				return true, nil
			}
			if strings.Contains(errGetLun.Error(), detachInProgress) {
				klog.V(2).Infof("polling on GET VM: detach disk(%s) is in progress", diskName)
				return false, nil
			}
			// if request throttled or other error occurred, we will return error to stop polling
			klog.Errorf("polling on GET VM: detach disk(%s) failed, error: %v", diskName, errGetLun)
			return false, errGetLun
		}
		klog.V(2).Infof("polling on GET VM: detach disk(%s) is still in progress", diskName)
		return false, nil
	})
}

// verifyAttach verifies if the disk is attached to the node by checking the disk lun.
func (c *controllerCommon) verifyAttach(ctx context.Context, diskName, diskURI string, nodeName types.NodeName) (int32, error) {
	lun, vmState, errGetLun := c.GetDiskLun(ctx, diskName, diskURI, nodeName)
	if errGetLun != nil {
		return -1, fmt.Errorf("disk(%s) could not be found on node(%s), vmState: %s, error: %w", diskURI, nodeName, ptr.Deref(vmState, ""), errGetLun)
	}
	klog.V(2).Infof("azureDisk - verify attach disk(%s, %s) succeeded on lun(%d)", diskName, diskURI, lun)
	return lun, nil
}

// batchDetachDiskRequest return (detachDiskRequestQueueLength, error)
func (c *controllerCommon) batchDetachDiskRequest(diskName, diskURI, nodeName string) (int, error) {
	var diskMap map[string]string
	detachDiskMapKey := nodeName + detachDiskMapKeySuffix
	c.lockMap.LockEntry(detachDiskMapKey)
	defer c.lockMap.UnlockEntry(detachDiskMapKey)
	nodeDetachRequests, ok := c.detachDiskMap.Load(nodeName)
	if ok {
		if diskMap, ok = nodeDetachRequests.(map[string]string); !ok {
			return -1, fmt.Errorf("convert detachDiskMap failure on node(%s)", nodeName)
		}
	} else {
		// if no detach disk requests for the node, create a new map
		diskMap = make(map[string]string)
		c.detachDiskMap.Store(nodeName, diskMap)
	}

	// check if the diskURI is already in the detach disk request queue
	_, ok = diskMap[diskURI]
	if ok {
		klog.V(2).Infof("azureDisk - duplicated detach disk(%s) request on node(%s)", diskURI, nodeName)
	} else {
		// insert detach disk request to queue
		diskMap[diskURI] = diskName
	}
	return len(diskMap), nil
}

// retrieveDetachBatchedDiskRequests removes the current detach disk requests for the node
// and returns it for processing
func (c *controllerCommon) retrieveDetachBatchedDiskRequests(nodeName, diskURI string) (map[string]string, error) {
	var diskMap map[string]string

	detachDiskMapKey := nodeName + detachDiskMapKeySuffix
	c.lockMap.LockEntry(detachDiskMapKey)
	defer c.lockMap.UnlockEntry(detachDiskMapKey)

	// load all detach disk requests for the node
	nodeDetachRequests, ok := c.detachDiskMap.Load(nodeName)
	if !ok {
		return nil, nil
	}
	if diskMap, ok = nodeDetachRequests.(map[string]string); !ok {
		return nil, fmt.Errorf("convert detachDiskMap failure on node(%s)", nodeName)
	}
	if _, ok = diskMap[diskURI]; !ok {
		klog.V(2).Infof("no detach disk(%s) request on node(%s), diskMap len:%d, %+v", diskURI, nodeName, len(diskMap), diskMap)
		return nil, nil
	}
	// clean up the detach disk requests for the node as they are being processed
	c.detachDiskMap.Store(nodeName, make(map[string]string))
	return diskMap, nil
}

func (c *controllerCommon) getDetachDiskRequestNum(nodeName string) (int, error) {
	detachDiskMapKey := nodeName + detachDiskMapKeySuffix
	c.lockMap.LockEntry(detachDiskMapKey)
	defer c.lockMap.UnlockEntry(detachDiskMapKey)
	v, ok := c.detachDiskMap.Load(nodeName)
	if !ok {
		return 0, nil
	}
	if diskMap, ok := v.(map[string]string); ok {
		return len(diskMap), nil
	}
	return -1, fmt.Errorf("convert detachDiskMap failure on node(%s)", nodeName)
}

// GetNodeDataDisks invokes vmSet interfaces to get data disks for the node.
func (c *controllerCommon) GetNodeDataDisks(ctx context.Context, nodeName types.NodeName, crt azcache.AzureCacheReadType) ([]*armcompute.DataDisk, *string, error) {
	vmset, err := c.cloud.GetNodeVMSet(ctx, nodeName, crt)
	if err != nil {
		return nil, nil, err
	}

	return vmset.GetDataDisks(ctx, nodeName, crt)
}

// GetDiskLun finds the lun on the host that the vhd is attached to, given a vhd's diskName and diskURI.
func (c *controllerCommon) GetDiskLun(ctx context.Context, diskName, diskURI string, nodeName types.NodeName) (int32, *string, error) {
	// GetNodeDataDisks need to fetch the cached data/fresh data if cache expired here
	// to ensure we get LUN based on latest entry.
	disks, provisioningState, err := c.GetNodeDataDisks(ctx, nodeName, azcache.CacheReadTypeDefault)
	if err != nil {
		klog.Errorf("error of getting data disks for node %s: %v", nodeName, err)
		return -1, provisioningState, err
	}

	for _, disk := range disks {
		if disk.Lun != nil && (disk.Name != nil && diskName != "" && strings.EqualFold(*disk.Name, diskName)) ||
			(disk.Vhd != nil && disk.Vhd.URI != nil && diskURI != "" && strings.EqualFold(*disk.Vhd.URI, diskURI)) ||
			(disk.ManagedDisk != nil && strings.EqualFold(*disk.ManagedDisk.ID, diskURI)) {
			if disk.ToBeDetached != nil && *disk.ToBeDetached {
				klog.Warningf("azureDisk - found disk(ToBeDetached): lun %d name %s uri %s", *disk.Lun, diskName, diskURI)
				return -1, provisioningState, fmt.Errorf("disk(%s) is in ToBeDetached state on node(%s): %s", diskURI, nodeName, detachInProgress)
			}
			// found the disk
			klog.V(2).Infof("azureDisk - found disk: lun %d name %s uri %s", *disk.Lun, diskName, diskURI)
			return *disk.Lun, provisioningState, nil

		}
	}
	return -1, provisioningState, fmt.Errorf("%s for disk %s", consts.CannotFindDiskLUN, diskName)
}

// SetDiskLun find unused luns and allocate lun for every disk in diskMap.
// occupiedLuns is used to avoid conflict with other disk attach in k8s VolumeAttachments
// Return lun of diskURI, -1 if all luns are used.
func (c *controllerCommon) SetDiskLun(ctx context.Context, nodeName types.NodeName, diskURI string, diskMap map[string]*provider.AttachDiskOptions, occupiedLuns []int) (int32, error) {
	disks, _, err := c.GetNodeDataDisks(ctx, nodeName, azcache.CacheReadTypeDefault)
	if err != nil {
		klog.Errorf("error of getting data disks for node %s: %v", nodeName, err)
		return -1, err
	}

	lun := int32(-1)
	_, isDiskInMap := diskMap[diskURI]
	used := make([]bool, maxLUN)
	for _, disk := range disks {
		if disk.Lun != nil {
			used[*disk.Lun] = true
			if !isDiskInMap {
				// find lun of diskURI since diskURI is not in diskMap
				if disk.ManagedDisk != nil && strings.EqualFold(*disk.ManagedDisk.ID, diskURI) {
					lun = *disk.Lun
				}
			}
		}
	}
	if !isDiskInMap && lun < 0 {
		return -1, fmt.Errorf("could not find disk(%s) in current disk list(len: %d) nor in diskMap(%v)", diskURI, len(disks), diskMap)
	}
	if len(diskMap) == 0 {
		// attach disk request is empty, return directly
		return lun, nil
	}

	for _, lun := range occupiedLuns {
		if lun >= 0 && lun <= maxLUN {
			used[int32(lun)] = true
		}
	}

	// allocate lun for every disk in diskMap
	var diskLuns []int32
	count := 0
	for k, v := range used {
		if !v {
			diskLuns = append(diskLuns, int32(k))
			count++
			if count >= len(diskMap) {
				break
			}
		}
	}

	if len(diskLuns) != len(diskMap) {
		return -1, fmt.Errorf("could not find enough disk luns(current: %d) for diskMap(%v, len=%d), diskURI(%s)",
			len(diskLuns), diskMap, len(diskMap), diskURI)
	}

	count = 0
	for uri, opt := range diskMap {
		if opt == nil {
			return -1, fmt.Errorf("unexpected nil pointer in diskMap(%v), diskURI(%s)", diskMap, diskURI)
		}
		if strings.EqualFold(uri, diskURI) {
			lun = diskLuns[count]
		}
		opt.Lun = diskLuns[count]
		count++
	}
	if lun < 0 {
		return lun, fmt.Errorf("could not find lun of diskURI(%s), diskMap(%v)", diskURI, diskMap)
	}
	return lun, nil
}

// isMaxDataDiskCountExceeded checks if the max data disk count is exceeded
func (c *controllerCommon) isMaxDataDiskCountExceeded(ctx context.Context, nodeName string) bool {
	_, err := c.hitMaxDataDiskCountCache.Get(ctx, nodeName, azcache.CacheReadTypeDefault)
	if err != nil {
		klog.Warningf("isMaxDataDiskCountExceeded(%s) return with error: %s", nodeName, err)
		return false
	}
	return true
}

func getValidCreationData(subscriptionID, resourceGroup string, options *ManagedDiskOptions) (armcompute.CreationData, error) {
	if options.SourceResourceID == "" {
		return armcompute.CreationData{
			CreateOption:    to.Ptr(armcompute.DiskCreateOptionEmpty),
			PerformancePlus: options.PerformancePlus,
		}, nil
	}

	sourceResourceID := options.SourceResourceID
	switch options.SourceType {
	case sourceSnapshot:
		if match := azureconstants.DiskSnapshotPathRE.FindString(sourceResourceID); match == "" {
			sourceResourceID = fmt.Sprintf(diskSnapshotPath, subscriptionID, resourceGroup, sourceResourceID)
		}

	case sourceVolume:
		if match := azureconstants.ManagedDiskPathRE.FindString(sourceResourceID); match == "" {
			sourceResourceID = fmt.Sprintf(managedDiskPath, subscriptionID, resourceGroup, sourceResourceID)
		}
	default:
		return armcompute.CreationData{
			CreateOption:    to.Ptr(armcompute.DiskCreateOptionEmpty),
			PerformancePlus: options.PerformancePlus,
		}, nil
	}

	splits := strings.Split(sourceResourceID, "/")
	if len(splits) > 9 {
		if options.SourceType == sourceSnapshot {
			return armcompute.CreationData{}, fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", sourceResourceID, azureconstants.DiskSnapshotPathRE)
		}
		return armcompute.CreationData{}, fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", sourceResourceID, azureconstants.ManagedDiskPathRE)
	}
	return armcompute.CreationData{
		CreateOption:     to.Ptr(armcompute.DiskCreateOptionCopy),
		SourceResourceID: &sourceResourceID,
		PerformancePlus:  options.PerformancePlus,
	}, nil
}

func isInstanceNotFoundError(err error) bool {
	errMsg := strings.ToLower(err.Error())
	if strings.Contains(errMsg, strings.ToLower(consts.VmssVMNotActiveErrorMessage)) {
		return true
	}
	return strings.Contains(errMsg, errStatusCode400) && strings.Contains(errMsg, errInvalidParameter) && strings.Contains(errMsg, errTargetInstanceIDs)
}

// preemptedByForceOperation checks if a regular detach operation was preempted by a concurrent force detach.
// Azure VMSS returns a DataDisksForceDetached error when a force detach operation is already in progress or completed,
// causing any pending regular detach operations to be cancelled and preempted.
func preemptedByForceOperation(err error) bool {
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, strings.ToLower("DataDisksForceDetached"))
}
