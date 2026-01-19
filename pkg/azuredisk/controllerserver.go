/*
Copyright 2017 The Kubernetes Authors.

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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	cloudprovider "k8s.io/cloud-provider"
	volerr "k8s.io/cloud-provider/volume/errors"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	consts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/optimization"
	volumehelper "sigs.k8s.io/azuredisk-csi-driver/pkg/util"
	azureconsts "sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/metrics"
	azure "sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

const (
	waitForSnapshotReadyInterval     = 5 * time.Second
	waitForSnapshotReadyTimeout      = 10 * time.Minute
	maxErrMsgLength                  = 990
	checkDiskLunThrottleLatency      = 1 * time.Second
	maxSnapshotSizeDifferenceAllowed = 50 // in GiB
)

// listVolumeStatus explains the return status of `listVolumesByResourceGroup`
type listVolumeStatus struct {
	numVisited    int  // the number of iterated azure disks
	isCompleteRun bool // isCompleteRun is flagged true if the function iterated through all azure disks
	entries       []*csi.ListVolumesResponse_Entry
	err           error
}

// startSKUMigrationMonitor starts (idempotently) an asynchronous monitor that tracks
// migration of a managed disk from one SKU to another (currently only when moving
// to PremiumV2).
//
// Parameters:
//
//	ctx                - request context
//	isProvisioningFlow - true when invoked from CreateVolume (PV may not exist yet);
//	                     false when invoked from ModifyVolume (PV should already exist).
//	fromSKUStr         - original disk SKU name (string form, may differ in casing)
//	toSKU              - target armcompute.DiskStorageAccountTypes (must be PremiumV2LRS to proceed)
//	diskURI            - full Azure disk resource ID
//	pvName             - Kubernetes PV name if known (CreateVolume path supplies req.Name;
//	                     ModifyVolume path does not, so we derive it from diskURI)
//	sizeBytes          - disk size (used to derive migration timeout)
//
// Workflow / decision points:
// 1. Guard clauses:
//   - If no migration monitor is configured OR fromSKUStr empty: do nothing.
//   - If target SKU is not PremiumV2 OR already matches fromSKU: no migration needed.
//
// 2. PV name resolution:
//   - If pvName is empty (ModifyVolume path) attempt to parse diskURI and use its last
//     token (disk name) as the PV name.
//   - If the cluster used static provisioning with a PV name different from the Azure
//     disk name, this heuristic cannot map the disk to the PV; monitoring is skipped.
//
// 3. StartMonitoring call:
//   - Calls migrationMonitor.StartMigrationMonitoring(ctx, isProvisioningFlow, ...).
//   - isProvisioningFlow influences initial behavior inside the monitor:
//   - Provisioning (true): PV may not yet exist. The monitor records a task with
//     empty PVC metadata, attempts an initial label add (may fail if PV absent),
//     and defers PV / PVC discovery, labeling, and start event emission to the
//     periodic polling loop (monitorMigrationProgress).
//   - Modify (false): PV should exist. The monitor immediately fetches the PV,
//     derives PVC name/namespace, adds/ensures the migration label, and emits
//     the "Started" event (unless label already existed; then it logs a resume).
//
// 4. Label management:
//   - addMigrationLabelIfNotExists is executed once at start; if it fails (e.g. PV
//     not yet created) task.PVLabeled remains false. The polling loop will retry
//     labeling until successful.
//
// 5. Asynchronous monitoring:
//   - A goroutine watches progress (polling Azure), emitting events for start (if
//     deferred), incremental progress, completion, failure or timeout.
//
// 6. Error handling:
//   - Non‑fatal issues (parse failure, start error) are logged and abort initiation
//     for that disk; they do not propagate back to the CSI operation.
//
// Concurrency notes:
//   - This function itself is lightweight and only delegates; StartMigrationMonitoring
//     handles internal synchronization of task state.
//   - Idempotency: if monitoring is already active for diskURI, underlying monitor
//     short‑circuits.
//
// Returns: nothing; logs warnings on recoverable issues.
//
// NOTE: If future migrations support additional target SKUs, relax the toSKU check.
func (d *Driver) startSKUMigrationMonitor(
	ctx context.Context,
	isProvisioningFlow bool,
	fromSKUStr string,
	toSKU armcompute.DiskStorageAccountTypes,
	diskURI, pvName string,
	sizeBytes int64,
) {
	if d.migrationMonitor == nil || fromSKUStr == "" {
		return
	}

	if toSKU != armcompute.DiskStorageAccountTypesPremiumV2LRS || fromSKUStr == string(toSKU) {
		return
	}
	if pvName == "" {
		var parseErr error
		_, _, pvName, parseErr = azureutils.GetInfoFromURI(diskURI)
		if parseErr != nil {
			klog.Warningf("Skipping monitor, failed to extract pv name from URI %s: %v", diskURI, parseErr)
			return
		}
	}
	if err := d.migrationMonitor.StartMigrationMonitoring(
		ctx, isProvisioningFlow, diskURI, pvName, fromSKUStr, toSKU, sizeBytes); err != nil {
		klog.Warningf("failed to start SKU migration monitoring for %s: %v", diskURI, err)
	}
}

// CreateVolume provisions an azure disk
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := d.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		klog.Errorf("invalid create volume req: %v", req)
		return nil, err
	}
	params := make(map[string]string, len(req.GetParameters())+len(req.GetMutableParameters()))
	for k, v := range req.GetParameters() {
		params[k] = v
	}
	for k, v := range req.GetMutableParameters() {
		params[k] = v
	}
	diskParams, err := azureutils.ParseDiskParameters(params)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Failed parsing disk parameters: %v", err)
	}
	name := req.GetName()
	if len(name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Name must be provided")
	}
	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Volume capabilities must be provided")
	}

	if err := azureutils.IsValidVolumeCapabilities(volCaps, diskParams.MaxShares); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	isAdvancedPerfProfile := strings.EqualFold(diskParams.PerfProfile, consts.PerfProfileAdvanced)
	// If perfProfile is set to advanced and no/invalid device settings are provided, fail the request
	if d.getPerfOptimizationEnabled() && isAdvancedPerfProfile {
		if err := optimization.AreDeviceSettingsValid(consts.DummyBlockDevicePathLinux, diskParams.DeviceSettings); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	if acquired := d.volumeLocks.TryAcquire(name); !acquired {
		return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, name)
	}
	defer d.volumeLocks.Release(name)

	capacityBytes := req.GetCapacityRange().GetRequiredBytes()
	volSizeBytes := int64(capacityBytes)
	requestSizeToBeSupplied := true
	requestGiB := int(volumehelper.RoundUpGiB(volSizeBytes))

	if diskParams.PerformancePlus != nil && *diskParams.PerformancePlus && requestGiB < consts.PerformancePlusMinimumDiskSizeGiB {
		klog.Warningf("using PerformancePlus, increasing requested disk size from %vGiB to %vGiB (minimal size for PerformancePlus feature)", requestGiB, consts.PerformancePlusMinimumDiskSizeGiB)
		requestGiB = consts.PerformancePlusMinimumDiskSizeGiB
	}
	if requestGiB < consts.MinimumDiskSizeGiB {
		klog.Infof("increasing requested disk size from %vGiB to %vGiB (minimal disk size)", requestGiB, consts.MinimumDiskSizeGiB)
		requestGiB = consts.MinimumDiskSizeGiB
	}

	maxVolSize := int(volumehelper.RoundUpGiB(req.GetCapacityRange().GetLimitBytes()))
	if (maxVolSize > 0) && (maxVolSize < requestGiB) {
		return nil, status.Error(codes.InvalidArgument, "After round-up, volume size exceeds the limit specified")
	}

	localCloud := d.cloud
	localDiskController := d.diskController

	if diskParams.UserAgent != "" {
		localCloud, err = azureutils.GetCloudProviderFromClient(ctx, d.kubeClient, d.cloudConfigSecretName, d.cloudConfigSecretNamespace, diskParams.UserAgent,
			d.allowEmptyCloudConfig, d.enableTrafficManager, d.enableMinimumRetryAfter, d.trafficManagerPort)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "create cloud with UserAgent(%s) failed with: (%s)", diskParams.UserAgent, err)
		}
		localDiskController = &ManagedDiskController{
			controllerCommon: &controllerCommon{
				cloud:                     localCloud,
				lockMap:                   newLockMap(),
				DisableDiskLunCheck:       true,
				clientFactory:             localCloud.ComputeClientFactory,
				ForceDetachBackoff:        d.forceDetachBackoff,
				WaitForDetach:             d.waitForDetach,
				CheckDiskCountForBatching: d.checkDiskCountForBatching,
			},
		}
		localDiskController.AttachDetachInitialDelayInMs = int(d.attachDetachInitialDelayInMs)
		localDiskController.VMSSDetachTimeoutInSeconds = int(d.vmssDetachTimeoutInSeconds)
		localDiskController.DetachOperationMinTimeoutInSeconds = int(d.detachOperationMinTimeoutInSeconds)

	}
	if azureutils.IsAzureStackCloud(localCloud.Config.Cloud, localCloud.Config.DisableAzureStackCloud) {
		if diskParams.MaxShares > 1 {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Invalid maxShares value: %d as Azure Stack does not support shared disk.", diskParams.MaxShares))
		}
	}

	if diskParams.DiskName == "" {
		diskParams.DiskName = name
	}
	diskParams.DiskName = azureutils.CreateValidDiskName(diskParams.DiskName)

	if diskParams.ResourceGroup == "" {
		diskParams.ResourceGroup = d.cloud.ResourceGroup
	}

	// normalize values
	skuName, err := azureutils.NormalizeStorageAccountType(diskParams.AccountType, localCloud.Config.Cloud, localCloud.Config.DisableAzureStackCloud)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if _, err := azureutils.NormalizeCachingMode(diskParams.CachingMode); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if skuName == armcompute.DiskStorageAccountTypesPremiumV2LRS {
		// PremiumV2LRS only supports None caching mode
		azureutils.SetKeyValueInMap(diskParams.VolumeContext, consts.CachingModeField, string(v1.AzureDataDiskCachingNone))
	}

	if err := azureutils.ValidateDiskEncryptionType(diskParams.DiskEncryptionType); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	networkAccessPolicy, err := azureutils.NormalizeNetworkAccessPolicy(diskParams.NetworkAccessPolicy)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	publicNetworkAccess, err := azureutils.NormalizePublicNetworkAccess(diskParams.PublicNetworkAccess)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	diskZone := azureutils.PickAvailabilityZone(req.GetAccessibilityRequirements(), diskParams.Location, topologyKey)
	if diskParams.Location == "" {
		diskParams.Location = d.cloud.Location
		region := azureutils.GetRegionFromAvailabilityZone(diskZone)
		if region != "" && region != d.cloud.Location {
			klog.V(2).Infof("got a different region from zone %s for disk %s", diskZone, diskParams.DiskName)
			diskParams.Location = region
		}
	}
	accessibleTopology := []*csi.Topology{}

	if d.enableDiskCapacityCheck {
		if ok, err := d.checkDiskCapacity(ctx, diskParams.SubscriptionID, diskParams.ResourceGroup, diskParams.DiskName, requestGiB); !ok {
			return nil, err
		}
	}

	contentSource := &csi.VolumeContentSource{}

	if strings.EqualFold(diskParams.WriteAcceleratorEnabled, consts.TrueValue) {
		diskParams.Tags[azure.WriteAcceleratorEnabled] = consts.TrueValue
	}
	var sourceID, sourceType, sourceSKU string
	metricsRequest := "controller_create_volume"
	content := req.GetVolumeContentSource()
	if content != nil {
		if content.GetSnapshot() != nil {
			sourceID = content.GetSnapshot().GetSnapshotId()
			sourceType = consts.SourceSnapshot
			contentSource = &csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{
						SnapshotId: sourceID,
					},
				},
			}

			// Get snapshot once and extract both SKU and disk size info
			snapshot, err := d.getSnapshot(ctx, sourceID)
			if err == nil {
				// fetch snapshot info and compare size with requested size
				// when snapshot size is larger than requested size, do not supply the size in the request
				// to allow Azure to create a disk with exact snapshot size in bytes.
				diskSizeInBytes, err := getDiskSizeInBytesFromSnapshot(snapshot)
				if err == nil {
					requestedGiBfromSnapshot := int(volumehelper.RoundUpGiB(diskSizeInBytes))
					differenceSize := requestedGiBfromSnapshot - requestGiB
					if requestedGiBfromSnapshot > requestGiB && differenceSize <= maxSnapshotSizeDifferenceAllowed {
						klog.V(4).Infof("snapshot size (%d GiB) is larger than requested size (%d GiB) but difference (%d GiB) is within the allowed limit (%d GiB), will not supply the size in the create disk request", requestedGiBfromSnapshot, requestGiB, differenceSize, maxSnapshotSizeDifferenceAllowed)
						requestSizeToBeSupplied = false
					}
				}

				// Get SKU if migration monitoring is enabled and target SKU is PremiumV2LRS
				if skuName == armcompute.DiskStorageAccountTypesPremiumV2LRS && d.migrationMonitor != nil {
					sourceSKU, _ = getSnapshotSKUFromSnapshot(snapshot)
				}
			} else {
				return nil, status.Errorf(codes.NotFound, "%v", err)
			}

			metricsRequest = "controller_create_volume_from_snapshot"
		} else {
			sourceID = content.GetVolume().GetVolumeId()
			sourceType = consts.SourceVolume
			contentSource = &csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Volume{
					Volume: &csi.VolumeContentSource_VolumeSource{
						VolumeId: sourceID,
					},
				},
			}
			subsID, resourceGroup, diskName, err := azureutils.GetInfoFromURI(sourceID)
			if err != nil {
				return nil, status.Errorf(codes.NotFound, "%v", err)
			}
			sourceGiB, disk, err := d.GetSourceDiskSize(ctx, subsID, resourceGroup, diskName, 0, consts.SourceDiskSearchMaxDepth)
			if err == nil {
				if sourceGiB != nil && *sourceGiB < int32(requestGiB) {
					diskParams.VolumeContext[consts.ResizeRequired] = strconv.FormatBool(true)
					klog.V(2).Infof("source disk(%s) size(%d) is less than requested size(%d), set resizeRequired as true", sourceID, *sourceGiB, requestGiB)
				}
				if disk != nil && len(disk.Zones) == 1 {
					if disk.Zones[0] != nil {
						diskZone = fmt.Sprintf("%s-%s", diskParams.Location, *disk.Zones[0])
						klog.V(2).Infof("source disk(%s) is in zone(%s), set diskZone as %s", sourceID, *disk.Zones[0], diskZone)
					}
				}
				if d.migrationMonitor != nil && disk != nil && disk.SKU != nil {
					sourceSKU = string(*disk.SKU.Name)
				}
			} else {
				klog.Warningf("failed to get source disk(%s) size, err: %v", sourceID, err)
			}
			metricsRequest = "controller_create_volume_from_volume"
		}
	}

	if strings.HasSuffix(strings.ToLower(string(skuName)), "zrs") {
		klog.V(2).Infof("diskZone(%s) is reset as empty since disk(%s) is ZRS(%s)", diskZone, diskParams.DiskName, skuName)
		diskZone = ""
		// make volume scheduled on all 4 availability zones
		for i := 1; i <= 4; i++ {
			topology := &csi.Topology{
				Segments: map[string]string{topologyKey: fmt.Sprintf("%s-%d", diskParams.Location, i)},
			}
			accessibleTopology = append(accessibleTopology, topology)
		}
		// make volume scheduled on all non-zone nodes
		topology := &csi.Topology{
			Segments: map[string]string{topologyKey: ""},
		}
		accessibleTopology = append(accessibleTopology, topology)
	} else {
		accessibleTopology = []*csi.Topology{
			{
				Segments: map[string]string{topologyKey: diskZone},
			},
		}
	}

	klog.V(2).Infof("begin to create azure disk(%s) account type(%s) rg(%s) location(%s) size(%d) diskZone(%v) maxShares(%d)",
		diskParams.DiskName, skuName, diskParams.ResourceGroup, diskParams.Location, requestGiB, diskZone, diskParams.MaxShares)

	if skuName == armcompute.DiskStorageAccountTypesUltraSSDLRS {
		if diskParams.DiskIOPSReadWrite == "" && diskParams.DiskMBPSReadWrite == "" {
			// set default DiskIOPSReadWrite, DiskMBPSReadWrite per request size
			diskParams.DiskIOPSReadWrite = strconv.Itoa(getDefaultDiskIOPSReadWrite(requestGiB))
			diskParams.DiskMBPSReadWrite = strconv.Itoa(getDefaultDiskMBPSReadWrite(requestGiB))
			klog.V(2).Infof("set default DiskIOPSReadWrite as %s, DiskMBPSReadWrite as %s on disk(%s)", diskParams.DiskIOPSReadWrite, diskParams.DiskMBPSReadWrite, diskParams.DiskName)
		}
	}

	diskParams.VolumeContext[consts.RequestedSizeGib] = strconv.Itoa(requestGiB)

	if !requestSizeToBeSupplied && sourceType == consts.SourceSnapshot {
		requestGiB = 0
	}

	volumeOptions := &ManagedDiskOptions{
		AvailabilityZone:    diskZone,
		BurstingEnabled:     diskParams.EnableBursting,
		DiskEncryptionSetID: diskParams.DiskEncryptionSetID,
		DiskEncryptionType:  diskParams.DiskEncryptionType,
		DiskIOPSReadWrite:   diskParams.DiskIOPSReadWrite,
		DiskMBpsReadWrite:   diskParams.DiskMBPSReadWrite,
		DiskName:            diskParams.DiskName,
		LogicalSectorSize:   int32(diskParams.LogicalSectorSize),
		MaxShares:           int32(diskParams.MaxShares),
		ResourceGroup:       diskParams.ResourceGroup,
		SubscriptionID:      diskParams.SubscriptionID,
		SizeGB:              requestGiB,
		StorageAccountType:  skuName,
		SourceResourceID:    sourceID,
		SourceType:          sourceType,
		Tags:                diskParams.Tags,
		Location:            diskParams.Location,
		PerformancePlus:     diskParams.PerformancePlus,
	}

	volumeOptions.SkipGetDiskOperation = d.isGetDiskThrottled(ctx)
	// Azure Stack Cloud does not support NetworkAccessPolicy, PublicNetworkAccess
	if !azureutils.IsAzureStackCloud(localCloud.Config.Cloud, localCloud.Config.DisableAzureStackCloud) {
		volumeOptions.NetworkAccessPolicy = networkAccessPolicy
		volumeOptions.PublicNetworkAccess = publicNetworkAccess
		if diskParams.DiskAccessID != "" {
			volumeOptions.DiskAccessID = &diskParams.DiskAccessID
		}
	}

	var diskURI string
	mc := metrics.NewMetricContext(consts.AzureDiskCSIDriverName, metricsRequest, d.cloud.ResourceGroup, d.cloud.SubscriptionID, d.Name)
	isOperationSucceeded := false
	defer func() {
		mc.ObserveOperationWithResult(isOperationSucceeded, consts.VolumeID, diskURI)
	}()

	diskURI, err = localDiskController.CreateManagedDisk(ctx, volumeOptions)
	if err != nil {
		if strings.Contains(err.Error(), consts.NotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	// Start migration monitoring if enabled
	d.startSKUMigrationMonitor(ctx, true, sourceSKU, skuName, diskURI, req.Name, volSizeBytes)

	isOperationSucceeded = true
	klog.V(2).Infof("create azure disk(%s) account type(%s) rg(%s) location(%s) size(%d) tags(%s) successfully", diskParams.DiskName, skuName, diskParams.ResourceGroup, diskParams.Location, requestGiB, diskParams.Tags)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           diskURI,
			CapacityBytes:      volumehelper.GiBToBytes(int64(requestGiB)),
			VolumeContext:      diskParams.VolumeContext,
			ContentSource:      contentSource,
			AccessibleTopology: accessibleTopology,
		},
	}, nil
}

// DeleteVolume delete an azure disk
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in the request")
	}

	if err := d.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		return nil, status.Errorf(codes.Internal, "invalid delete volume req: %v", req)
	}
	diskURI := volumeID

	if !azureutils.IsARMResourceID(diskURI) {
		klog.Errorf("diskURI(%s) is not a valid ARM resource ID", diskURI)
		return &csi.DeleteVolumeResponse{}, nil
	}

	if acquired := d.volumeLocks.TryAcquire(volumeID); !acquired {
		return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, volumeID)
	}
	defer d.volumeLocks.Release(volumeID)

	mc := metrics.NewMetricContext(consts.AzureDiskCSIDriverName, "controller_delete_volume", d.cloud.ResourceGroup, d.cloud.SubscriptionID, d.Name)
	isOperationSucceeded := false
	defer func() {
		mc.ObserveOperationWithResult(isOperationSucceeded, consts.VolumeID, diskURI)
	}()

	klog.V(2).Infof("deleting azure disk(%s)", diskURI)
	err := d.diskController.DeleteManagedDisk(ctx, diskURI)
	klog.V(2).Infof("delete azure disk(%s) returned with %v", diskURI, err)
	isOperationSucceeded = (err == nil)
	return &csi.DeleteVolumeResponse{}, err
}

// ControllerGetVolume get volume
func (d *Driver) ControllerGetVolume(context.Context, *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerModifyVolume modify volume
func (d *Driver) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in the request")
	}

	if err := d.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_MODIFY_VOLUME); err != nil {
		return nil, status.Errorf(codes.Internal, "invalid modify volume req: %v", req)
	}

	diskURI := volumeID
	currentDisk, err := d.checkDiskExists(ctx, diskURI)
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Volume not found, failed with error: %v", err))
	}

	diskParams, err := azureutils.ParseDiskParameters(req.GetMutableParameters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Failed parsing disk parameters: %v", err)
	}

	// normalize values
	skuName, err := azureutils.NormalizeStorageAccountType(diskParams.AccountType, d.cloud.Config.Cloud, d.cloud.Config.DisableAzureStackCloud)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if diskParams.AccountType == "" {
		skuName = ""
	}

	// Check if this is a SKU migration
	var fromSKU armcompute.DiskStorageAccountTypes
	var monitorSKUMigration bool
	if currentDisk != nil && currentDisk.Properties != nil && d.migrationMonitor != nil {
		if currentDisk.SKU != nil && currentDisk.SKU.Name != nil {
			fromSKU = *currentDisk.SKU.Name
			monitorSKUMigration = skuName == armcompute.DiskStorageAccountTypesPremiumV2LRS && fromSKU == armcompute.DiskStorageAccountTypesPremiumLRS
		}

		// modifyVolume will be reattempted if controller restarts. In case if the controller restarted after updating disk sku to PremiumV2LRS & before we label,
		// we may not be able to detect the migration post restart, hence during re-attempt, we can check this condition and set monitorSKUMigration accordingly
		if !monitorSKUMigration {
			if currentDisk.Properties != nil && currentDisk.Properties.DiskSizeGB != nil &&
				currentDisk.Properties.CompletionPercent != nil && *currentDisk.Properties.CompletionPercent < float32(100.0) {
				monitorSKUMigration = fromSKU == armcompute.DiskStorageAccountTypesPremiumV2LRS
			}
		}
	}

	klog.V(2).Infof("begin to modify azure disk(%s) account type(%s) rg(%s) location(%s)",
		diskParams.DiskName, skuName, diskParams.ResourceGroup, diskParams.Location)

	volumeOptions := &ManagedDiskOptions{
		DiskIOPSReadWrite:  diskParams.DiskIOPSReadWrite,
		DiskMBpsReadWrite:  diskParams.DiskMBPSReadWrite,
		ResourceGroup:      diskParams.ResourceGroup,
		SubscriptionID:     diskParams.SubscriptionID,
		StorageAccountType: skuName,
		SourceResourceID:   diskURI,
		SourceType:         consts.SourceVolume,
	}

	mc := metrics.NewMetricContext(consts.AzureDiskCSIDriverName, "controller_modify_volume", d.cloud.ResourceGroup, d.cloud.SubscriptionID, d.Name)
	isOperationSucceeded := false
	defer func() {
		mc.ObserveOperationWithResult(isOperationSucceeded, consts.VolumeID, diskURI)
	}()

	if err = d.diskController.ModifyDisk(ctx, volumeOptions); err != nil {
		if strings.Contains(err.Error(), consts.NotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	isOperationSucceeded = true

	// Start migration monitoring if this is a SKU change
	if monitorSKUMigration {
		volSizeBytes := int64(*currentDisk.Properties.DiskSizeGB) * 1024 * 1024 * 1024
		d.startSKUMigrationMonitor(ctx, false, string(fromSKU), skuName, diskURI, "", volSizeBytes)
	}

	klog.V(2).Infof("modify azure disk(%s) account type(%s) rg(%s) location(%s) successfully", diskParams.DiskName, skuName, diskParams.ResourceGroup, diskParams.Location)

	return &csi.ControllerModifyVolumeResponse{}, err
}

// ControllerPublishVolume attach an azure disk to a required node
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	diskURI := req.GetVolumeId()
	if len(diskURI) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
	}

	caps := []*csi.VolumeCapability{volCap}
	maxShares, err := azureutils.GetMaxShares(req.GetVolumeContext())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "MaxShares value not supported")
	}

	if err := azureutils.IsValidVolumeCapabilities(caps, maxShares); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	disk, err := d.checkDiskExists(ctx, diskURI)
	if err != nil {
		if strings.Contains(err.Error(), "context deadline") {
			disk = nil
			klog.Warningf("checkDiskExists(%s) failed with %v, proceed to attach disk", diskURI, err)
		} else {
			return nil, status.Error(codes.NotFound, fmt.Sprintf("Volume not found, failed with error: %v", err))
		}
	}

	nodeID := req.GetNodeId()
	if len(nodeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Node ID not provided")
	}

	nodeName := types.NodeName(nodeID)
	_, _, diskName, err := azureutils.GetInfoFromURI(diskURI)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	mc := metrics.NewMetricContext(consts.AzureDiskCSIDriverName, "controller_publish_volume", d.cloud.ResourceGroup, d.cloud.SubscriptionID, d.Name)
	isOperationSucceeded := false
	defer func() {
		mc.ObserveOperationWithResult(isOperationSucceeded, consts.VolumeID, diskURI, consts.Node, string(nodeName))
	}()

	lun, vmState, err := d.diskController.GetDiskLun(ctx, diskName, diskURI, nodeName)
	if err == cloudprovider.InstanceNotFound {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("failed to get azure instance id for node %q (%v)", nodeName, err))
	}

	vmStateStr := "<nil>"
	if vmState != nil {
		vmStateStr = *vmState
	}

	klog.V(2).Infof("GetDiskLun returned: %v. Initiating attaching volume %s to node %s (vmState %s).", err, diskURI, nodeName, vmStateStr)

	volumeContext := req.GetVolumeContext()
	if volumeContext == nil {
		volumeContext = map[string]string{}
	}

	if err == nil {
		if vmState != nil && strings.ToLower(*vmState) == "failed" {
			klog.Warningf("VM(%s) is in failed state, update VM first", nodeName)
			if err := d.diskController.UpdateVM(ctx, nodeName); err != nil {
				return nil, status.Errorf(codes.Internal, "update instance %q failed with %v", nodeName, err)
			}
		}
		// Volume is already attached to node.
		klog.V(2).Infof("Attach operation is successful. volume %s is already attached to node %s at lun %d.", diskURI, nodeName, lun)
	} else {
		if !strings.Contains(err.Error(), azureconsts.CannotFindDiskLUN) {
			return nil, status.Errorf(codes.Internal, "could not get disk lun for volume %s: %v", diskURI, err)
		}
		var cachingMode armcompute.CachingTypes
		if cachingMode, err = azureutils.GetCachingMode(volumeContext); err != nil {
			return nil, status.Errorf(codes.Internal, "%v", err)
		}

		if d.convertRWCachingModeForIntreePV && cachingMode == armcompute.CachingTypesReadWrite {
			if strings.EqualFold(volumeContext[consts.KindField], string(v1.AzureManagedDisk)) {
				klog.V(2).Infof("converting RWCachingMode to ReadOnly for intree PV volume %s", diskURI)
				cachingMode = armcompute.CachingTypesReadOnly
			}
		}

		occupiedLuns := d.getOccupiedLunsFromNode(ctx, nodeName, diskURI)
		klog.V(2).Infof("Trying to attach volume %s to node %s", diskName, nodeName)

		attachDiskInitialDelay := azureutils.GetAttachDiskInitialDelay(volumeContext)
		if attachDiskInitialDelay > 0 {
			klog.V(2).Infof("attachDiskInitialDelayInMs is set to %d", attachDiskInitialDelay)
			d.diskController.AttachDetachInitialDelayInMs = attachDiskInitialDelay
		}
		lun, err = d.diskController.AttachDisk(ctx, diskName, diskURI, nodeName, cachingMode, disk, occupiedLuns)
		if err == nil {
			klog.V(2).Infof("Attach operation successful: volume %s attached to node %s.", diskName, nodeName)
		} else {
			if derr, ok := err.(*volerr.DanglingAttachError); ok {
				if strings.EqualFold(string(nodeName), string(derr.CurrentNode)) {
					err := status.Errorf(codes.Internal, "volume %s is actually attached to current node %s, return error", diskURI, nodeName)
					klog.Warningf("%v", err)
					return nil, err
				}
				klog.Warningf("volume %s is already attached to node %s, try detach first", diskURI, derr.CurrentNode)
				if err = d.diskController.DetachDisk(ctx, diskName, diskURI, derr.CurrentNode); err != nil {
					return nil, status.Errorf(codes.Internal, "Could not detach volume %s from node %s: %v", diskURI, derr.CurrentNode, err)
				}
				klog.V(2).Infof("Trying to attach volume %s to node %s again", diskName, nodeName)
				lun, err = d.diskController.AttachDisk(ctx, diskName, diskURI, nodeName, cachingMode, disk, occupiedLuns)
			}
			if err != nil {
				klog.Errorf("Attach volume %s to instance %s failed with %v", diskName, nodeName, err)
				errMsg := fmt.Sprintf("Attach volume %s to instance %s failed with %v", diskName, nodeName, err)
				if len(errMsg) > maxErrMsgLength {
					errMsg = errMsg[:maxErrMsgLength]
				}
				return nil, status.Errorf(codes.Internal, "%v", errMsg)
			}
		}
		klog.V(2).Infof("attach volume %s to node %s successfully", diskName, nodeName)
	}

	publishContext := map[string]string{consts.LUN: strconv.Itoa(int(lun))}
	if disk != nil {
		if _, ok := volumeContext[consts.RequestedSizeGib]; !ok {
			klog.V(6).Infof("found static PV(%s), insert disk properties to volumeattachments", diskURI)
			azureutils.InsertDiskProperties(disk, publishContext)
		}
	}
	isOperationSucceeded = true
	return &csi.ControllerPublishVolumeResponse{PublishContext: publishContext}, nil
}

// ControllerUnpublishVolume detach an azure disk from a required node
func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	diskURI := req.GetVolumeId()
	if len(diskURI) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	nodeID := req.GetNodeId()
	if len(nodeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Node ID not provided")
	}
	nodeName := types.NodeName(nodeID)

	_, _, diskName, err := azureutils.GetInfoFromURI(diskURI)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	mc := metrics.NewMetricContext(consts.AzureDiskCSIDriverName, "controller_unpublish_volume", d.cloud.ResourceGroup, d.cloud.SubscriptionID, d.Name)
	isOperationSucceeded := false
	defer func() {
		mc.ObserveOperationWithResult(isOperationSucceeded, consts.VolumeID, diskURI, consts.Node, string(nodeName))
	}()

	klog.V(2).Infof("Trying to detach volume %s from node %s", diskURI, nodeID)

	if err := d.diskController.DetachDisk(ctx, diskName, diskURI, nodeName); err != nil {
		if strings.Contains(err.Error(), consts.ErrDiskNotFound) {
			klog.Warningf("volume %s already detached from node %s", diskURI, nodeID)
		} else {
			klog.Errorf("Could not detach volume %s from node %s: %v", diskURI, nodeID, err)
			errMsg := fmt.Sprintf("Could not detach volume %s from node %s: %v", diskURI, nodeID, err)
			if len(errMsg) > maxErrMsgLength {
				errMsg = errMsg[:maxErrMsgLength]
			}
			return nil, status.Errorf(codes.Internal, "%v", errMsg)
		}
	}
	klog.V(2).Infof("detach volume %s from node %s successfully", diskURI, nodeID)
	isOperationSucceeded = true

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities return the capabilities of the volume
func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	diskURI := req.GetVolumeId()
	if len(diskURI) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in the request")
	}

	volumeCapabilities := req.GetVolumeCapabilities()
	if volumeCapabilities == nil {
		return nil, status.Error(codes.InvalidArgument, "VolumeCapabilities missing in the request")
	}

	params := req.GetParameters()
	maxShares, err := azureutils.GetMaxShares(params)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "MaxShares value not supported")
	}

	if err := azureutils.IsValidVolumeCapabilities(volumeCapabilities, maxShares); err != nil {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil
	}

	if _, err := d.checkDiskExists(ctx, diskURI); err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Volume not found, failed with error: %v", err))
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: volumeCapabilities,
		}}, nil
}

// getOccupiedLunsFromNode returns the occupied luns from node
func (d *Driver) getOccupiedLunsFromNode(ctx context.Context, nodeName types.NodeName, diskURI string) []int {
	var occupiedLuns []int
	if d.checkDiskLUNCollision && !d.isCheckDiskLunThrottled(ctx) {
		timer := time.AfterFunc(checkDiskLunThrottleLatency, func() {
			klog.Warningf("checkDiskLun(%s) on node %s took longer than %v, disable disk lun check temporarily", diskURI, nodeName, checkDiskLunThrottleLatency)
			d.checkDiskLunThrottlingCache.Set(consts.CheckDiskLunThrottlingKey, "")
		})
		now := time.Now()
		if usedLunsFromVA, err := d.getUsedLunsFromVolumeAttachments(ctx, string(nodeName)); err == nil {
			if len(usedLunsFromVA) > 0 {
				if usedLunsFromNode, err := d.getUsedLunsFromNode(ctx, nodeName); err == nil {
					occupiedLuns = volumehelper.GetElementsInArray1NotInArray2(usedLunsFromVA, usedLunsFromNode)
					if len(occupiedLuns) > 0 {
						klog.Warningf("node: %s, usedLuns from VolumeAttachments: %v, usedLuns from Node: %v, occupiedLuns: %v, disk: %s", nodeName, usedLunsFromVA, usedLunsFromNode, occupiedLuns, diskURI)
					} else {
						klog.V(6).Infof("node: %s, usedLuns from VolumeAttachments: %v, usedLuns from Node: %v, occupiedLuns: %v, disk: %s", nodeName, usedLunsFromVA, usedLunsFromNode, occupiedLuns, diskURI)
					}
				} else {
					klog.Warningf("getUsedLunsFromNode(%s, %s) failed with %v", nodeName, diskURI, err)
				}
			}
		} else {
			klog.Warningf("getUsedLunsFromVolumeAttachments(%s, %s) failed with %v", nodeName, diskURI, err)
		}
		latency := time.Since(now)
		if latency > checkDiskLunThrottleLatency {
			klog.Warningf("checkDiskLun(%s) on node %s took %v (limit: %v)", diskURI, nodeName, latency, checkDiskLunThrottleLatency)
		} else {
			timer.Stop() // cancel the timer
			klog.V(6).Infof("checkDiskLun(%s) on node %s took %v", diskURI, nodeName, latency)
		}
	}
	return occupiedLuns
}

// ControllerGetCapabilities returns the capabilities of the Controller plugin
func (d *Driver) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: d.Cap,
	}, nil
}

// GetCapacity returns the capacity of the total available storage pool
func (d *Driver) GetCapacity(_ context.Context, _ *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ListVolumes return all available volumes
func (d *Driver) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	start := 0
	if req.StartingToken != "" {
		var err error
		start, err = strconv.Atoi(req.StartingToken)
		if err != nil {
			return nil, status.Errorf(codes.Aborted, "ListVolumes starting token(%s) parsing with error: %v", req.StartingToken, err)
		}
		if start < 0 {
			return nil, status.Errorf(codes.Aborted, "ListVolumes starting token(%d) can not be negative", start)
		}
	}
	if d.cloud.KubeClient != nil && d.cloud.KubeClient.CoreV1() != nil && d.cloud.KubeClient.CoreV1().PersistentVolumes() != nil {
		klog.V(6).Infof("List Volumes in Cluster:")
		return d.listVolumesInCluster(ctx, start, int(req.MaxEntries))
	}
	klog.V(6).Infof("List Volumes in Node Resource Group: %s", d.cloud.ResourceGroup)
	return d.listVolumesInNodeResourceGroup(ctx, start, int(req.MaxEntries))
}

// listVolumesInCluster is a helper function for ListVolumes used for when there is an available kubeclient
func (d *Driver) listVolumesInCluster(ctx context.Context, start, maxEntries int) (*csi.ListVolumesResponse, error) {
	pvList, err := d.cloud.KubeClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ListVolumes failed while fetching PersistentVolumes List with error: %v", err)
	}

	// get all resource groups and put them into a sorted slice
	rgMap := make(map[string]bool)
	volSet := make(map[string]bool)
	for _, pv := range pvList.Items {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == d.Name {
			diskURI := pv.Spec.CSI.VolumeHandle
			_, rg, _, err := azureutils.GetInfoFromURI(diskURI)
			if err != nil {
				klog.Warningf("failed to get subscription id, resource group from disk uri (%s) with error(%v)", diskURI, err)
				continue
			}
			rg, diskURI = strings.ToLower(rg), strings.ToLower(diskURI)
			volSet[diskURI] = true
			if _, visited := rgMap[rg]; visited {
				continue
			}
			rgMap[rg] = true
		}
	}

	resourceGroups := make([]string, len(rgMap))
	i := 0
	for rg := range rgMap {
		resourceGroups[i] = rg
		i++
	}
	sort.Strings(resourceGroups)

	// loop through each resourceGroup to get disk lists
	entries := []*csi.ListVolumesResponse_Entry{}
	numVisited := 0
	isCompleteRun, startFound := true, false
	for _, resourceGroup := range resourceGroups {
		if !isCompleteRun || (maxEntries > 0 && len(entries) >= maxEntries) {
			isCompleteRun = false
			break
		}
		localStart := start - numVisited
		if startFound {
			localStart = 0
		}
		listStatus := d.listVolumesByResourceGroup(ctx, resourceGroup, entries, localStart, maxEntries-len(entries), volSet)
		numVisited += listStatus.numVisited
		if listStatus.err != nil {
			if status.Code(listStatus.err) == codes.FailedPrecondition {
				continue
			}
			return nil, listStatus.err
		}
		startFound = true
		entries = listStatus.entries
		isCompleteRun = isCompleteRun && listStatus.isCompleteRun
	}
	// if start was not found, start token was greater than total number of disks
	if start > 0 && !startFound {
		return nil, status.Errorf(codes.FailedPrecondition, "ListVolumes starting token(%d) is greater than total number of disks", start)
	}

	nextTokenString := ""
	if !isCompleteRun {
		nextTokenString = strconv.Itoa(start + numVisited)
	}

	listVolumesResp := &csi.ListVolumesResponse{
		Entries:   entries,
		NextToken: nextTokenString,
	}

	return listVolumesResp, nil
}

// listVolumesInNodeResourceGroup is a helper function for ListVolumes used for when there is no available kubeclient
func (d *Driver) listVolumesInNodeResourceGroup(ctx context.Context, start, maxEntries int) (*csi.ListVolumesResponse, error) {
	entries := []*csi.ListVolumesResponse_Entry{}
	listStatus := d.listVolumesByResourceGroup(ctx, d.cloud.ResourceGroup, entries, start, maxEntries, nil)
	if listStatus.err != nil {
		return nil, listStatus.err
	}

	nextTokenString := ""
	if !listStatus.isCompleteRun {
		nextTokenString = strconv.Itoa(listStatus.numVisited)
	}

	listVolumesResp := &csi.ListVolumesResponse{
		Entries:   listStatus.entries,
		NextToken: nextTokenString,
	}

	return listVolumesResp, nil
}

// listVolumesByResourceGroup is a helper function that updates the ListVolumeResponse_Entry slice and returns number of total visited volumes, number of volumes that needs to be visited and an error if found
func (d *Driver) listVolumesByResourceGroup(ctx context.Context, resourceGroup string, entries []*csi.ListVolumesResponse_Entry, start, maxEntries int, volSet map[string]bool) listVolumeStatus {
	diskClient := d.clientFactory.GetDiskClient()
	disks, derr := diskClient.List(ctx, resourceGroup)
	if derr != nil {
		return listVolumeStatus{err: status.Errorf(codes.Internal, "ListVolumes on rg(%s) failed with error: %v", resourceGroup, derr)}
	}
	// if volSet is initialized but is empty, return
	if volSet != nil && len(volSet) == 0 {
		return listVolumeStatus{
			numVisited:    len(disks),
			isCompleteRun: true,
			entries:       entries,
		}
	}
	if start > 0 && start >= len(disks) {
		return listVolumeStatus{
			numVisited: len(disks),
			err:        status.Errorf(codes.FailedPrecondition, "ListVolumes starting token(%d) on rg(%s) is greater than total number of volumes", start, d.cloud.ResourceGroup),
		}
	}
	if start < 0 {
		start = 0
	}
	i := start
	isCompleteRun := true
	// Loop until
	for ; i < len(disks); i++ {
		if maxEntries > 0 && len(entries) >= maxEntries {
			isCompleteRun = false
			break
		}

		disk := disks[i]
		// if given a set of volumes from KubeClient, only continue if the disk can be found in the set
		if volSet != nil && !volSet[strings.ToLower(*disk.ID)] {
			continue
		}
		// HyperVGeneration property is only setup for os disks. Only the non os disks should be included in the list
		if disk.Properties == nil || disk.Properties.HyperVGeneration == nil || *disk.Properties.HyperVGeneration == "" {
			nodeList := []string{}

			if disk.ManagedBy != nil {
				attachedNode, err := d.cloud.VMSet.GetNodeNameByProviderID(ctx, *disk.ManagedBy)
				if err != nil {
					return listVolumeStatus{err: err}
				}
				nodeList = append(nodeList, string(attachedNode))
			}

			entries = append(entries, &csi.ListVolumesResponse_Entry{
				Volume: &csi.Volume{
					VolumeId: *disk.ID,
				},
				Status: &csi.ListVolumesResponse_VolumeStatus{
					PublishedNodeIds: nodeList,
				},
			})
		}
	}
	return listVolumeStatus{
		numVisited:    i - start,
		isCompleteRun: isCompleteRun,
		entries:       entries,
	}
}

// ControllerExpandVolume controller expand volume
func (d *Driver) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in the request")
	}
	if err := d.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_EXPAND_VOLUME); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid expand volume request: %v", req)
	}

	capacityBytes := req.GetCapacityRange().GetRequiredBytes()
	if capacityBytes == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capacity range missing in request")
	}
	requestSize := *resource.NewQuantity(capacityBytes, resource.BinarySI)

	diskURI := req.GetVolumeId()
	result, rerr := d.diskController.GetDiskByURI(ctx, diskURI)
	if rerr != nil {
		return nil, status.Errorf(codes.Internal, "GetDiskByURI(%s) failed with error(%v)", diskURI, rerr)
	}
	if result == nil || result.Properties == nil || result.Properties.DiskSizeGB == nil {
		return nil, status.Errorf(codes.Internal, "could not get size of the disk(%s)", diskURI)
	}
	oldSize := *resource.NewQuantity(int64(*result.Properties.DiskSizeGB), resource.BinarySI)

	mc := metrics.NewMetricContext(consts.AzureDiskCSIDriverName, "controller_expand_volume", d.cloud.ResourceGroup, d.cloud.SubscriptionID, d.Name)
	isOperationSucceeded := false
	defer func() {
		mc.ObserveOperationWithResult(isOperationSucceeded, consts.VolumeID, diskURI)
	}()

	klog.V(2).Infof("begin to expand azure disk(%s) with new size(%d)", diskURI, requestSize.Value())
	newSize, err := d.diskController.ResizeDisk(ctx, diskURI, oldSize, requestSize, d.enableDiskOnlineResize)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to resize disk(%s) with error(%v)", diskURI, err)
	}

	currentSize, ok := newSize.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal, "failed to transform disk size with error(%v)", err)
	}

	isOperationSucceeded = true
	klog.V(2).Infof("expand azure disk(%s) successfully, currentSize(%d)", diskURI, currentSize)

	if result.ManagedBy != nil {
		attachedNode, err := d.cloud.VMSet.GetNodeNameByProviderID(ctx, *result.ManagedBy)
		if err == nil {
			klog.V(2).Infof("delete cache for node (%s, %s) after disk(%s) expanded", attachedNode, *result.ManagedBy, diskURI)
			if err = d.cloud.VMSet.DeleteCacheForNode(ctx, string(attachedNode)); err != nil {
				klog.Warningf("failed to delete cache for node %s with error(%v)", attachedNode, err)
			}
		} else {
			klog.Warningf("failed to get attached node for disk(%s) with error(%v)", diskURI, err)
		}
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         currentSize,
		NodeExpansionRequired: true,
	}, nil
}

// CreateSnapshot create a snapshot
func (d *Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	sourceVolumeID := req.GetSourceVolumeId()
	if len(sourceVolumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateSnapshot Source Volume ID must be provided")
	}
	snapshotName := req.Name
	if len(snapshotName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "snapshot name must be provided")
	}

	snapshotName = azureutils.CreateValidDiskName(snapshotName)

	var customTags string
	// set incremental snapshot as true by default
	incremental := true
	var subsID, resourceGroup, dataAccessAuthMode, networkAccessPolicy, publicNetworkAccess, tagValueDelimiter string
	var err error
	localCloud := d.cloud
	location := d.cloud.Location

	tags := make(map[string]*string)

	parameters := req.GetParameters()
	for k, v := range parameters {
		switch strings.ToLower(k) {
		case consts.TagsField:
			customTags = v
		case consts.IncrementalField:
			if v == "false" {
				incremental = false
			}
		case consts.ResourceGroupField:
			resourceGroup = v
		case consts.LocationField:
			location = v
		case consts.UserAgentField:
			newUserAgent := v
			localCloud, err = azureutils.GetCloudProviderFromClient(ctx, d.kubeClient, d.cloudConfigSecretName, d.cloudConfigSecretNamespace, newUserAgent,
				d.allowEmptyCloudConfig, d.enableTrafficManager, d.enableMinimumRetryAfter, d.trafficManagerPort)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "create cloud with UserAgent(%s) failed with: (%s)", newUserAgent, err)
			}
		case consts.SubscriptionIDField:
			subsID = v
		case consts.DataAccessAuthModeField:
			dataAccessAuthMode = v
		case consts.NetworkAccessPolicyField:
			networkAccessPolicy = v
		case consts.PublicNetworkAccessField:
			publicNetworkAccess = v
		case consts.TagValueDelimiterField:
			tagValueDelimiter = v
		case consts.VolumeSnapshotNameKey:
			tags[consts.SnapshotNameTag] = ptr.To(v)
		case consts.VolumeSnapshotNamespaceKey:
			tags[consts.SnapshotNamespaceTag] = ptr.To(v)
		case consts.VolumeSnapshotContentNameKey:
			// ignore the key
		default:
			return nil, status.Errorf(codes.Internal, "AzureDisk - invalid option %s in VolumeSnapshotClass", k)
		}
	}

	if azureutils.IsAzureStackCloud(localCloud.Config.Cloud, localCloud.Config.DisableAzureStackCloud) {
		klog.V(2).Info("Use full snapshot instead as Azure Stack does not support incremental snapshot.")
		incremental = false
	}

	if resourceGroup == "" {
		if _, resourceGroup, _, err = azureutils.GetInfoFromURI(sourceVolumeID); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "could not get resource group from diskURI(%s) with error(%v)", sourceVolumeID, err)
		}
	}

	customTagsMap, err := volumehelper.ConvertTagsToMap(customTags, tagValueDelimiter)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	tags[azureconsts.CreatedByTag] = ptr.To(consts.AzureDiskDriverTag)
	tags["source_volume_id"] = ptr.To(sourceVolumeID)
	for k, v := range customTagsMap {
		value := v
		tags[k] = &value
	}

	snapshot := armcompute.Snapshot{
		Properties: &armcompute.SnapshotProperties{
			CreationData: &armcompute.CreationData{
				CreateOption:     to.Ptr(armcompute.DiskCreateOptionCopy),
				SourceResourceID: &sourceVolumeID,
			},
			Incremental: &incremental,
		},
		Location: &d.cloud.Location,
		Tags:     tags,
	}

	if d.cloud.HasExtendedLocation() {
		klog.V(2).Infof("extended location Name:%s Type:%s is set on snapshot %s, source volume %s", d.cloud.ExtendedLocationName, d.cloud.ExtendedLocationType, snapshotName, sourceVolumeID)
		snapshot.ExtendedLocation = &armcompute.ExtendedLocation{
			Name: to.Ptr(d.cloud.ExtendedLocationName),
			Type: to.Ptr(armcompute.ExtendedLocationTypes(d.cloud.ExtendedLocationType)),
		}
	}

	if dataAccessAuthMode != "" {
		if err := azureutils.ValidateDataAccessAuthMode(dataAccessAuthMode); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		snapshot.Properties.DataAccessAuthMode = to.Ptr(armcompute.DataAccessAuthMode(dataAccessAuthMode))
	}

	if networkAccessPolicy != "" {
		policy, err := azureutils.NormalizeNetworkAccessPolicy(networkAccessPolicy)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		snapshot.Properties.NetworkAccessPolicy = to.Ptr(armcompute.NetworkAccessPolicy(policy))
	}

	if publicNetworkAccess != "" {
		pna, err := azureutils.NormalizePublicNetworkAccess(publicNetworkAccess)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		snapshot.Properties.PublicNetworkAccess = to.Ptr(armcompute.PublicNetworkAccess(pna))
	}

	if acquired := d.volumeLocks.TryAcquire(snapshotName); !acquired {
		return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, snapshotName)
	}
	defer d.volumeLocks.Release(snapshotName)

	var crossRegionSnapshotName string
	if location != "" && location != d.cloud.Location {
		if incremental {
			crossRegionSnapshotName = snapshotName
			snapshotName = azureutils.CreateValidDiskName("local_" + snapshotName)
		} else {
			return nil, status.Errorf(codes.InvalidArgument, "could not create snapshot cross region with incremental is false")
		}
	}

	metricsRequest := "controller_create_snapshot"
	if crossRegionSnapshotName != "" {
		metricsRequest = "controller_create_snapshot_cross_region"
	}
	mc := metrics.NewMetricContext(consts.AzureDiskCSIDriverName, metricsRequest, d.cloud.ResourceGroup, d.cloud.SubscriptionID, d.Name)
	isOperationSucceeded := false
	isOperationInProgress := false
	defer func() {
		if !isOperationInProgress {
			mc.ObserveOperationWithResult(isOperationSucceeded, consts.SourceResourceID, sourceVolumeID, consts.SnapshotName, snapshotName)
		}
	}()

	klog.V(2).Infof("begin to create snapshot(%s, incremental: %v) under rg(%s) region(%s)", snapshotName, incremental, resourceGroup, d.cloud.Location)
	snapshotClient, err := d.clientFactory.GetSnapshotClientForSub(subsID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not get snapshot client for subscription(%s) with error(%v)", subsID, err)
	}

	csiSnapshot, _ := d.getSnapshotByID(ctx, subsID, resourceGroup, snapshotName, "")
	if csiSnapshot == nil || sourceVolumeID != csiSnapshot.SourceVolumeId {
		if _, err := snapshotClient.CreateOrUpdate(ctx, resourceGroup, snapshotName, snapshot); err != nil {
			if strings.Contains(err.Error(), "existing disk") {
				return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("request snapshot(%s) under rg(%s) already exists, but the SourceVolumeId is different, error details: %v", snapshotName, resourceGroup, err))
			}

			azureutils.SleepIfThrottled(err, consts.SnapshotOpThrottlingSleepSec)
			return nil, status.Error(codes.Internal, fmt.Sprintf("create snapshot error: %v", err.Error()))
		}
	}

	if d.shouldWaitForSnapshotReady {
		if err := d.waitForSnapshotReady(ctx, subsID, resourceGroup, snapshotName, waitForSnapshotReadyInterval, waitForSnapshotReadyTimeout); err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("waitForSnapshotReady(%s, %s, %s) failed with %v", subsID, resourceGroup, snapshotName, err))
		}
	}
	klog.V(2).Infof("create snapshot(%s) under rg(%s) region(%s) successfully", snapshotName, resourceGroup, d.cloud.Location)

	csiSnapshot, err = d.getSnapshotByID(ctx, subsID, resourceGroup, snapshotName, sourceVolumeID)
	if err != nil {
		return nil, err
	} else if csiSnapshot == nil {
		klog.Errorf("getSnapshotByID(%s, %s, %s) did not return a valid snapshot", subsID, resourceGroup, snapshotName)
		return nil, status.Error(codes.Internal, fmt.Sprintf("getSnapshotByID(%s, %s, %s) did not return a valid snapshot", subsID, resourceGroup, snapshotName))
	}

	if csiSnapshot.ReadyToUse && crossRegionSnapshotName != "" {
		crossRegionSnapshot, _ := d.getSnapshotByID(ctx, subsID, resourceGroup, crossRegionSnapshotName, sourceVolumeID)
		if crossRegionSnapshot == nil {
			copySnapshot := snapshot
			if copySnapshot.Properties == nil {
				copySnapshot.Properties = &armcompute.SnapshotProperties{}
			}
			if copySnapshot.Properties.CreationData == nil {
				copySnapshot.Properties.CreationData = &armcompute.CreationData{}
			}
			copySnapshot.Properties.CreationData.SourceResourceID = &csiSnapshot.SnapshotId
			copySnapshot.Properties.CreationData.CreateOption = to.Ptr(armcompute.DiskCreateOptionCopyStart)
			copySnapshot.Location = &location

			klog.V(2).Infof("begin to create snapshot(%s, incremental: %v) under rg(%s) region(%s)", crossRegionSnapshotName, incremental, resourceGroup, location)
			if _, err := snapshotClient.CreateOrUpdate(ctx, resourceGroup, crossRegionSnapshotName, copySnapshot); err != nil {
				if strings.Contains(err.Error(), "existing disk") {
					return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("request snapshot(%s) under rg(%s) already exists, but the SourceVolumeId is different, error details: %v", crossRegionSnapshotName, resourceGroup, err))
				}

				azureutils.SleepIfThrottled(err, consts.SnapshotOpThrottlingSleepSec)
				return nil, status.Error(codes.Internal, fmt.Sprintf("create snapshot error: %v", err))
			}
			klog.V(2).Infof("create snapshot(%s) under rg(%s) region(%s) successfully", crossRegionSnapshotName, resourceGroup, location)
		}

		if d.shouldWaitForSnapshotReady {
			if err := d.waitForSnapshotReady(ctx, subsID, resourceGroup, crossRegionSnapshotName, waitForSnapshotReadyInterval, waitForSnapshotReadyTimeout); err != nil {
				return nil, status.Error(codes.Internal, fmt.Sprintf("waitForSnapshotReady(%s, %s, %s) failed with %v", subsID, resourceGroup, crossRegionSnapshotName, err))
			}
		}

		csiSnapshot, err = d.getSnapshotByID(ctx, subsID, resourceGroup, crossRegionSnapshotName, sourceVolumeID)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		if csiSnapshot.ReadyToUse {
			klog.V(2).Infof("begin to delete snapshot(%s) under rg(%s) region(%s)", snapshotName, resourceGroup, d.cloud.Location)
			if err = snapshotClient.Delete(ctx, resourceGroup, snapshotName); err != nil {
				klog.Errorf("delete snapshot error: %v", err)
				azureutils.SleepIfThrottled(err, consts.SnapshotOpThrottlingSleepSec)
			} else {
				klog.V(2).Infof("delete snapshot(%s) under rg(%s) region(%s) successfully", snapshotName, resourceGroup, d.cloud.Location)
			}
		}

	} else if crossRegionSnapshotName != "" {
		// replace the last token of csiSnapshot.SnapshotId with crossRegionSnapshotName
		csiSnapshot.SnapshotId = strings.TrimSuffix(csiSnapshot.SnapshotId, snapshotName) + crossRegionSnapshotName
	}

	isOperationInProgress = !csiSnapshot.ReadyToUse

	createResp := &csi.CreateSnapshotResponse{
		Snapshot: csiSnapshot,
	}
	isOperationSucceeded = true
	return createResp, nil
}

// DeleteSnapshot delete a snapshot
func (d *Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	snapshotID := req.SnapshotId
	if len(snapshotID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot ID must be provided")
	}

	var err error
	var subsID string
	snapshotName := snapshotID
	resourceGroup := d.cloud.ResourceGroup

	if azureutils.IsARMResourceID(snapshotID) {
		subsID, resourceGroup, snapshotName, err = azureutils.GetInfoFromURI(snapshotID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
	}

	mc := metrics.NewMetricContext(consts.AzureDiskCSIDriverName, "controller_delete_snapshot", d.cloud.ResourceGroup, d.cloud.SubscriptionID, d.Name)
	isOperationSucceeded := false
	defer func() {
		mc.ObserveOperationWithResult(isOperationSucceeded, consts.SnapshotID, snapshotID)
	}()

	klog.V(2).Infof("begin to delete snapshot(%s) under rg(%s)", snapshotName, resourceGroup)
	snapshotClient, err := d.clientFactory.GetSnapshotClientForSub(subsID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not get snapshot client for subscription(%s) with error(%v)", subsID, err)
	}
	if err := snapshotClient.Delete(ctx, resourceGroup, snapshotName); err != nil {
		azureutils.SleepIfThrottled(err, consts.SnapshotOpThrottlingSleepSec)
		return nil, status.Error(codes.Internal, fmt.Sprintf("delete snapshot error: %v", err))
	}
	klog.V(2).Infof("delete snapshot(%s) under rg(%s) successfully", snapshotName, resourceGroup)
	isOperationSucceeded = true
	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots list all snapshots
func (d *Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	// SnapshotId is not empty, return snapshot that match the snapshot id.
	if len(req.GetSnapshotId()) != 0 {
		snapshot, err := d.getSnapshotByID(ctx, "", d.cloud.ResourceGroup, req.GetSnapshotId(), req.SourceVolumeId)
		if err != nil {
			if strings.Contains(err.Error(), consts.ResourceNotFound) {
				return &csi.ListSnapshotsResponse{}, nil
			}
			return nil, err
		}
		entries := []*csi.ListSnapshotsResponse_Entry{
			{
				Snapshot: snapshot,
			},
		}
		listSnapshotResp := &csi.ListSnapshotsResponse{
			Entries: entries,
		}
		return listSnapshotResp, nil
	}
	snapshotClient := d.clientFactory.GetSnapshotClient()
	// no SnapshotId is set, return all snapshots that satisfy the request.
	snapshots, err := snapshotClient.List(ctx, d.cloud.ResourceGroup)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Unknown list snapshot error: %v", err.Error()))
	}

	return azureutils.GetEntriesAndNextToken(req, snapshots)
}

func (d *Driver) getSnapshotByID(ctx context.Context, subsID, resourceGroup, snapshotID, sourceVolumeID string) (*csi.Snapshot, error) {
	var err error
	snapshotName := snapshotID
	if azureutils.IsARMResourceID(snapshotID) {
		subsID, resourceGroup, snapshotName, err = azureutils.GetInfoFromURI(snapshotID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
	}
	snapshotClient, err := d.clientFactory.GetSnapshotClientForSub(subsID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not get snapshot client for subscription(%s) with error(%v)", subsID, err)
	}
	snapshot, err := snapshotClient.Get(ctx, resourceGroup, snapshotName)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("get snapshot %s from rg(%s) error: %v", snapshotName, resourceGroup, err))
	}

	return azureutils.GenerateCSISnapshot(sourceVolumeID, snapshot)
}

// GetSourceDiskSize recursively searches for the sourceDisk and returns: sourceDisk disk size, error
func (d *Driver) GetSourceDiskSize(ctx context.Context, subsID, resourceGroup, diskName string, curDepth, maxDepth int) (*int32, *armcompute.Disk, error) {
	if curDepth > maxDepth {
		return nil, nil, status.Error(codes.Internal, fmt.Sprintf("current depth (%d) surpassed the max depth (%d) while searching for the source disk size", curDepth, maxDepth))
	}
	result, err := d.diskController.GetDisk(ctx, subsID, resourceGroup, diskName)
	if err != nil {
		return nil, result, err
	}
	if result == nil || result.Properties == nil {
		return nil, result, status.Error(codes.Internal, fmt.Sprintf("DiskProperty not found for disk (%s) in resource group (%s)", diskName, resourceGroup))
	}

	if result.Properties.CreationData != nil && result.Properties.CreationData.CreateOption != nil && *result.Properties.CreationData.CreateOption == armcompute.DiskCreateOptionCopy {
		klog.V(2).Infof("Clone source disk has a parent source")
		sourceResourceID := *result.Properties.CreationData.SourceResourceID
		subsID, parentResourceGroup, parentDiskName, err := azureutils.GetInfoFromURI(sourceResourceID)
		if err != nil {
			return nil, result, status.Error(codes.Internal, fmt.Sprintf("failed to get subscription id, resource group from disk uri (%s) with error(%v)", sourceResourceID, err))
		}
		return d.GetSourceDiskSize(ctx, subsID, parentResourceGroup, parentDiskName, curDepth+1, maxDepth)
	}

	if (*result.Properties).DiskSizeGB == nil {
		return nil, result, status.Error(codes.Internal, fmt.Sprintf("DiskSizeGB for disk (%s) in resourcegroup (%s) is nil", diskName, resourceGroup))
	}
	return (*result.Properties).DiskSizeGB, result, nil
}

// getSnapshot retrieves the Snapshot and returns the Snapshot or the error if any error occurs
func (d *Driver) getSnapshot(ctx context.Context, sourceID string) (*armcompute.Snapshot, error) {
	subsID, resourceGroup, snapshotName, err := azureutils.GetInfoFromURI(sourceID)
	if err != nil {
		klog.Warningf("could not get subscription id, resource group from snapshot uri (%s) with error(%v)", sourceID, err)
		return nil, err
	}
	snapClient, err := d.clientFactory.GetSnapshotClientForSub(subsID)
	if err != nil {
		klog.Warningf("could not get snapshot client for subscription(%s) with error(%v)", subsID, err)
		return nil, err
	}
	snapshotRetrieved, err := snapClient.Get(ctx, resourceGroup, snapshotName)
	if err != nil {
		klog.Warningf("get snapshot %s from rg(%s) error: %v", snapshotName, resourceGroup, err)
		return nil, err
	}
	return snapshotRetrieved, nil
}

// getSnapshotSKU retrieves the SKU of the snapshot and returns the SKU or if any error occurs
func getSnapshotSKUFromSnapshot(computeSnapshot *armcompute.Snapshot) (string, error) {
	if computeSnapshot == nil {
		klog.Warningf("Snapshot is nil")
		return "", status.Error(codes.NotFound, "Snapshot is nil")
	}
	if computeSnapshot.SKU == nil || computeSnapshot.SKU.Name == nil {
		klog.Warningf("Snapshot or Snapshot Properties SKU not found for snapshot")
		return "", status.Error(codes.NotFound, "Snapshot SKU property not found")
	}
	return string(*computeSnapshot.SKU.Name), nil
}

// getDiskSizeInBytes retrieves the size of the disk and returns the size or if any error occurs
func getDiskSizeInBytesFromSnapshot(computeSnapshot *armcompute.Snapshot) (int64, error) {
	if computeSnapshot == nil {
		klog.Warningf("Snapshot is nil")
		return 0, status.Error(codes.NotFound, "Snapshot is nil")
	}
	if computeSnapshot.Properties == nil || computeSnapshot.Properties.DiskSizeBytes == nil {
		klog.Warningf("Snapshot or Snapshot Properties.DiskSizeBytes not found for snapshot")
		return 0, status.Error(codes.NotFound, "Snapshot size not found")
	}
	return *computeSnapshot.Properties.DiskSizeBytes, nil
}
