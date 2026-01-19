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
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"

	"k8s.io/apimachinery/pkg/api/resource"
	kwait "k8s.io/apimachinery/pkg/util/wait"
	volumehelpers "k8s.io/cloud-provider/volume/helpers"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	csidriverconsts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"

	azureconsts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
	azcache "sigs.k8s.io/cloud-provider-azure/pkg/cache"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

// ManagedDiskController : managed disk controller struct
type ManagedDiskController struct {
	*controllerCommon
}

func NewManagedDiskController(provider *provider.Cloud) *ManagedDiskController {
	common := &controllerCommon{
		cloud:                              provider,
		lockMap:                            newLockMap(),
		AttachDetachInitialDelayInMs:       defaultAttachDetachInitialDelayInMs,
		DetachOperationMinTimeoutInSeconds: defaultDetachOperationMinTimeoutInSeconds,
		clientFactory:                      provider.ComputeClientFactory,
	}
	getter := func(_ context.Context, _ string) (interface{}, error) { return nil, nil }
	common.hitMaxDataDiskCountCache, _ = azcache.NewTimedCache(5*time.Minute, getter, false)

	return &ManagedDiskController{common}
}

// ManagedDiskOptions specifies the options of managed disks.
type ManagedDiskOptions struct {
	// The SKU of storage account.
	StorageAccountType armcompute.DiskStorageAccountTypes
	// The name of the disk.
	DiskName string
	// The name of PVC.
	PVCName string
	// The name of resource group.
	ResourceGroup string
	// The AvailabilityZone to create the disk.
	AvailabilityZone string
	// The tags of the disk.
	Tags map[string]string
	// IOPS Caps for UltraSSD disk
	DiskIOPSReadWrite string
	// Throughput Cap (MBps) for UltraSSD disk
	DiskMBpsReadWrite string
	// if SourceResourceID is not empty, then it's a disk copy operation(for snapshot)
	SourceResourceID string
	// The type of source
	SourceType string
	// ResourceId of the disk encryption set to use for enabling encryption at rest.
	DiskEncryptionSetID string
	// DiskEncryption type, available values: EncryptionAtRestWithCustomerKey, EncryptionAtRestWithPlatformAndCustomerKeys
	DiskEncryptionType string
	// The size in GB.
	SizeGB int
	// The maximum number of VMs that can attach to the disk at the same time. Value greater than one indicates a disk that can be mounted on multiple VMs at the same time.
	MaxShares int32
	// Logical sector size in bytes for Ultra disks
	LogicalSectorSize int32
	// SkipGetDiskOperation indicates whether skip GetDisk operation(mainly due to throttling)
	SkipGetDiskOperation bool
	// PublicNetworkAccess - Possible values include: 'Enabled', 'Disabled'
	PublicNetworkAccess armcompute.PublicNetworkAccess
	// NetworkAccessPolicy - Possible values include: 'AllowAll', 'AllowPrivate', 'DenyAll'
	NetworkAccessPolicy armcompute.NetworkAccessPolicy
	// DiskAccessID - ARM id of the DiskAccess resource for using private endpoints on disks.
	DiskAccessID *string
	// BurstingEnabled - Set to true to enable bursting beyond the provisioned performance target of the disk.
	BurstingEnabled *bool
	// SubscriptionID - specify a different SubscriptionID
	SubscriptionID string
	// Location - specify a different location
	Location string
	// PerformancePlus - Set this flag to true to get a boost on the performance target of the disk deployed
	PerformancePlus *bool
}

// CreateManagedDisk: create managed disk
func (c *ManagedDiskController) CreateManagedDisk(ctx context.Context, options *ManagedDiskOptions) (string, error) {
	var err error
	klog.V(4).Infof("azureDisk - creating new managed Name:%s StorageAccountType:%s Size:%v", options.DiskName, options.StorageAccountType, options.SizeGB)

	var createZones []string
	if len(options.AvailabilityZone) > 0 {
		requestedZone := c.cloud.GetZoneID(options.AvailabilityZone)
		if requestedZone != "" {
			createZones = append(createZones, requestedZone)
		}
	}

	// insert original tags to newTags
	newTags := make(map[string]*string)
	newTags[consts.CreatedByTag] = ptr.To(azureconsts.AzureDiskDriverTag)
	if options.Tags != nil {
		for k, v := range options.Tags {
			value := v
			newTags[k] = &value
		}
	}

	diskSizeGB := int32(options.SizeGB)
	diskSku := options.StorageAccountType

	rg := c.cloud.ResourceGroup
	if options.ResourceGroup != "" {
		rg = options.ResourceGroup
	}
	if options.SubscriptionID != "" && !strings.EqualFold(options.SubscriptionID, c.cloud.SubscriptionID) && options.ResourceGroup == "" {
		return "", fmt.Errorf("resourceGroup must be specified when subscriptionID(%s) is not empty", options.SubscriptionID)
	}
	subsID := c.cloud.SubscriptionID
	if options.SubscriptionID != "" {
		subsID = options.SubscriptionID
	}

	creationData, err := getValidCreationData(subsID, rg, options)
	if err != nil {
		return "", err
	}

	diskProperties := armcompute.DiskProperties{}
	// when creating from snapshot, and diskSizeGB is 0, let disk RP calculate size from snapshot bytes size.
	if diskSizeGB == 0 && options.SourceType == csidriverconsts.SourceSnapshot {
		diskProperties = armcompute.DiskProperties{
			CreationData:    &creationData,
			BurstingEnabled: options.BurstingEnabled,
		}
	} else {
		diskProperties = armcompute.DiskProperties{
			CreationData:    &creationData,
			BurstingEnabled: options.BurstingEnabled,
			DiskSizeGB:      &diskSizeGB,
		}
	}

	isAzureStack := azureutils.IsAzureStackCloud(c.cloud.Config.Cloud, c.cloud.Config.DisableAzureStackCloud)

	if options.PublicNetworkAccess != "" {
		diskProperties.PublicNetworkAccess = to.Ptr(options.PublicNetworkAccess)
	} else {
		if isAzureStack {
			klog.V(2).Infof("AzureDisk - PublicNetworkAccess is not supported in Azure Stack, skipping the property setting")
		} else {
			diskProperties.PublicNetworkAccess = to.Ptr(armcompute.PublicNetworkAccessDisabled)
		}
	}

	if options.NetworkAccessPolicy != "" {
		diskProperties.NetworkAccessPolicy = to.Ptr(options.NetworkAccessPolicy)
		if options.NetworkAccessPolicy == armcompute.NetworkAccessPolicyAllowPrivate {
			if options.DiskAccessID == nil {
				return "", fmt.Errorf("DiskAccessID should not be empty when NetworkAccessPolicy is AllowPrivate")
			}
			diskProperties.DiskAccessID = options.DiskAccessID
		} else {
			if options.DiskAccessID != nil {
				return "", fmt.Errorf("DiskAccessID(%s) must be empty when NetworkAccessPolicy(%s) is not AllowPrivate", *options.DiskAccessID, options.NetworkAccessPolicy)
			}
		}
	} else {
		if isAzureStack {
			klog.V(2).Infof("AzureDisk - NetworkAccessPolicy is not supported in Azure Stack, skipping the property setting")
		} else {
			diskProperties.NetworkAccessPolicy = to.Ptr(armcompute.NetworkAccessPolicyDenyAll)
		}
	}

	if diskSku == armcompute.DiskStorageAccountTypesUltraSSDLRS || diskSku == armcompute.DiskStorageAccountTypesPremiumV2LRS {
		if options.DiskIOPSReadWrite == "" {
			if diskSku == armcompute.DiskStorageAccountTypesUltraSSDLRS {
				diskIOPSReadWrite := int64(consts.DefaultDiskIOPSReadWrite)
				diskProperties.DiskIOPSReadWrite = ptr.To(int64(diskIOPSReadWrite))
			}
		} else {
			v, err := strconv.Atoi(options.DiskIOPSReadWrite)
			if err != nil {
				return "", fmt.Errorf("AzureDisk - failed to parse DiskIOPSReadWrite: %w", err)
			}
			diskIOPSReadWrite := int64(v)
			diskProperties.DiskIOPSReadWrite = ptr.To(int64(diskIOPSReadWrite))
		}

		if options.DiskMBpsReadWrite == "" {
			if diskSku == armcompute.DiskStorageAccountTypesUltraSSDLRS {
				diskMBpsReadWrite := int64(consts.DefaultDiskMBpsReadWrite)
				diskProperties.DiskMBpsReadWrite = ptr.To(int64(diskMBpsReadWrite))
			}
		} else {
			v, err := strconv.Atoi(options.DiskMBpsReadWrite)
			if err != nil {
				return "", fmt.Errorf("AzureDisk - failed to parse DiskMBpsReadWrite: %w", err)
			}
			diskMBpsReadWrite := int64(v)
			diskProperties.DiskMBpsReadWrite = ptr.To(int64(diskMBpsReadWrite))
		}

		if options.LogicalSectorSize != 0 {
			klog.V(2).Infof("AzureDisk - requested LogicalSectorSize: %v", options.LogicalSectorSize)
			diskProperties.CreationData.LogicalSectorSize = ptr.To(int32(options.LogicalSectorSize))
		}
	} else {
		if options.DiskIOPSReadWrite != "" {
			return "", fmt.Errorf("AzureDisk - DiskIOPSReadWrite parameter is only applicable in UltraSSD_LRS disk type")
		}
		if options.DiskMBpsReadWrite != "" {
			return "", fmt.Errorf("AzureDisk - DiskMBpsReadWrite parameter is only applicable in UltraSSD_LRS disk type")
		}
		if options.LogicalSectorSize != 0 {
			return "", fmt.Errorf("AzureDisk - LogicalSectorSize parameter is only applicable in UltraSSD_LRS disk type")
		}
	}

	if options.DiskEncryptionSetID != "" {
		if strings.Index(strings.ToLower(options.DiskEncryptionSetID), "/subscriptions/") != 0 {
			return "", fmt.Errorf("AzureDisk - format of DiskEncryptionSetID(%s) is incorrect, correct format: %s", options.DiskEncryptionSetID, consts.DiskEncryptionSetIDFormat)
		}
		encryptionType := armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey
		if options.DiskEncryptionType != "" {
			encryptionType = armcompute.EncryptionType(options.DiskEncryptionType)
			klog.V(4).Infof("azureDisk - DiskEncryptionType: %s, DiskEncryptionSetID: %s", options.DiskEncryptionType, options.DiskEncryptionSetID)
		}
		diskProperties.Encryption = &armcompute.Encryption{
			DiskEncryptionSetID: &options.DiskEncryptionSetID,
			Type:                to.Ptr(encryptionType),
		}
	} else {
		if options.DiskEncryptionType != "" {
			return "", fmt.Errorf("AzureDisk - DiskEncryptionType(%s) should be empty when DiskEncryptionSetID is not set", options.DiskEncryptionType)
		}
	}

	if options.MaxShares > 1 {
		diskProperties.MaxShares = &options.MaxShares
	}

	location := c.cloud.Location
	if options.Location != "" {
		location = options.Location
	}
	model := armcompute.Disk{
		Location: &location,
		Tags:     newTags,
		SKU: &armcompute.DiskSKU{
			Name: to.Ptr(diskSku),
		},
		Properties: &diskProperties,
	}

	if c.cloud.HasExtendedLocation() {
		klog.V(2).Infof("extended location Name:%s Type:%s is set on disk(%s)", c.cloud.ExtendedLocationName, c.cloud.ExtendedLocationType, options.DiskName)
		model.ExtendedLocation = &armcompute.ExtendedLocation{
			Name: ptr.To(c.cloud.ExtendedLocationName),
			Type: to.Ptr(armcompute.ExtendedLocationTypes(c.cloud.ExtendedLocationType)),
		}
	}

	if len(createZones) > 0 {
		model.Zones = to.SliceOfPtrs(createZones...)
	}
	diskClient, err := c.clientFactory.GetDiskClientForSub(subsID)
	if err != nil {
		return "", err
	}

	diskID := fmt.Sprintf(managedDiskPath, subsID, rg, options.DiskName)
	disk, err := diskClient.CreateOrUpdate(ctx, rg, options.DiskName, model)
	if err != nil {
		return "", err
	}

	err = kwait.ExponentialBackoffWithContext(ctx, defaultBackOff, func(_ context.Context) (bool, error) {
		if disk != nil && disk.Properties != nil && strings.EqualFold(ptr.Deref((*disk.Properties).ProvisioningState, ""), "succeeded") {
			if ptr.Deref(disk.ID, "") != "" {
				diskID = *disk.ID
			}
			return true, nil
		}

		if options.SkipGetDiskOperation {
			klog.Warningf("azureDisk - GetDisk(%s, StorageAccountType:%s) is throttled, unable to confirm provisioningState in poll process", options.DiskName, options.StorageAccountType)
			return true, nil
		}
		klog.V(4).Infof("azureDisk - waiting for disk(%s) in resourceGroup(%s) to be provisioned", options.DiskName, rg)
		if disk, err = diskClient.Get(ctx, rg, options.DiskName); err != nil {
			// We are waiting for provisioningState==Succeeded
			// We don't want to hand-off managed disks to k8s while they are
			//still being provisioned, this is to avoid some race conditions
			return false, err
		}
		return false, nil
	})

	if err != nil {
		klog.Warningf("azureDisk - created new MD Name:%s StorageAccountType:%s Size:%v but was unable to confirm provisioningState in poll process", options.DiskName, options.StorageAccountType, options.SizeGB)
	} else {
		klog.V(2).Infof("azureDisk - created new MD Name:%s StorageAccountType:%s Size:%v", options.DiskName, options.StorageAccountType, options.SizeGB)
	}
	return diskID, nil
}

// DeleteManagedDisk : delete managed disk
func (c *ManagedDiskController) DeleteManagedDisk(ctx context.Context, diskURI string) error {
	subsID, resourceGroup, diskName, err := azureutils.GetInfoFromURI(diskURI)
	if err != nil {
		return err
	}

	if state, ok := c.diskStateMap.Load(strings.ToLower(diskURI)); ok {
		return fmt.Errorf("failed to delete disk(%s) since it's in %s state", diskURI, state.(string))
	}

	diskClient, err := c.clientFactory.GetDiskClientForSub(subsID)
	if err != nil {
		return err
	}

	disk, err := diskClient.Get(ctx, resourceGroup, diskName)
	if err != nil {
		var respErr = &azcore.ResponseError{}
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			klog.V(2).Infof("azureDisk - disk(%s) is already deleted", diskURI)
			return nil
		}
		return err
	}
	if disk.ManagedBy != nil {
		return fmt.Errorf("disk(%s) already attached to node(%s), could not be deleted", diskURI, *disk.ManagedBy)
	}

	if err = diskClient.Delete(ctx, resourceGroup, diskName); err != nil {
		return err
	}
	// We don't need poll here, k8s will immediately stop referencing the disk
	// the disk will be eventually deleted - cleanly - by ARM
	klog.V(2).Infof("azureDisk - deleted a managed disk: %s", diskURI)
	return nil
}

func (c *ManagedDiskController) GetDiskByURI(ctx context.Context, diskURI string) (*armcompute.Disk, error) {
	subsID, resourceGroup, diskName, err := azureutils.GetInfoFromURI(diskURI)
	if err != nil {
		return nil, err
	}
	return c.GetDisk(ctx, subsID, resourceGroup, diskName)
}

func (c *ManagedDiskController) GetDisk(ctx context.Context, subsID, resourceGroup, diskName string) (*armcompute.Disk, error) {
	diskclient, err := c.clientFactory.GetDiskClientForSub(subsID)
	if err != nil {
		return nil, err
	}
	return diskclient.Get(ctx, resourceGroup, diskName)
}

// ResizeDisk Expand the disk to new size
func (c *ManagedDiskController) ResizeDisk(ctx context.Context, diskURI string, oldSize resource.Quantity, newSize resource.Quantity, supportOnlineResize bool) (resource.Quantity, error) {
	subsID, resourceGroup, diskName, err := azureutils.GetInfoFromURI(diskURI)
	if err != nil {
		return oldSize, err
	}
	diskClient, err := c.clientFactory.GetDiskClientForSub(subsID)
	if err != nil {
		return oldSize, err
	}
	result, err := diskClient.Get(ctx, resourceGroup, diskName)
	if err != nil {
		return oldSize, err
	}

	if result == nil || result.Properties == nil || result.Properties.DiskSizeGB == nil {
		return oldSize, fmt.Errorf("DiskProperties of disk(%s) is nil", diskName)
	}

	// Azure resizes in chunks of GiB (not GB)
	requestGiB, err := volumehelpers.RoundUpToGiBInt32(newSize)
	if err != nil {
		return oldSize, err
	}

	newSizeQuant := resource.MustParse(fmt.Sprintf("%dGi", requestGiB))

	klog.V(2).Infof("azureDisk - begin to resize disk(%s) with new size(%d), old size(%v)", diskName, requestGiB, oldSize)
	// If disk already of greater or equal size than requested we return
	if *result.Properties.DiskSizeGB >= requestGiB {
		return newSizeQuant, nil
	}

	if !supportOnlineResize && *result.Properties.DiskState != armcompute.DiskStateUnattached {
		return oldSize, fmt.Errorf("azureDisk - disk resize is only supported on Unattached disk, current disk state: %s, already attached to %s", *result.Properties.DiskState, ptr.Deref(result.ManagedBy, ""))
	}

	diskParameter := armcompute.DiskUpdate{
		Properties: &armcompute.DiskUpdateProperties{
			DiskSizeGB: &requestGiB,
		},
	}

	if _, err := diskClient.Patch(ctx, resourceGroup, diskName, diskParameter); err != nil {
		return oldSize, err
	}

	klog.V(2).Infof("azureDisk - resize disk(%s) with new size(%d) completed", diskName, requestGiB)
	return newSizeQuant, nil
}

// ModifyDisk: modify disk
func (c *ManagedDiskController) ModifyDisk(ctx context.Context, options *ManagedDiskOptions) error {
	klog.V(4).Infof("azureDisk - modifying managed disk URI:%s, StorageAccountType:%s, DiskIOPSReadWrite:%s, DiskMBpsReadWrite:%s", options.SourceResourceID, options.StorageAccountType, options.DiskIOPSReadWrite, options.DiskMBpsReadWrite)

	subsID, rg, diskName, err := azureutils.GetInfoFromURI(options.SourceResourceID)
	if err != nil {
		return err
	}

	diskClient, err := c.clientFactory.GetDiskClientForSub(subsID)
	if err != nil {
		return err
	}

	result, err := diskClient.Get(ctx, rg, diskName)
	if err != nil {
		return err
	}

	if result == nil || result.Properties == nil || result.SKU == nil || result.SKU.Name == nil {
		return fmt.Errorf("DiskProperties or SKU of disk(%s) is nil", diskName)
	}

	model := armcompute.DiskUpdate{}
	diskSku := *result.SKU.Name
	if options.StorageAccountType != "" && options.StorageAccountType != diskSku {
		diskSku = options.StorageAccountType
		model.SKU = &armcompute.DiskSKU{
			Name: to.Ptr(diskSku),
		}
	}

	diskProperties := armcompute.DiskUpdateProperties{}

	if diskSku == armcompute.DiskStorageAccountTypesUltraSSDLRS || diskSku == armcompute.DiskStorageAccountTypesPremiumV2LRS {
		if options.DiskIOPSReadWrite != "" {
			v, err := strconv.Atoi(options.DiskIOPSReadWrite)
			if err != nil {
				return fmt.Errorf("AzureDisk - failed to parse DiskIOPSReadWrite: %w", err)
			}
			diskIOPSReadWrite := int64(v)
			diskProperties.DiskIOPSReadWrite = ptr.To(int64(diskIOPSReadWrite))
		}

		if options.DiskMBpsReadWrite != "" {
			v, err := strconv.Atoi(options.DiskMBpsReadWrite)
			if err != nil {
				return fmt.Errorf("AzureDisk - failed to parse DiskMBpsReadWrite: %w", err)
			}
			diskMBpsReadWrite := int64(v)
			diskProperties.DiskMBpsReadWrite = ptr.To(int64(diskMBpsReadWrite))
		}

		model.Properties = &diskProperties
	} else {
		if options.DiskIOPSReadWrite != "" {
			return fmt.Errorf("AzureDisk - DiskIOPSReadWrite parameter is only applicable in UltraSSD_LRS or PremiumV2_LRS disk type")
		}
		if options.DiskMBpsReadWrite != "" {
			return fmt.Errorf("AzureDisk - DiskMBpsReadWrite parameter is only applicable in UltraSSD_LRS or PremiumV2_LRS disk type")
		}
	}

	if model.SKU != nil || model.Properties != nil {
		if _, err := diskClient.Patch(ctx, rg, diskName, model); err != nil {
			return err
		}
	} else {
		klog.V(4).Infof("azureDisk - no modification needed for disk(%s)", diskName)
	}
	return nil
}
