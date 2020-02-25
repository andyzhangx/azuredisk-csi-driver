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
	"errors"
	"fmt"
	"strconv"
	"strings"

	volumehelper "sigs.k8s.io/azuredisk-csi-driver/pkg/util"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2019-07-01/compute"
	"github.com/Azure/azure-sdk-for-go/services/storage/mgmt/2019-06-01/storage"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog"
	"k8s.io/legacy-cloud-providers/azure"
)

var (
	// volumeCaps represents how the volume could be accessed.
	// It is SINGLE_NODE_WRITER since azure disk could only be attached to a single node at any given time.
	volumeCaps = []csi.VolumeCapability_AccessMode{
		{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}
)

const (
	azureDiskKind  = "kind"
	sourceSnapshot = "snapshot"
	sourceVolume   = "volume"
)

// CreateVolume provisions an azure disk
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := d.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		klog.Errorf("invalid create volume req: %v", req)
		return nil, err
	}

	volumeCapabilities := req.GetVolumeCapabilities()
	name := req.GetName()
	if len(name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Name must be provided")
	}
	if len(volumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Volume capabilities must be provided")
	}

	if !isValidVolumeCapabilities(volumeCapabilities) {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not supported")
	}
	capacityBytes := req.GetCapacityRange().GetRequiredBytes()
	volSizeBytes := int64(capacityBytes)
	requestGiB := int(volumehelper.RoundUpGiB(volSizeBytes))
	if requestGiB == 0 {
		requestGiB = defaultDiskSize
	}

	maxVolSize := int(volumehelper.RoundUpGiB(req.GetCapacityRange().GetLimitBytes()))
	if (maxVolSize > 0) && (maxVolSize < requestGiB) {
		return nil, status.Error(codes.InvalidArgument, "After round-up, volume size exceeds the limit specified")
	}

	var (
		location, account       string
		storageAccountType      string
		cachingMode             v1.AzureDataDiskCachingMode
		strKind                 string
		err                     error
		resourceGroup           string
		diskIopsReadWrite       string
		diskMbpsReadWrite       string
		diskName                string
		diskEncryptionSetID     string
		customTags              string
		writeAcceleratorEnabled string
	)

	parameters := req.GetParameters()
	for k, v := range parameters {
		switch strings.ToLower(k) {
		case "skuname":
			storageAccountType = v
		case "location":
			location = v
		case "storageaccount":
			account = v
		case "storageaccounttype":
			storageAccountType = v
		case azureDiskKind:
			strKind = v
		case "cachingmode":
			cachingMode = v1.AzureDataDiskCachingMode(v)
		case "resourcegroup":
			resourceGroup = v
		case "diskiopsreadwrite":
			diskIopsReadWrite = v
		case "diskmbpsreadwrite":
			diskMbpsReadWrite = v
		case "diskname":
			diskName = v
		case "diskencryptionsetid":
			diskEncryptionSetID = v
		case "tags":
			customTags = v
		case azure.WriteAcceleratorEnabled:
			writeAcceleratorEnabled = v
		default:
			//don't return error here since there are some parameters(e.g. fsType) used in disk mount process
			//return nil, fmt.Errorf("AzureDisk - invalid option %s in storage class", k)
		}
	}

	if diskName == "" {
		diskName = name
	}
	diskName = getValidDiskName(diskName)

	if resourceGroup == "" {
		resourceGroup = d.cloud.ResourceGroup
	}

	// normalize values
	skuName, err := normalizeStorageAccountType(storageAccountType)
	if err != nil {
		return nil, err
	}

	if _, err = normalizeCachingMode(cachingMode); err != nil {
		return nil, err
	}

	kind, err := normalizeKind(strFirstLetterToUpper(strKind))
	if err != nil {
		return nil, err
	}

	if kind != v1.AzureManagedDisk {
		if resourceGroup != "" {
			return nil, errors.New("StorageClass option 'resourceGroup' can be used only for managed disks")
		}
	}

	selectedAvailabilityZone := pickAvailabilityZone(req.GetAccessibilityRequirements(), d.cloud.Location)

	if ok, err := d.checkDiskCapacity(ctx, resourceGroup, diskName, requestGiB); !ok {
		return nil, err
	}

	customTagsMap, err := volumehelper.ConvertTagsToMap(customTags)
	if err != nil {
		return nil, err
	}

	klog.V(2).Infof("begin to create azure disk(%s) account type(%s) rg(%s) location(%s) size(%d) selectedAvailabilityZone(%v)",
		diskName, skuName, resourceGroup, location, requestGiB, selectedAvailabilityZone)

	diskURI := ""
	contentSource := &csi.VolumeContentSource{}
	if kind == v1.AzureManagedDisk {
		tags := make(map[string]string)
		for k, v := range customTagsMap {
			tags[k] = v
		}
		/* todo: check where are the tags in CSI
		if p.options.CloudTags != nil {
			tags = *(p.options.CloudTags)
		}
		*/

		if strings.EqualFold(writeAcceleratorEnabled, "true") {
			tags[azure.WriteAcceleratorEnabled] = "true"
		}
		sourceID := ""
		sourceType := ""
		content := req.GetVolumeContentSource()
		if content != nil {
			if content.GetSnapshot() != nil {
				sourceID = content.GetSnapshot().GetSnapshotId()
				sourceType = sourceSnapshot
				contentSource = &csi.VolumeContentSource{
					Type: &csi.VolumeContentSource_Snapshot{
						Snapshot: &csi.VolumeContentSource_SnapshotSource{
							SnapshotId: sourceID,
						},
					},
				}
			} else {
				sourceID = content.GetVolume().GetVolumeId()
				sourceType = sourceVolume
				contentSource = &csi.VolumeContentSource{
					Type: &csi.VolumeContentSource_Volume{
						Volume: &csi.VolumeContentSource_VolumeSource{
							VolumeId: sourceID,
						},
					},
				}

			}
		}

		volumeOptions := &azure.ManagedDiskOptions{
			DiskName:            diskName,
			StorageAccountType:  skuName,
			ResourceGroup:       resourceGroup,
			PVCName:             "",
			SizeGB:              requestGiB,
			Tags:                tags,
			AvailabilityZone:    selectedAvailabilityZone,
			DiskIOPSReadWrite:   diskIopsReadWrite,
			DiskMBpsReadWrite:   diskMbpsReadWrite,
			SourceResourceID:    sourceID,
			SourceType:          sourceType,
			DiskEncryptionSetID: diskEncryptionSetID,
		}
		diskURI, err = d.cloud.CreateManagedDisk(volumeOptions)
		if err != nil {
			if strings.Contains(err.Error(), "NotFound") {
				return nil, status.Error(codes.NotFound, err.Error())
			}
			return nil, err
		}
	} else {
		if kind == v1.AzureDedicatedBlobDisk {
			_, diskURI, _, err = d.cloud.CreateVolume(diskName, account, storageAccountType, location, requestGiB)
			if err != nil {
				return nil, err
			}
		} else {
			diskURI, err = d.cloud.CreateBlobDisk(diskName, storage.SkuName(storageAccountType), requestGiB)
			if err != nil {
				return nil, err
			}
		}
	}

	klog.V(2).Infof("create azure disk(%s) account type(%s) rg(%s) location(%s) size(%d) successfully", diskName, skuName, resourceGroup, location, requestGiB)

	/*  todo: block volume support
	if utilfeature.DefaultFeatureGate.Enabled(features.BlockVolume) {
		volumeMode = p.options.PVC.Spec.VolumeMode
		if volumeMode != nil && *volumeMode == v1.PersistentVolumeBlock {
			// Block volumes should not have any FSType
			fsType = ""
		}
	}
	*/

	// for CSI migration: reset kind value, make it well formatted
	if _, ok := parameters[azureDiskKind]; ok {
		parameters[azureDiskKind] = string(kind)
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      diskURI,
			CapacityBytes: capacityBytes,
			VolumeContext: parameters,
			ContentSource: contentSource,
			AccessibleTopology: []*csi.Topology{
				{
					Segments: map[string]string{topologyKey: selectedAvailabilityZone},
				},
			},
		},
	}, nil
}

// DeleteVolume delete an azure disk
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if err := d.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		return nil, fmt.Errorf("invalid delete volume req: %v", req)
	}
	diskURI := req.VolumeId

	if err := isValidDiskURI(diskURI); err != nil {
		klog.Errorf("validateDiskURI(%s) in DeleteVolume failed with error: %v", diskURI, err)
		return &csi.DeleteVolumeResponse{}, nil
	}

	klog.V(2).Infof("deleting azure disk(%s)", diskURI)
	if isManagedDisk(diskURI) {
		if err := d.cloud.DeleteManagedDisk(diskURI); err != nil {
			return &csi.DeleteVolumeResponse{}, err
		}
	} else {
		if err := d.cloud.DeleteBlobDisk(diskURI); err != nil {
			return &csi.DeleteVolumeResponse{}, err
		}
	}
	klog.V(2).Infof("delete azure disk(%s) successfully", diskURI)

	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume attach an azure disk to a required node
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	klog.V(2).Infof("ControllerPublishVolume: called with args %+v", *req)
	diskURI := req.GetVolumeId()
	if len(diskURI) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
	}

	caps := []*csi.VolumeCapability{volCap}
	if !isValidVolumeCapabilities(caps) {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not supported")
	}

	nodeID := req.GetNodeId()
	if len(nodeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Node ID not provided")
	}

	nodeName := types.NodeName(nodeID)
	diskName, err := getDiskName(diskURI)
	if err != nil {
		return nil, err
	}

	klog.V(2).Infof("GetDiskLun returned: %v. Initiating attaching volume %q to node %q.", err, diskURI, nodeName)

	lun, err := d.cloud.GetDiskLun(diskName, diskURI, nodeName)
	if err == cloudprovider.InstanceNotFound {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("failed to get azure instance id for node %q (%v)", nodeName, err))
	}

	if err == nil {
		// Volume is already attached to node.
		klog.V(2).Infof("Attach operation is successful. volume %q is already attached to node %q at lun %d.", diskURI, nodeName, lun)
	} else {
		isManagedDisk := isManagedDisk(diskURI)

		var cachingMode compute.CachingTypes
		if cachingMode, err = getCachingMode(req.GetVolumeContext()); err != nil {
			return nil, err
		}
		klog.V(2).Infof("Trying to attach volume %q to node %q", diskURI, nodeName)
		// Disable UseInstanceMetadata to mitigate a timeout issue from IMDS
		// https://github.com/kubernetes-sigs/azuredisk-csi-driver/issues/168
		d.cloud.Config.UseInstanceMetadata = false
		lun, err = d.cloud.AttachDisk(isManagedDisk, diskName, diskURI, nodeName, cachingMode)
		if err == nil {
			klog.V(2).Infof("Attach operation successful: volume %q attached to node %q.", diskURI, nodeName)
		} else {
			klog.Errorf("Attach volume %q to instance %q failed with %v", diskURI, nodeName, err)
			return nil, fmt.Errorf("Attach volume %q to instance %q failed with %v", diskURI, nodeName, err)
		}
		klog.V(2).Infof("attach volume %q to node %q successfully", diskURI, nodeName)
	}

	pvInfo := map[string]string{LUN: strconv.Itoa(int(lun))}
	return &csi.ControllerPublishVolumeResponse{PublishContext: pvInfo}, nil
}

// ControllerUnpublishVolume detach an azure disk from a required node
func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	klog.V(2).Infof("ControllerUnpublishVolume: called with args %+v", *req)
	diskURI := req.GetVolumeId()
	if len(diskURI) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	nodeID := req.GetNodeId()
	if len(nodeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Node ID not provided")
	}
	nodeName := types.NodeName(nodeID)

	diskName, err := getDiskName(diskURI)
	if err != nil {
		return nil, err
	}

	klog.V(2).Infof("Trying to detach volume %s from node %s", diskURI, nodeID)
	// Disable UseInstanceMetadata to mitigate a timeout issue from IMDS
	// https://github.com/kubernetes-sigs/azuredisk-csi-driver/issues/168
	d.cloud.Config.UseInstanceMetadata = false
	if err := d.cloud.DetachDisk(diskName, diskURI, nodeName); err != nil {
		if strings.Contains(err.Error(), errDiskNotFound) {
			klog.Warningf("volume %s already detached from node %s", diskURI, nodeID)
		} else {
			return nil, status.Errorf(codes.Internal, "Could not detach volume %q from node %q: %v", diskURI, nodeID, err)
		}
	}
	klog.V(2).Infof("detach volume %s from node %s successfully", diskURI, nodeID)

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities return the capabilities of the volume
func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	diskURI := req.GetVolumeId()
	if len(diskURI) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities missing in request")
	}

	err := d.checkDiskExists(ctx, diskURI)
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Volume not found, failed with error: %v", err))
	}

	for _, cap := range req.VolumeCapabilities {
		if cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: ""}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{Message: ""}, nil
}

// ControllerGetCapabilities returns the capabilities of the Controller plugin
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(2).Infof("Using default ControllerGetCapabilities")

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: d.Cap,
	}, nil
}

// GetCapacity returns the capacity of the total available storage pool
func (d *Driver) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ListVolumes return all available volumes
func (d *Driver) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerExpandVolume controller expand volume
func (d *Driver) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
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
	if !isManagedDisk(diskURI) {
		return nil, status.Errorf(codes.InvalidArgument, "the disk type(%s) is not ManagedDisk", diskURI)
	}
	if err := isValidDiskURI(diskURI); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "disk URI(%s) is not valid: %v", diskURI, err)
	}

	diskName, err := getDiskName(diskURI)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not get disk name from diskURI(%s) with error(%v)", diskURI, err)
	}
	resourceGroup, err := getResourceGroupFromURI(diskURI)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not get resource group from diskURI(%s) with error(%v)", diskURI, err)
	}

	result, rerr := d.cloud.DisksClient.Get(ctx, resourceGroup, diskName)
	if rerr != nil {
		return nil, status.Errorf(codes.Internal, "could not get the disk(%s) under rg(%s) with error(%v)", diskName, resourceGroup, rerr.Error())
	}
	if result.DiskProperties.DiskSizeGB == nil {
		return nil, status.Errorf(codes.Internal, "could not get size of the disk(%s)", diskName)
	}
	oldSize := *resource.NewQuantity(int64(*result.DiskProperties.DiskSizeGB), resource.BinarySI)

	klog.V(2).Infof("begin to expand azure disk(%s) with new size(%v)", diskURI, requestSize)
	newSize, err := d.cloud.ResizeDisk(diskURI, oldSize, requestSize)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to resize disk(%s) with error(%v)", diskURI, err)
	}

	currentSize, ok := newSize.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal, "failed to transform disk size with error(%v)", err)
	}

	klog.V(2).Infof("expand azure disk(%s) successfully, currentSize(%v)", diskURI, currentSize)

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         currentSize,
		NodeExpansionRequired: true,
	}, nil
}

// CreateSnapshot create a snapshot
func (d *Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	klog.V(2).Infof("CreateSnapshot called with request %v", *req)

	sourceVolumeID := req.GetSourceVolumeId()
	snapshotName := req.Name
	if len(snapshotName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot name must be provided")
	}
	if len(sourceVolumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateSnapshot Source Volume ID must be provided")
	}

	snapshotName = getValidDiskName(snapshotName)

	snapshot := compute.Snapshot{
		SnapshotProperties: &compute.SnapshotProperties{
			CreationData: &compute.CreationData{
				CreateOption: compute.Copy,
				SourceURI:    &sourceVolumeID,
			},
		},
		Location: &d.cloud.Location,
	}

	//todo: add metrics here
	klog.V(2).Infof("begin to create snapshot(%s) under rg(%s)", snapshotName, d.cloud.ResourceGroup)
	rerr := d.cloud.SnapshotsClient.CreateOrUpdate(ctx, d.cloud.ResourceGroup, snapshotName, snapshot)
	if rerr != nil {
		if strings.Contains(rerr.Error().Error(), "existing disk") {
			return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("request snapshot(%s) under rg(%s) already exists, but the SourceVolumeId is different, error details: %v", snapshotName, d.cloud.ResourceGroup, rerr.Error()))
		} else {
			return nil, status.Error(codes.Internal, fmt.Sprintf("create snapshot error: %v", rerr.Error()))
		}
	}
	klog.V(2).Infof("create snapshot(%s) under rg(%s) successfully", snapshotName, d.cloud.ResourceGroup)

	csiSnapshot, err := d.getSnapshotByID(ctx, snapshotName, sourceVolumeID)
	if err != nil {
		return nil, err
	}

	createResp := &csi.CreateSnapshotResponse{
		Snapshot: csiSnapshot,
	}
	return createResp, nil
}

// DeleteSnapshot delete a snapshot
func (d *Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	klog.V(2).Infof("DeleteSnapshot called with request %v", *req)

	if len(req.SnapshotId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot ID must be provided")
	}

	snapshotName, resourceGroup, err := d.extractSnapshotInfo(req.SnapshotId)
	if err != nil {
		return nil, err
	}

	//todo: add metrics here
	klog.V(2).Infof("begin to delete snapshot(%s) under rg(%s)", snapshotName, resourceGroup)
	rerr := d.cloud.SnapshotsClient.Delete(ctx, resourceGroup, snapshotName)
	if rerr != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("delete snapshot error: %v", rerr.Error()))
	}
	klog.V(2).Infof("delete snapshot(%s) under rg(%s) successfully", snapshotName, resourceGroup)
	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots list all snapshots
func (d *Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	klog.V(2).Infof("ListSnapshots called with request %v", *req)

	// SnapshotId is not empty, return snapshot that match the snapshot id.
	if len(req.GetSnapshotId()) != 0 {
		snapshot, err := d.getSnapshotByID(ctx, req.GetSnapshotId(), req.SourceVolumeId)
		if err != nil {
			if strings.Contains(err.Error(), resourceNotFound) {
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

	// no SnapshotId is set, so we return all snapshots that satify the reqeust.
	snapshots, err := d.cloud.SnapshotsClient.ListByResourceGroup(ctx, d.cloud.ResourceGroup)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Unknown list snapshot error: %v", err.Error()))
	}

	return getEntriesAndNextToken(req, snapshots)
}

func (d *Driver) getSnapshotByID(ctx context.Context, snapshotID, sourceVolumeID string) (*csi.Snapshot, error) {
	snapshotName, resourceGroup, err := d.extractSnapshotInfo(snapshotID)
	if err != nil {
		return nil, err
	}

	snapshot, rerr := d.cloud.SnapshotsClient.Get(ctx, resourceGroup, snapshotName)
	if rerr != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("get snapshot %s from rg(%s) error: %v", snapshotName, resourceGroup, rerr.Error()))
	}

	return generateCSISnapshot(sourceVolumeID, &snapshot)
}

func isValidVolumeCapabilities(volCaps []*csi.VolumeCapability) bool {
	hasSupport := func(cap *csi.VolumeCapability) bool {
		for _, c := range volumeCaps {
			// todo: Block volume support
			/* compile error here
			if blk := c.GetBlock(); blk != nil {
				return false
			}
			*/
			if c.GetMode() == cap.AccessMode.GetMode() {
				return true
			}
		}
		return false
	}

	foundAll := true
	for _, c := range volCaps {
		if !hasSupport(c) {
			foundAll = false
		}
	}
	return foundAll
}

// pickAvailabilityZone selects 1 zone given topology requirement.
// if not found or topology requirement is not zone format, empty string is returned.
func pickAvailabilityZone(requirement *csi.TopologyRequirement, region string) string {
	if requirement == nil {
		return ""
	}
	for _, topology := range requirement.GetPreferred() {
		if zone, exists := topology.GetSegments()[topologyKey]; exists {
			if isAvailabilityZone(zone, region) {
				return zone
			}
		}
	}
	for _, topology := range requirement.GetRequisite() {
		if zone, exists := topology.GetSegments()[topologyKey]; exists {
			if isAvailabilityZone(zone, region) {
				return zone
			}
		}
	}
	return ""
}

func getCachingMode(attributes map[string]string) (compute.CachingTypes, error) {
	var (
		cachingMode v1.AzureDataDiskCachingMode
		err         error
	)

	for k, v := range attributes {
		switch strings.ToLower(k) {
		case "cachingmode":
			cachingMode = v1.AzureDataDiskCachingMode(v)
		}
	}

	cachingMode, err = normalizeCachingMode(cachingMode)
	return compute.CachingTypes(cachingMode), err
}

func generateCSISnapshot(sourceVolumeID string, snapshot *compute.Snapshot) (*csi.Snapshot, error) {
	if snapshot == nil || snapshot.SnapshotProperties == nil {
		return nil, fmt.Errorf("snapshot property is nil")
	}

	tp, err := ptypes.TimestampProto(snapshot.SnapshotProperties.TimeCreated.ToTime())
	if err != nil {
		return nil, fmt.Errorf("Failed to covert creation timestamp: %v", err)
	}
	ready, _ := isCSISnapshotReady(*snapshot.SnapshotProperties.ProvisioningState)

	if snapshot.SnapshotProperties.DiskSizeGB == nil {
		return nil, fmt.Errorf("diskSizeGB of snapshot property is nil")
	}

	if sourceVolumeID == "" {
		sourceVolumeID = getSourceVolumeId(snapshot)
	}

	return &csi.Snapshot{
		SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.SnapshotProperties.DiskSizeGB)),
		SnapshotId:     *snapshot.ID,
		SourceVolumeId: sourceVolumeID,
		CreationTime:   tp,
		ReadyToUse:     ready,
	}, nil
}

func isCSISnapshotReady(state string) (bool, error) {
	switch strings.ToLower(state) {
	case "succeeded":
		return true, nil
	default:
		return false, nil
	}
}

func (d *Driver) extractSnapshotInfo(snapshotID string) (string, string, error) {
	var snapshotName, resourceGroup string
	var err error
	if !strings.Contains(snapshotID, "/subscriptions/") {
		snapshotName = snapshotID
		resourceGroup = d.cloud.ResourceGroup
	} else {
		if snapshotName, err = getSnapshotName(snapshotID); err != nil {
			return "", "", err
		}
		if resourceGroup, err = getResourceGroupFromURI(snapshotID); err != nil {
			return "", "", err
		}
	}
	return snapshotName, resourceGroup, err
}

// There are 4 scenarios for listing snapshots.
// 1. StartingToken is null, and MaxEntries is null. Return all snapshots from zero.
// 2. StartingToken is null, and MaxEntries is not null. Return `MaxEntries` snapshots from zero.
// 3. StartingToken is not null, and MaxEntries is null. Return all snapshots from `StartingToken`.
// 4. StartingToken is not null, and MaxEntries is not null. Return `MaxEntries` snapshots from `StartingToken`.
func getEntriesAndNextToken(req *csi.ListSnapshotsRequest, snapshots []compute.Snapshot) (*csi.ListSnapshotsResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.Aborted, "request is nil")
	}

	var err error
	start := 0
	if req.StartingToken != "" {
		start, err = strconv.Atoi(req.StartingToken)
		if err != nil {
			return nil, status.Errorf(codes.Aborted, "ListSnapshots starting token(%s) parsing with error: %v", req.StartingToken, err)

		}
		if start >= len(snapshots) {
			return nil, status.Errorf(codes.Aborted, "ListSnapshots starting token(%d) is greater than total number of snapshots", start)
		}
		if start < 0 {
			return nil, status.Errorf(codes.Aborted, "ListSnapshots starting token(%d) can not be negative", start)
		}
	}
	perPage := 0
	if req.MaxEntries > 0 {
		perPage = int(req.MaxEntries)
	}

	match := false
	entries := []*csi.ListSnapshotsResponse_Entry{}
	for i, snapshot := range snapshots {
		if req.SourceVolumeId == getSourceVolumeId(&snapshot) {
			match = true
		}
		csiSnapshot, err := generateCSISnapshot(req.SourceVolumeId, &snapshot)
		if err != nil {
			return nil, fmt.Errorf("failed to generate snapshot entry: %v", err)
		}
		if i >= start && (perPage == 0 || (perPage > 0 && i < start+perPage)) {
			entries = append(entries, &csi.ListSnapshotsResponse_Entry{Snapshot: csiSnapshot})
		}
	}
	// return empty if SourceVolumeId is not empty and does not match the listed snapshot's SourceVolumeId
	if req.SourceVolumeId != "" && !match {
		return &csi.ListSnapshotsResponse{}, nil
	}

	nextToken := len(snapshots)
	if start+perPage < len(snapshots) {
		nextToken = start + perPage
	}

	listSnapshotResp := &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: strconv.Itoa(nextToken),
	}

	return listSnapshotResp, nil
}
