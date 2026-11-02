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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/optimization"
	volumehelper "sigs.k8s.io/azuredisk-csi-driver/pkg/util"
	azcache "sigs.k8s.io/cloud-provider-azure/pkg/cache"
	azure "sigs.k8s.io/cloud-provider-azure/pkg/provider"

	"github.com/container-storage-interface/spec/lib/go/csi"
	directvolume "github.com/kata-containers/kata-containers/src/runtime/pkg/direct-volume"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kwait "k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
	consts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
	csiMetrics "sigs.k8s.io/azuredisk-csi-driver/pkg/metrics"
)

const (
	defaultLinuxFsType              = "ext4"
	defaultWindowsFsType            = "ntfs"
	defaultAzureVolumeLimit         = 16
	volumeOperationAlreadyExistsFmt = "An operation with the given Volume ID %s already exists"
	attachOperation                 = "Attach"
	detachOperation                 = "Detach"
)

// Define the request payload structure
type DiskOp struct {
	BlobURL         string `json:"blobUrl"`
	ClaimIdentifier string `json:"claimIdentifier,omitempty"`
	AttachSequence  int    `json:"attachSequence"`
	Action          string `json:"action"`
	CachePolicy     string `json:"cachePolicy"`
}

type WireserverRequest struct {
	VMAccessToken string             `json:"vmAccessToken"`
	DiskOps       map[string]*DiskOp `json:"diskOps"`
}

// DiskOperationRequest captures disk details required for an attach/detach request.
type DiskOperationRequest struct {
	DiskURI         string
	BlobURL         string
	ClaimIdentifier string
	AttachSequence  int
	CachePolicy     string
}

// DiskStatus represents the status information for a single disk
type DiskStatus struct {
	Status        AttachmentStatus `json:"status"`
	StatusMessage string           `json:"status_message"`
	LUN           int              `json:"lun"`
	Error         *DiskError       `json:"error,omitempty"`
}

// DiskError represents a per-disk error in the wireserver response (HTTP 2XX partial failure)
type DiskError struct {
	DiskErrorType DiskErrorType `json:"diskErrorType"`
	SeverityHint  SeverityHint  `json:"severityHint"`
	Message       string        `json:"message"`
}

type DiskErrorType int

const (
	QADDiskErrUnknown                 DiskErrorType = 0
	QADDiskErrInvalidArgs             DiskErrorType = 1
	QADDiskErrInternal                DiskErrorType = 2
	QADDiskErrXFEAuthFailure          DiskErrorType = 3
	QADDiskErrAttachSequenceMismatch  DiskErrorType = 4
	QADDiskErrClaimIdentifierMismatch DiskErrorType = 5
	QADDiskErrFetchDSTSToken          DiskErrorType = 6
	QADDiskErrDiskNotFound            DiskErrorType = 7
	QADDiskErrNoLUNAvailable          DiskErrorType = 8
)

// QADErrorResponse represents the JSON error response from QAD agent (HTTP non-2XX)
type QADErrorResponse struct {
	RequestErrorType RequestErrorType `json:"requestErrorType"`
	SeverityHint     SeverityHint     `json:"severityHint"`
	Message          string           `json:"message"`
}

type RequestErrorType int

const (
	QADRequestErrUnknown            RequestErrorType = 0
	QADRequestErrInvalidArgs        RequestErrorType = 1
	QADRequestErrInternal           RequestErrorType = 2
	QADRequestErrFetchAttachedDisks RequestErrorType = 3
	QADRequestErrFetchMadariCGS     RequestErrorType = 4
	QADRequestErrFetchVMMetadata    RequestErrorType = 5
	QADRequestErrFetchDeployedVMs   RequestErrorType = 6
	QADRequestErrVMNotInCCF         RequestErrorType = 7
	QADRequestErrFetchVMAccessToken RequestErrorType = 8
	QADRequestErrMadariCGSPublish   RequestErrorType = 9
)

type SeverityHint string

const (
	SeverityHintRetriable SeverityHint = "RETRIABLE"
	SeverityHintFatal     SeverityHint = "FATAL"
)

type AttachmentStatus string

const (
	AttachmentStatusAttached  AttachmentStatus = "DISK_STATUS_ATTACHED"
	AttachmentStatusAttaching AttachmentStatus = "DISK_STATUS_ATTACHING"
	AttachmentStatusDetaching AttachmentStatus = "DISK_STATUS_DETACHING"
	AttachmentStatusDetached  AttachmentStatus = "DISK_STATUS_DETACHED"
	AttachmentStatusError     AttachmentStatus = "DISK_STATUS_ERR"
)

// WireserverDiskStatusResponse represents the response from wireserver GET call
// The key is the disk resource ID (e.g., "/subscriptions/.../disks/disk-name")
type WireserverDiskStatusResponse map[string]*DiskStatus

func getDefaultFsType() string {
	if runtime.GOOS == "windows" {
		return defaultWindowsFsType
	}

	return defaultLinuxFsType
}

// postAttachErrorCode returns the gRPC code for a NodeStage failure that happens
// after the node-driven (QAD) physical attach already succeeded. A retryable code
// (Unavailable) keeps kubelet's device state uncertain, which guarantees
// NodeUnstage (and the QAD detach) runs on teardown so the attach is not
// stranded. Controller-driven volumes keep Internal because the controller, not
// the node, performs the detach.
func postAttachErrorCode(qadAttached bool) codes.Code {
	if qadAttached {
		return codes.Unavailable
	}
	return codes.Internal
}

// NodeStageVolume mount disk device to a staging path
func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	target := req.GetStagingTargetPath()
	if len(target) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Staging target not provided")
	}

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
	}

	params := req.GetVolumeContext()
	maxShares, err := azureutils.GetMaxShares(params)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "MaxShares value not supported")
	}

	if err := azureutils.IsValidVolumeCapabilities([]*csi.VolumeCapability{volumeCapability}, maxShares); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Resolve fsType before device discovery so a bad value fails fast.
	fstype, mountFlags, err := resolveFSType(volumeCapability, params)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	mc := csiMetrics.NewCSIMetricContext("node_stage_volume").WithBasicVolumeInfo(d.cloud.ResourceGroup, "", d.Name)
	isOperationSucceeded := false
	defer func() {
		mc.WithAdditionalVolumeInfo(consts.VolumeID, volumeID).Observe(isOperationSucceeded)
	}()

	// Label the root span so this trace can be correlated by volume.
	oteltrace.SpanFromContext(ctx).SetAttributes(attribute.String(attrVolumeID, volumeID))
	if _, _, diskName, err := azureutils.GetInfoFromURI(volumeID); err == nil {
		ctx = withDiskCorrelation(ctx, diskName)
	}

	if acquired := d.volumeLocks.TryAcquire(volumeID); !acquired {
		return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, volumeID)
	}
	defer d.volumeLocks.Release(volumeID)

	// Ensure this is idempotent for Kata mounts (otherwise we might
	// attempt to remount and reformat a disk that is owned by a guest
	// VM, e.g. if the driver restarts).
	// Block volumes are ignored as they don't use MountInfo.
	// This runs before any PV lookup or QAD/LUN resolution, since a
	// Kata-owned volume needs no further work here.
	if d.kataSupported() && volumeCapability.GetBlock() == nil {
		assigned, err := d.kataDirectVolume.IsVolumeMountedByID(volumeID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "could not check Kata assignment for %s: %v", volumeID, err)
		}
		if assigned {
			isOperationSucceeded = true
			return &csi.NodeStageVolumeResponse{}, nil
		}
	}

	// A controller-provided LUN identifies the controller-driven path. Only consult
	// PV metadata for a potential QAD volume when no LUN was provided.
	lun, hasLUN := req.PublishContext[consts.LUN]
	var pv *v1.PersistentVolume
	if !hasLUN && d.nodeDrivenAttachDetachEnabled {
		pv, err = d.getPVFromDiskURI(ctx, volumeID)
		if err != nil && !errors.Is(err, errPVNotFound) {
			return nil, status.Errorf(codes.Internal, "NodeStageVolume: failed to get PV from diskURI %s: %v", volumeID, err)
		}
	}

	// qadAttached records that the node-driven (QAD) physical attach already
	// succeeded, so post-attach failures return a retryable code (see
	// postAttachErrorCode) to keep kubelet's device state uncertain and ensure
	// NodeUnstage still runs to detach.
	qadAttached := false

	if pv != nil && d.hasQADInfo(pv) {
		blobURL := pv.Annotations[consts.BlobURLAnnotation]
		claimIdentifier := pv.Annotations[consts.ClaimIdentifierAnnotation]
		cachePolicy, err := getQADCachePolicy(pv)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failed to determine cache policy: %v", err)
		}
		attachSequenceVal, err := incrementAttachSequenceAnnotation(ctx, d.kubeClient, pv)
		if err != nil {
			klog.Errorf("NodeStageVolume: failed to increment attach-sequence for volume %s: %v", volumeID, err)
			return nil, status.Error(codes.Internal, "failed to increment attach-sequence")
		}
		klog.V(2).Infof("NodeStageVolume: volume %s is using QAD path, making POST call to wireserver with attach-sequence %d", volumeID, attachSequenceVal)

		attachTimer := time.Now()
		statusResp, err := d.executeQADDiskOperation(ctx, DiskOperationRequest{
			DiskURI:         volumeID,
			BlobURL:         blobURL,
			ClaimIdentifier: claimIdentifier,
			AttachSequence:  attachSequenceVal,
			CachePolicy:     string(cachePolicy),
		}, attachOperation)

		if err != nil {
			klog.Errorf("NodeStageVolume: failed to make POST call to wireserver for volume %s: %v", volumeID, err)
			return nil, err
		}

		if statusResp.Status == AttachmentStatusAttaching {
			// Wait for the disk to be attached
			if err = kwait.PollUntilContextTimeout(ctx, 250*time.Millisecond, 15*time.Second, true, func(pollCtx context.Context) (bool, error) {
				diskState, err := getDiskState(pollCtx, *d.httpClient, volumeID)
				if err != nil {
					if status.Code(err) == codes.NotFound {
						klog.V(4).Infof("NodeStageVolume: QAD status does not yet contain volume %s", volumeID)
						return false, nil
					}
					klog.Errorf("NodeStageVolume: failed to get disk state for volume %s: %v", volumeID, err)
					return false, err
				}
				if diskState == nil {
					klog.V(4).Infof("NodeStageVolume: QAD status does not yet contain volume %s", volumeID)
					return false, nil
				}
				switch diskState.Status {
				case AttachmentStatusAttached:
					// Disk is attached, get the lun number
					klog.Infof("NodeStageVolume: Latency observed for attach operation of disk %s is %v", volumeID, time.Since(attachTimer).Milliseconds())
					lun = strconv.Itoa(diskState.LUN)
					return true, nil
				case AttachmentStatusError:
					klog.Errorf("NodeStageVolume: QAD attach failed for volume %s: status=%s, message=%q, error=%+v", volumeID, diskState.Status, diskState.StatusMessage, diskState.Error)
					return false, status.Errorf(codes.Internal, "NodeStageVolume: QAD attach failed for volume %q with status %q", volumeID, diskState.Status)
				default:
					// Wait for the disk to be attached
					return false, nil
				}
			}); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return nil, status.Errorf(codes.DeadlineExceeded, "NodeStageVolume: Timed out waiting for disk: %s to be attached on node: %s", volumeID, d.NodeID)
				}
				return nil, status.Errorf(codes.Internal, "NodeStageVolume: Error occurred while waiting for disk: %s to be attached on node: %s, error: %v", volumeID, d.NodeID, err)
			}

		} else if statusResp.Status == AttachmentStatusAttached {
			klog.Infof("NodeStageVolume: Latency observed for attach operation of disk %s is %v", volumeID, time.Since(attachTimer).Milliseconds())
			lun = strconv.Itoa(statusResp.LUN)
		} else {
			klog.Errorf("NodeStageVolume: QAD attach failed for volume %s: status=%s, message=%q, error=%+v", volumeID, statusResp.Status, statusResp.StatusMessage, statusResp.Error)
			return nil, status.Errorf(codes.Internal, "NodeStageVolume: QAD attach failed for volume %q with status %q", volumeID, statusResp.Status)
		}

		// The QAD physical attach succeeded; later failures must be retryable so
		// kubelet marks the device uncertain and still runs NodeUnstage to detach.
		qadAttached = true
	} else if !hasLUN {
		return nil, status.Error(codes.InvalidArgument, "lun not provided")
	}

	_, devSpan := startSpan(ctx, "waitForDevice", attribute.String("lun", lun))
	source, err := d.getDevicePathWithLUN(lun)
	recordSpanResult(devSpan, err)
	devSpan.End()
	if err != nil {
		return nil, status.Errorf(postAttachErrorCode(qadAttached), "failed to find disk on lun %s. %v", lun, err)
	}

	// If perf optimizations are enabled
	// tweak device settings to enhance performance
	if d.getPerfOptimizationEnabled() {
		profile, accountType, diskSizeGibStr, diskIopsStr, diskBwMbpsStr, deviceSettings, err := optimization.GetDiskPerfAttributes(req.GetVolumeContext())
		if err != nil {
			return nil, status.Errorf(postAttachErrorCode(qadAttached), "failed to get perf attributes for %s. Error: %v", source, err)
		}

		if d.getDeviceHelper().DiskSupportsPerfOptimization(profile, accountType) {
			if err := d.getDeviceHelper().OptimizeDiskPerformance(d.getNodeInfo(), source, profile, accountType,
				diskSizeGibStr, diskIopsStr, diskBwMbpsStr, deviceSettings); err != nil {
				return nil, status.Errorf(postAttachErrorCode(qadAttached), "failed to optimize device performance for target(%s) error(%s)", source, err)
			}
		} else {
			klog.V(6).Infof("NodeStageVolume: perf optimization is disabled for %s. perfProfile %s accountType %s", source, profile, accountType)
		}
	}

	// If the access type is block, do nothing for stage
	switch req.GetVolumeCapability().GetAccessType().(type) {
	case *csi.VolumeCapability_Block:
		isOperationSucceeded = true
		return &csi.NodeStageVolumeResponse{}, nil
	}

	mnt, err := d.ensureMountPoint(target)
	if err != nil {
		return nil, status.Errorf(postAttachErrorCode(qadAttached), "could not mount target %q: %v", target, err)
	}
	if mnt {
		klog.V(2).Infof("NodeStageVolume: already mounted on target %s", target)
		isOperationSucceeded = true
		return &csi.NodeStageVolumeResponse{}, nil
	}

	options := collectMountOptions(fstype, mountFlags)

	// If partition is specified, should mount it only instead of the entire disk.
	if partition, ok := req.GetVolumeContext()[consts.VolumeAttributePartition]; ok {
		source = source + "-part" + partition
	}

	// FormatAndMount will format only if needed
	klog.V(2).Infof("NodeStageVolume: formatting %s and mounting at %s with mount options(%s)", source, target, options)
	_, fmtSpan := startSpan(ctx, "formatAndMount",
		attribute.String("fs_type", fstype))
	err = d.formatAndMount(source, target, fstype, options)
	recordSpanResult(fmtSpan, err)
	fmtSpan.End()
	if err != nil {
		return nil, status.Errorf(postAttachErrorCode(qadAttached), "could not format %s(lun: %s), and mount it at %s, failed with %v", source, lun, target, err)
	}
	klog.V(2).Infof("NodeStageVolume: format %s and mounting at %s successfully.", source, target)

	var needResize bool
	if required, ok := req.GetVolumeContext()[consts.ResizeRequired]; ok && strings.EqualFold(required, consts.TrueValue) {
		needResize = true
	}
	if !needResize {
		// Filesystem resize is required after snapshot restore / volume clone
		// https://github.com/kubernetes/kubernetes/issues/94929
		if needResize, err = needResizeVolume(source, target, d.mounter); err != nil {
			klog.Errorf("NodeStageVolume: could not determine if volume %s needs to be resized: %v", volumeID, err)
		}
	}

	// if resize is required, resize filesystem
	if needResize {
		klog.V(2).Infof("NodeStageVolume: fs resize initiating on target(%s) volumeid(%s)", target, volumeID)
		_, resizeSpan := startSpan(ctx, "resizeVolume")
		err = resizeVolume(source, target, d.mounter)
		recordSpanResult(resizeSpan, err)
		resizeSpan.End()
		if err != nil {
			return nil, status.Errorf(postAttachErrorCode(qadAttached), "NodeStageVolume: could not resize volume %s (%s):  %v", source, target, err)
		}
		klog.V(2).Infof("NodeStageVolume: fs resize successful on target(%s) volumeid(%s).", target, volumeID)
	}
	isOperationSucceeded = true
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume unmount disk device from a staging path
func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	stagingTargetPath := req.GetStagingTargetPath()
	if len(stagingTargetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Staging target not provided")
	}

	mc := csiMetrics.NewCSIMetricContext("node_unstage_volume").WithBasicVolumeInfo(d.cloud.ResourceGroup, "", d.Name)
	isOperationSucceeded := false
	defer func() {
		mc.WithAdditionalVolumeInfo(consts.VolumeID, volumeID).Observe(isOperationSucceeded)
	}()

	// Label the root span and stamp disk.name so teardown correlates by disk.
	oteltrace.SpanFromContext(ctx).SetAttributes(attribute.String(attrVolumeID, volumeID))
	if _, _, diskName, err := azureutils.GetInfoFromURI(volumeID); err == nil {
		ctx = withDiskCorrelation(ctx, diskName)
	}

	if acquired := d.volumeLocks.TryAcquire(volumeID); !acquired {
		return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, volumeID)
	}
	defer d.volumeLocks.Release(volumeID)

	klog.V(2).Infof("NodeUnstageVolume: unmounting %s", stagingTargetPath)
	// Unmount and invalidate the backing device so a fast LUN/namespace reuse can't serve stale content.
	_, umSpan := startSpan(ctx, "unmount")
	err := unmountAndInvalidateDevice(stagingTargetPath, d.ioHandler, d.mounter)
	recordSpanResult(umSpan, err)
	umSpan.End()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount staging target %q: %v", stagingTargetPath, err)
	}
	klog.V(2).Infof("NodeUnstageVolume: unmount %s successfully", stagingTargetPath)

	// QAD detach is only performed when the feature gate is enabled; otherwise the
	// controller performs the detach and no PV lookup is needed here.
	var pv *v1.PersistentVolume
	if d.nodeDrivenAttachDetachEnabled {
		var err error
		pv, err = d.getPVFromDiskURI(ctx, volumeID)
		if errors.Is(err, errPVMetadataUnavailable) {
			return nil, status.Error(codes.FailedPrecondition, "Kubernetes client is required to read QAD volume metadata")
		}
		if errors.Is(err, errPVNotFound) {
			return nil, status.Errorf(codes.Unavailable, "NodeUnstageVolume: PV metadata for volume %s is unavailable; cannot determine whether QAD detach is required", volumeID)
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "NodeUnstageVolume: failed to get PV from diskURI %s: %v", volumeID, err)
		}
	}

	if pv != nil && d.hasQADInfo(pv) {
		blobURL := pv.Annotations[azureconstants.BlobURLAnnotation]
		claimIdentifier := pv.Annotations[azureconstants.ClaimIdentifierAnnotation]
		cachePolicy, err := getQADCachePolicy(pv)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to determine cache policy for volume %s: %v", volumeID, err)
		}
		attachSequenceVal, err := incrementAttachSequenceAnnotation(ctx, d.kubeClient, pv)
		if err != nil {
			klog.Errorf("NodeUnStageVolume: failed to increment attach-sequence for volume %s: %v", volumeID, err)
			return nil, status.Error(codes.Internal, "failed to increment attach-sequence")
		}
		klog.V(2).Infof("NodeUnStageVolume: volume %s is using QAD path, making POST call to wireserver with attach-sequence %d", volumeID, attachSequenceVal)
		detachTimer := time.Now()
		statusResp, err := d.executeQADDiskOperation(ctx, DiskOperationRequest{
			DiskURI:         volumeID,
			BlobURL:         blobURL,
			ClaimIdentifier: claimIdentifier,
			AttachSequence:  attachSequenceVal,
			CachePolicy:     string(cachePolicy),
		}, detachOperation)
		if err != nil {
			klog.Errorf("NodeUnStageVolume: failed to make POST call to wireserver for volume %s: %v", volumeID, err)
			return nil, err
		}

		if statusResp == nil || statusResp.Status == AttachmentStatusDetached {
			klog.Infof("The volume with id %s is already detached", volumeID)
			klog.Infof("NodeUnStageVolume: Latency observed for detach operation of disk %s is %v", volumeID, time.Since(detachTimer).Milliseconds())
		} else if statusResp.Status == AttachmentStatusDetaching {
			if err = kwait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 15*time.Second, true, func(pollCtx context.Context) (bool, error) {
				diskState, err := getDiskState(pollCtx, *d.httpClient, volumeID)
				if err != nil {
					if status.Code(err) == codes.NotFound {
						klog.Infof("NodeUnStageVolume: Latency observed for detach operation of disk %s is %v", volumeID, time.Since(detachTimer).Milliseconds())
						return true, nil
					}
					klog.Errorf("NodeUnStageVolume: failed to get disk state for volume %s: %v", volumeID, err)
					return false, err
				}
				if diskState == nil {
					klog.Infof("NodeUnStageVolume: Latency observed for detach operation of disk %s is %v", volumeID, time.Since(detachTimer).Milliseconds())
					return true, nil
				}
				switch diskState.Status {
				case AttachmentStatusDetached:
					// The disk is detached now
					klog.Infof("NodeUnStageVolume: Latency observed for detach operation of disk %s is %v", volumeID, time.Since(detachTimer).Milliseconds())
					return true, nil
				case AttachmentStatusError:
					klog.Errorf("NodeUnstageVolume: QAD detach failed for volume %s: status=%s, message=%q, error=%+v", volumeID, diskState.Status, diskState.StatusMessage, diskState.Error)
					return false, status.Errorf(codes.Internal, "NodeUnstageVolume: QAD detach failed for volume %q with status %q", volumeID, diskState.Status)
				default:
					// Disk is still detaching, wait for it to be detached
					return false, nil
				}
			}); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return nil, status.Errorf(codes.DeadlineExceeded, "NodeUnstageVolume: Timed out waiting for disk: %s to be detached from node: %s", volumeID, d.NodeID)
				}
				return nil, status.Errorf(codes.Internal, "NodeUnstageVolume: Error occurred while waiting for disk: %s to be detached from node: %s, error: %v", volumeID, d.NodeID, err)
			}

		} else {
			klog.Errorf("NodeUnstageVolume: QAD detach failed for volume %s: status=%s, message=%q, error=%+v", volumeID, statusResp.Status, statusResp.StatusMessage, statusResp.Error)
			return nil, status.Errorf(codes.Internal, "NodeUnstageVolume: QAD detach failed for volume %q with status %q", volumeID, statusResp.Status)
		}
	}

	isOperationSucceeded = true
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume mount the volume from staging to target path
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	mc := csiMetrics.NewCSIMetricContext("node_publish_volume")
	isOperationSucceeded := false
	defer func() {
		mc.Observe(isOperationSucceeded)
	}()

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in the request")
	}

	// Label the root span so this trace can be correlated by volume, and stamp
	// the disk name onto the span, its child sub-spans and klog lines.
	oteltrace.SpanFromContext(ctx).SetAttributes(attribute.String(attrVolumeID, volumeID))
	if _, _, diskName, err := azureutils.GetInfoFromURI(volumeID); err == nil {
		ctx = withDiskCorrelation(ctx, diskName)
	}

	volumeCapability := req.GetVolumeCapability()
	if volumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}

	params := req.GetVolumeContext()
	maxShares, err := azureutils.GetMaxShares(params)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "MaxShares value not supported")
	}

	if err := azureutils.IsValidVolumeCapabilities([]*csi.VolumeCapability{volumeCapability}, maxShares); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	source := req.GetStagingTargetPath()
	if len(source) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Staging target not provided")
	}

	target := req.GetTargetPath()
	if len(target) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path not provided")
	}

	var kataPod *corev1.Pod
	// Kata already supports raw block volumes through the regular device publish path.
	if d.kataSupported() && volumeCapability.GetBlock() == nil {
		// Stage and publish must not race between releasing the host mount and
		// committing metadata, including when discovery falls back to ordinary CSI.
		if acquired := d.volumeLocks.TryAcquire(volumeID); !acquired {
			return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, volumeID)
		}
		defer d.volumeLocks.Release(volumeID)

		// Ensure this is idempotent and preserve an existing
		// NodePublish (either Kata or non-Kata) on retries before
		// kataGetMountPod can select a runtime.
		//
		// Otherwise, a NodePublish retry could select a different
		// runtime, leading to a double mount (kataGetMountPod fails ->
		// regular publish succeeds -> response lost -> kataGetMountPod
		// retry succeeds -> double mount).
		published, err := d.kataPublished(req)
		if err != nil {
			return nil, err
		}
		if published {
			isOperationSucceeded = true
			return &csi.NodePublishVolumeResponse{}, nil
		}

		if kataPod, err = kataGetMountPod(ctx, d.kubeClient, params); err != nil {
			klog.Warningf("NodePublishVolume: failed to probe pod for Kata mount for %s, falling back to regular mount: %v", target, err)
			// Don't return, fall back to regular handling.
			// TODO: Return an error when Kata V2 is GA?
		}
	}

	if kataPod != nil {
		// Direct assignment requires pod-exclusive access enforced by ReadWriteOncePod.
		if volumeCapability.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
			return nil, status.Error(codes.FailedPrecondition, "volume needs ReadWriteOncePod access mode with Kata")
		}

		// Resolve fsType before device discovery, as in NodeStageVolume.
		fstype, mountFlags, err := resolveFSType(volumeCapability, params)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}

		lun, ok := req.PublishContext[consts.LUN]
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "lun not provided")
		}
		device, err := d.getDevicePathWithLUN(lun)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to find device path with lun %s. %v", lun, err)
		}
		klog.V(2).Infof("NodePublishVolume: found device path %s with lun %s", device, lun)

		// Use the same filesystem partition selected by NodeStageVolume.
		if partition, ok := params[consts.VolumeAttributePartition]; ok {
			device = device + "-part" + partition
		}

		fsGroup, fsGroupChangePolicy := getPodFSGroup(kataPod, volumeCapability, req.GetReadonly())
		metadata := map[string]string{kataVolumeIDKey: volumeID}
		if fsGroup != nil {
			metadata[directvolume.FSGroupMetadataKey] = strconv.FormatInt(*fsGroup, 10)
		}
		if fsGroupChangePolicy != nil {
			metadata[directvolume.FSGroupChangePolicyMetadataKey] = string(*fsGroupChangePolicy)
		}
		if fsGroup != nil {
			// Ensure target is a directory because Kubelet will unconditionally
			// attempt to apply fsGroup permission to that directory after NodePublishVolume.
			mounted, err := d.ensureMountPoint(target)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to create Kata direct volume target %q: %v", target, err)
			}
			if mounted {
				return nil, status.Errorf(codes.FailedPrecondition, "Kata direct volume target %q is already mounted", target)
			}
		}

		// NodeStageVolume mounts the disk so it can be formatted and resized.
		// Direct assignment requires the host to release it before Kata hotplugs it.
		if err := CleanupMountPoint(source, d.mounter, true /*extensiveMountPointCheck*/); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to unmount staging target %q for direct volume: %v", source, err)
		}

		options := kataMountOptions(fstype, mountFlags, req.GetReadonly())

		mountInfo := directvolume.MountInfo{
			VolumeType: kataDirectVolumeType,
			Device:     device,
			FsType:     fstype,
			Metadata:   metadata,
			Options:    options,
		}

		if err := d.kataDirectVolume.AddMountInfo(target, mountInfo); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to add direct volume: %v", err)
		}
		klog.V(2).Infof("NodePublishVolume: add direct volume: %v", mountInfo)

		isOperationSucceeded = true
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if err := preparePublishPath(target, d.mounter); err != nil {
		return nil, status.Errorf(codes.Internal, "Target path could not be prepared: %v", err)
	}

	mountOptions := []string{"bind"}
	if req.GetReadonly() {
		mountOptions = append(mountOptions, "ro")
	}

	switch req.GetVolumeCapability().GetAccessType().(type) {
	case *csi.VolumeCapability_Block:
		lun, ok := req.PublishContext[consts.LUN]
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "lun not provided")
		}
		var err error
		_, devSpan := startSpan(ctx, "waitForDevice", attribute.String("lun", lun))
		source, err = d.getDevicePathWithLUN(lun)
		recordSpanResult(devSpan, err)
		devSpan.End()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to find device path with lun %s. %v", lun, err)
		}
		klog.V(2).Infof("NodePublishVolume [block]: found device path %s with lun %s", source, lun)
		if err = d.ensureBlockTargetFile(target); err != nil {
			return nil, status.Errorf(codes.Internal, "%v", err)
		}
	case *csi.VolumeCapability_Mount:
		if d.kataSupported() {
			// It's possible that a volume went through:
			// - Kata NodePublish
			// - No intermediary NodeStage
			// - Non-Kata NodePublish
			// In this case, we need to restore the staging for the volume.
			if err := d.kataRestoreStaging(req); err != nil {
				return nil, status.Errorf(codes.Internal, "could not restore staging for %s: %v", volumeID, err)
			}
		}
		mnt, err := d.ensureMountPoint(target)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "could not mount target %q: %v", target, err)
		}
		if mnt {
			klog.V(2).Infof("NodePublishVolume: already mounted on target %s", target)
			isOperationSucceeded = true
			return &csi.NodePublishVolumeResponse{}, nil
		}
	}

	klog.V(2).Infof("NodePublishVolume: mounting %s at %s", source, target)
	_, mountSpan := startSpan(ctx, "bindMount")
	err = d.mounter.Mount(source, target, "", mountOptions)
	recordSpanResult(mountSpan, err)
	mountSpan.End()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not mount %q at %q: %v", source, target, err)
	}

	klog.V(2).Infof("NodePublishVolume: mount %s at %s successfully", source, target)
	isOperationSucceeded = true
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmount the volume from the target path
func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	mc := csiMetrics.NewCSIMetricContext("node_unpublish_volume")
	isOperationSucceeded := false
	defer func() {
		mc.Observe(isOperationSucceeded)
	}()

	targetPath := req.GetTargetPath()
	volumeID := req.GetVolumeId()

	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in the request")
	}
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	// Label the root span and stamp disk.name so teardown correlates by disk.
	oteltrace.SpanFromContext(ctx).SetAttributes(attribute.String(attrVolumeID, volumeID))
	if _, _, diskName, err := azureutils.GetInfoFromURI(volumeID); err == nil {
		ctx = withDiskCorrelation(ctx, diskName)
	}

	if d.kataSupported() {
		// Serialize cleanup against NodePublishVolume and NodeStageVolume when
		// recovery RPCs overlap after a restart or timeout.
		if acquired := d.volumeLocks.TryAcquire(volumeID); !acquired {
			return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, volumeID)
		}
		defer d.volumeLocks.Release(volumeID)
	}

	klog.V(2).Infof("NodeUnpublishVolume: unmounting volume %s on %s", volumeID, targetPath)
	extensiveMountPointCheck := true
	if runtime.GOOS == "windows" {
		// on Windows, this parameter indicates whether to unmount volume, not necessary in NodeUnpublishVolume
		extensiveMountPointCheck = false
	}
	_, umSpan := startSpan(ctx, "unmount")
	err := CleanupMountPoint(targetPath, d.mounter, extensiveMountPointCheck)
	recordSpanResult(umSpan, err)
	umSpan.End()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount target %q: %v", targetPath, err)
	}

	if d.kataSupported() {
		if err := d.kataDirectVolume.Remove(targetPath); err != nil { // Remove is idempotent.
			return nil, status.Errorf(codes.Internal, "failed to remove direct volume %q: %v", targetPath, err)
		}
	}

	klog.V(2).Infof("NodeUnpublishVolume: unmount volume %s on %s successfully", volumeID, targetPath)
	isOperationSucceeded = true
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetCapabilities return the capabilities of the Node plugin
func (d *Driver) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: d.NSCap,
	}, nil
}

// NodeGetInfo return info of the node on which this plugin is running
func (d *Driver) NodeGetInfo(ctx context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	topology := &csi.Topology{
		Segments: map[string]string{topologyKey: ""},
	}

	var failureDomainFromLabels, instanceTypeFromLabels string
	var err error

	if d.supportZone {
		var zone cloudprovider.Zone
		if d.getNodeInfoFromLabels {
			failureDomainFromLabels, instanceTypeFromLabels, err = GetNodeInfoFromLabels(ctx, d.NodeID, d.cloud.KubeClient)
		} else {
			if runtime.GOOS == "windows" && (!d.cloud.UseInstanceMetadata || d.cloud.Metadata == nil) {
				zone, err = d.cloud.VMSet.GetZoneByNodeName(ctx, d.NodeID)
			} else {
				zone, err = d.cloud.GetZone(ctx)
			}
			if err != nil {
				klog.Warningf("get zone(%s) failed with: %v, fall back to get zone from node labels", d.NodeID, err)
				failureDomainFromLabels, instanceTypeFromLabels, err = GetNodeInfoFromLabels(ctx, d.NodeID, d.cloud.KubeClient)
			}
		}
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("GetNodeInfoFromLabels on node(%s) failed with %v", d.NodeID, err))
		}
		if zone.FailureDomain == "" {
			zone.FailureDomain = failureDomainFromLabels
		}

		klog.V(2).Infof("NodeGetInfo, nodeName: %s, failureDomain: %s", d.NodeID, zone.FailureDomain)
		if azureutils.IsValidAvailabilityZone(zone.FailureDomain, d.cloud.Location) {
			topology.Segments[topologyKey] = zone.FailureDomain
			topology.Segments[consts.WellKnownTopologyKey] = zone.FailureDomain
		}
	}

	// Lazily compute and cache maxDataDiskCount on first call
	maxDataDiskCount := d.VolumeAttachLimit
	if maxDataDiskCount < 0 {
		var instanceType string
		var err error
		if d.getNodeInfoFromLabels {
			if instanceTypeFromLabels == "" {
				_, instanceTypeFromLabels, err = GetNodeInfoFromLabels(ctx, d.NodeID, d.cloud.KubeClient)
			}
		} else {
			if runtime.GOOS == "windows" && d.cloud.UseInstanceMetadata && d.cloud.Metadata != nil {
				var metadata *azure.InstanceMetadata
				metadata, err = d.cloud.Metadata.GetMetadata(ctx, azcache.CacheReadTypeDefault)
				if err == nil && metadata != nil && metadata.Compute != nil {
					instanceType = metadata.Compute.VMSize
					klog.V(2).Infof("NodeGetInfo: nodeName(%s), VM Size(%s)", d.NodeID, instanceType)
				}
			} else {
				instances, ok := d.cloud.Instances()
				if !ok {
					klog.Warningf("failed to get instances from cloud provider")
				} else {
					instanceType, err = instances.InstanceType(ctx, types.NodeName(d.NodeID))
				}
			}
			if err != nil {
				klog.Warningf("get instance type(%s) failed with: %v", d.NodeID, err)
			}
			if instanceType == "" && instanceTypeFromLabels == "" {
				klog.Warningf("fall back to get instance type from node labels")
				_, instanceTypeFromLabels, err = GetNodeInfoFromLabels(ctx, d.NodeID, d.cloud.KubeClient)
			}
		}
		if err != nil {
			klog.Warningf("GetNodeInfoFromLabels on node(%s) failed with %v", d.NodeID, err)
		}
		if instanceType == "" {
			instanceType = instanceTypeFromLabels
		}
		totalDiskDataCount, _ := GetMaxDataDiskCount(instanceType)
		maxDataDiskCount = totalDiskDataCount - d.ReservedDataDiskSlotNum
	}

	nodeID := d.NodeID
	if d.getNodeIDFromIMDS && d.cloud.UseInstanceMetadata && d.cloud.Metadata != nil {
		metadata, err := d.cloud.Metadata.GetMetadata(ctx, azcache.CacheReadTypeDefault)
		if err == nil && metadata != nil && metadata.Compute != nil {
			klog.V(2).Infof("NodeGetInfo: NodeID(%s), metadata.Compute.Name(%s)", d.NodeID, metadata.Compute.Name)
			if metadata.Compute.Name != "" {
				if metadata.Compute.VMScaleSetName != "" {
					id, err := getVMSSInstanceName(metadata.Compute.Name)
					if err != nil {
						klog.Errorf("getVMSSInstanceName failed with %v", err)
						if nodeID == "" {
							klog.V(2).Infof("NodeGetInfo: NodeID is empty, use metadata.Compute.Name(%s)", metadata.Compute.Name)
							nodeID = metadata.Compute.Name
						}
					} else {
						nodeID = id
					}
				} else {
					nodeID = metadata.Compute.Name
				}
			}
		} else {
			klog.Warningf("get instance type(%s) failed with: %v", d.NodeID, err)
		}
	}

	return &csi.NodeGetInfoResponse{
		NodeId:             nodeID,
		MaxVolumesPerNode:  maxDataDiskCount,
		AccessibleTopology: topology,
	}, nil
}

func GetMaxDataDiskCount(instanceType string) (int64, bool) {
	vmsize := strings.ToUpper(instanceType)
	maxDataDiskCount, exists := maxDataDiskCountMap[vmsize]
	if exists {
		klog.V(5).Infof("got a matching size in getMaxDataDiskCount, VM Size: %s, MaxDataDiskCount: %d", vmsize, maxDataDiskCount)
		return maxDataDiskCount, true
	}

	klog.V(5).Infof("not found a matching size in getMaxDataDiskCount, VM Size: %s, use default volume limit: %d", vmsize, defaultAzureVolumeLimit)
	return defaultAzureVolumeLimit, false
}

func (d *Driver) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if len(req.VolumeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume ID was empty")
	}
	if len(req.VolumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume path was empty")
	}

	if d.kataSupported() {
		// NOTE: This cannot detect CSI block requests passed through Kata.
		if isKataMount, err := d.kataDirectVolume.IsVolumeMountedByID(req.VolumeId); isKataMount {
			return nil, status.Error(codes.Unimplemented, "volume stats are not supported for Kata mounts")
		} else if err != nil {
			klog.Warningf("NodeGetVolumeStats: failed to probe for Kata mount at %s: %v", req.VolumePath, err)
			// Don't return, fall back to regular handling.
		}
	}

	volUsage, err := d.GetVolumeStats(ctx, d.mounter, req.VolumeId, req.VolumePath, d.hostUtil)
	if err != nil {
		klog.Errorf("NodeGetVolumeStats: failed to get volume stats for volume %s path %s: %v", req.VolumeId, req.VolumePath, err)
	}
	return &csi.NodeGetVolumeStatsResponse{
		Usage: volUsage,
	}, err
}

// NodeExpandVolume node expand volume
func (d *Driver) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	// Label the root span and stamp disk.name so expand correlates by disk.
	oteltrace.SpanFromContext(ctx).SetAttributes(attribute.String(attrVolumeID, volumeID))
	if _, _, diskName, err := azureutils.GetInfoFromURI(volumeID); err == nil {
		ctx = withDiskCorrelation(ctx, diskName)
	}

	capacityBytes := req.GetCapacityRange().GetRequiredBytes()
	volSizeBytes := int64(capacityBytes)
	requestGiB := volumehelper.RoundUpGiB(volSizeBytes)

	volumePath := req.GetVolumePath()
	if len(volumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume path must be provided")
	}

	if d.kataSupported() {
		if acquired := d.volumeLocks.TryAcquire(volumeID); !acquired {
			return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, volumeID)
		}
		defer d.volumeLocks.Release(volumeID)

		// NOTE: This cannot detect CSI block requests passed through Kata.
		assigned, err := d.kataDirectVolume.IsVolumeMountedByID(volumeID)
		if err != nil {
			// We must not fall back to non-Kata handling here,
			// as that could resize a disk owned by a guest VM.
			return nil, status.Errorf(codes.Internal, "could not check Kata assignment for %s: %v", volumeID, err)
		}
		if assigned {
			return nil, status.Error(codes.Unimplemented, "volume resize is not supported for Kata mounts")
		}
	}

	isBlock, err := d.getHostUtil().PathIsDevice(volumePath)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to determine device path for volumePath [%v]: %v", volumePath, err)
	}
	if !isBlock {
		volumeCapability := req.GetVolumeCapability()
		if volumeCapability != nil {
			isBlock = volumeCapability.GetBlock() != nil
		}
	}

	if isBlock {
		if d.enableDiskOnlineResize {
			klog.V(2).Infof("NodeExpandVolume begin to rescan all devices on block volume(%s)", volumeID)
			if err := rescanAllVolumes(d.ioHandler); err != nil {
				klog.Errorf("NodeExpandVolume rescanAllVolumes failed with error: %v", err)
			}
		}
		klog.V(2).Infof("NodeExpandVolume skip resize operation on block volume(%s)", volumeID)
		return &csi.NodeExpandVolumeResponse{}, nil
	}

	mc := csiMetrics.NewCSIMetricContext("node_expand_volume").WithBasicVolumeInfo(d.cloud.ResourceGroup, "", d.Name)
	isOperationSucceeded := false
	defer func() {
		mc.WithAdditionalVolumeInfo(consts.VolumeID, volumeID).Observe(isOperationSucceeded)
	}()

	if !d.kataSupported() {
		if acquired := d.volumeLocks.TryAcquire(volumeID); !acquired {
			return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, volumeID)
		}
		defer d.volumeLocks.Release(volumeID)
	}

	devicePath, err := getDevicePathWithMountPath(volumePath, d.mounter)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	if d.enableDiskOnlineResize {
		klog.V(2).Infof("NodeExpandVolume begin to rescan device %s on volume(%s)", devicePath, volumeID)
		_, rescanSpan := startSpan(ctx, "rescanDevice")
		err := rescanVolume(d.ioHandler, devicePath)
		recordSpanResult(rescanSpan, err)
		rescanSpan.End()
		if err != nil {
			klog.Errorf("NodeExpandVolume rescanVolume failed with error: %v", err)
		}
	}

	// CRITICAL SAFETY CHECK: Verify block device size BEFORE filesystem resize
	// This prevents filesystem corruption if the underlying disk hasn't been expanded yet
	actualDevicePath := devicePath
	if runtime.GOOS == "windows" && d.enableWindowsHostProcess {
		// in windows host process mode, this driver could get the volume size from the volume path
		actualDevicePath = volumePath
	}

	currentBlockSizeBytes, err := d.validateBlockDeviceSize(actualDevicePath, requestGiB)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "NodeExpandVolume: block device size did not match requested size: %v", err)
	}

	klog.V(2).Infof("NodeExpandVolume: block device size verified (%d bytes >= %d GiB requested), proceeding with filesystem resize",
		currentBlockSizeBytes, requestGiB)

	// Now safe to resize filesystem since we've verified the block device is large enough
	var retErr error
	_, resizeSpan := startSpan(ctx, "resizeFilesystem")
	err = resizeVolume(devicePath, volumePath, d.mounter)
	recordSpanResult(resizeSpan, err)
	resizeSpan.End()
	if err != nil {
		retErr = status.Errorf(codes.Internal, "could not resize volume %q (%q):  %v", volumeID, devicePath, err)
		klog.Errorf("%v, will continue checking whether the volume has been resized", retErr)
	}

	// Final verification after filesystem resize
	finalBlockSizeBytes, err := d.validateBlockDeviceSize(actualDevicePath, requestGiB)
	if err != nil {
		if retErr != nil {
			return nil, retErr
		}
		return nil, status.Errorf(codes.Internal, "NodeExpandVolume: block device size did not match requested size after filesystem resize: %v", err)
	}

	klog.V(2).Infof("NodeExpandVolume succeeded on resizing volume %v to %v", volumeID, finalBlockSizeBytes)
	isOperationSucceeded = true
	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: finalBlockSizeBytes,
	}, nil
}

// ensureMountPoint: create mount point if not exists
// return <true, nil> if it's already a mounted point otherwise return <false, nil>
func (d *Driver) ensureMountPoint(target string) (bool, error) {
	notMnt, err := d.mounter.IsLikelyNotMountPoint(target)
	if err != nil && !os.IsNotExist(err) {
		if azureutils.IsCorruptedDir(target) {
			notMnt = false
			klog.Warningf("detected corrupted mount for targetPath [%s]", target)
		} else {
			return !notMnt, err
		}
	}

	if runtime.GOOS != "windows" {
		// Check all the mountpoints in case IsLikelyNotMountPoint
		// cannot handle --bind mount
		mountList, err := d.mounter.List()
		if err != nil {
			return !notMnt, err
		}

		targetAbs, err := filepath.Abs(target)
		if err != nil {
			return !notMnt, err
		}

		for _, mountPoint := range mountList {
			if mountPoint.Path == targetAbs {
				notMnt = false
				break
			}
		}
	}

	if !notMnt {
		// testing original mount point, make sure the mount link is valid
		// Use ReadDir(1) instead of full os.ReadDir to avoid expensive directory listing
		f, err := os.Open(target)
		if err == nil {
			defer f.Close()
			_, err = f.ReadDir(1)
			// EOF means empty directory, which is valid
			if err == io.EOF {
				err = nil
			}
		}
		if err == nil {
			klog.V(2).Infof("already mounted to target %s", target)
			return !notMnt, nil
		}
		// mount link is invalid, now unmount and remount later
		klog.Warningf("ReadDir %s failed with %v, unmount this directory", target, err)
		if err := d.mounter.Unmount(target); err != nil {
			klog.Errorf("Unmount directory %s failed with %v", target, err)
			return !notMnt, err
		}
		notMnt = true
		return !notMnt, err
	}

	if runtime.GOOS != "windows" {
		// in windows, we will use mklink to mount, will MkdirAll in Mount func
		if err := volumehelper.MakeDir(target); err != nil {
			klog.Errorf("mkdir failed on target: %s (%v)", target, err)
			return !notMnt, err
		}
	}

	return !notMnt, nil
}

func (d *Driver) formatAndMount(source, target, fstype string, options []string) error {
	return formatAndMount(source, target, fstype, options, d.mounter, d.formatSem, d.formatTimeout)
}

func (d *Driver) getDevicePathWithLUN(lunStr string) (string, error) {
	lun, err := azureutils.GetDiskLUN(lunStr)
	if err != nil {
		return "", err
	}

	scsiHostRescan(d.ioHandler, d.mounter)

	newDevicePath := ""
	err = kwait.PollUntilContextTimeout(context.Background(), 1*time.Second, 2*time.Minute, true, func(context.Context) (bool, error) {
		var err error
		if newDevicePath, err = findDiskByLun(int(lun), d.ioHandler, d.mounter); err != nil {
			return false, fmt.Errorf("azureDisk - findDiskByLun(%v) failed with error(%s)", lun, err)
		}

		// did we find it?
		if newDevicePath != "" {
			return true, nil
		}
		// wait until timeout
		return false, nil
	})
	if errors.Is(err, context.DeadlineExceeded) && newDevicePath == "" {
		err = fmt.Errorf("azureDisk - findDiskByLun(%v) failed within timeout", lun)
	}
	return newDevicePath, err
}

func (d *Driver) ensureBlockTargetFile(target string) error {
	// Since the block device target path is file, its parent directory should be ensured to be valid.
	parentDir := filepath.Dir(target)
	if _, err := d.ensureMountPoint(parentDir); err != nil {
		return status.Errorf(codes.Internal, "could not mount target %q: %v", parentDir, err)
	}
	// Create the mount point as a file since bind mount device node requires it to be a file
	klog.V(2).Infof("ensureBlockTargetFile [block]: making target file %s", target)
	err := volumehelper.MakeFile(target)
	if err != nil {
		if removeErr := os.Remove(target); removeErr != nil {
			return status.Errorf(codes.Internal, "could not remove mount target %q: %v", target, removeErr)
		}
		return status.Errorf(codes.Internal, "could not create file %q: %v", target, err)
	}

	return nil
}

// validateBlockDeviceSize checks if the block device is large enough for the requested size
// Returns the actual block size in bytes if validation passes, otherwise returns an error
func (d *Driver) validateBlockDeviceSize(devicePath string, requestGiB int64) (int64, error) {
	blockSizeBytes, err := getBlockSizeBytes(devicePath, d.mounter)
	if err != nil {
		return 0, status.Error(codes.Internal, fmt.Sprintf("could not get size of block volume at path %s: %v", devicePath, err))
	}

	blockGiB := volumehelper.RoundUpGiB(blockSizeBytes)

	if blockGiB < requestGiB {
		return 0, status.Error(codes.Internal, fmt.Sprintf("block volume at path %s size check failed: current %d GiB < requested %d GiB", devicePath, blockGiB, requestGiB))
	}

	return blockSizeBytes, nil
}

// resolveFSType returns the filesystem type and mount flags to stage a volume with,
// or an empty fsType for the block access type, which has no filesystem. Values outside
// the supported set are rejected, except on Windows where the mounters ignore fsType and
// always format NTFS, so rejecting would break configurations that already worked.
func resolveFSType(volumeCapability *csi.VolumeCapability, params map[string]string) (string, []string, error) {
	// IsValidVolumeCapabilities rejects caps with block and mount both set or both nil,
	// so a nil mount here is the block access type.
	mnt := volumeCapability.GetMount()
	if mnt == nil {
		return "", nil, nil
	}

	fstype := getDefaultFsType()
	if mnt.FsType != "" {
		fstype = mnt.FsType
	}

	if volContextFSType := azureutils.GetFStype(params); volContextFSType != "" {
		// respect "fstype" setting in storage class parameters
		fstype = volContextFSType
	}

	// fstype is attacker-controllable via volume attributes and ends up in mkfs/mount, so allowlist it
	normalized, err := azureutils.NormalizeFSType(fstype)
	if err != nil {
		if runtime.GOOS != "windows" {
			return "", nil, err
		}
		normalized = getDefaultFsType()
		klog.Warningf("NodeStageVolume: %v, falling back to %s", err, normalized)
	}
	return normalized, mnt.MountFlags, nil
}

func collectMountOptions(fsType string, mntFlags []string) []string {
	var options []string
	options = append(options, mntFlags...)

	// By default, xfs does not allow mounting of two volumes with the same filesystem uuid.
	// Force ignore this uuid to be able to mount volume + its clone / restored snapshot on the same node.
	if fsType == "xfs" {
		options = append(options, "nouuid")
	}
	return options
}

func incrementAttachSequenceAnnotation(ctx context.Context, kubeClient clientset.Interface, pv *v1.PersistentVolume) (int, error) {
	klog.Infof("Incrementing attach sequence annotation for PV %s", pv.Name)

	pv = pv.DeepCopy()
	updatedSequence := 0
	attempt := 0
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if attempt > 0 {
			var err error
			pv, err = kubeClient.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
		}
		attempt++

		if pv.Annotations == nil {
			pv.Annotations = make(map[string]string)
		}
		currentAttachSequence := 0
		if val, ok := pv.Annotations[azureconstants.AttachSequenceAnnotation]; ok {
			parsedSequence, parseErr := strconv.Atoi(val)
			if parseErr != nil || parsedSequence < 0 {
				return fmt.Errorf("invalid attach-sequence annotation %q on PersistentVolume %s", val, pv.Name)
			}
			if parsedSequence == int(^uint(0)>>1) {
				return fmt.Errorf("attach-sequence annotation on PersistentVolume %s cannot be incremented", pv.Name)
			}
			currentAttachSequence = parsedSequence
		}
		updatedSequence = currentAttachSequence + 1
		pv.Annotations[azureconstants.AttachSequenceAnnotation] = strconv.Itoa(updatedSequence)

		_, err := kubeClient.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("failed to update PersistentVolume %s: %v", pv.Name, err)
	}

	klog.V(2).Infof("Successfully incremented attach sequence annotation for PV %s to %d", pv.Name, updatedSequence)
	return updatedSequence, nil
}

func getQADCachePolicy(pv *v1.PersistentVolume) (armcompute.CachingTypes, error) {
	if pv != nil && pv.Annotations[azureconstants.QADCachePolicyAnnotation] != "" {
		return azureutils.GetCachingMode(map[string]string{
			consts.CachingModeField: pv.Annotations[azureconstants.QADCachePolicyAnnotation],
		})
	}
	if pv == nil || pv.Spec.CSI == nil {
		return "", fmt.Errorf("PV has no CSI volume source")
	}
	return azureutils.GetCachingMode(pv.Spec.CSI.VolumeAttributes)
}

func (d *Driver) executeQADDiskOperation(ctx context.Context, diskRequest DiskOperationRequest, operationType string) (*DiskStatus, error) {
	// httpClient is only initialized on the node service; guard against a nil
	// dereference if the QAD path is reached without it.
	if d.httpClient == nil {
		return nil, status.Errorf(codes.Internal, "QAD HTTP client is not initialized; node-driven attach/detach is only available on the node service")
	}
	response, err := attachOrDetachDiskInternal(ctx, *d.httpClient, diskRequest, d.cloud.AuthProvider.GetAzIdentity(), operationType)
	if err != nil {
		return nil, err
	}

	var diskStatus *DiskStatus
	for diskURI, status := range response {
		if strings.EqualFold(diskURI, diskRequest.DiskURI) {
			diskStatus = status
			break
		}
	}
	if diskStatus == nil {
		if operationType == detachOperation {
			return nil, nil
		}
		return nil, status.Errorf(codes.Internal, "The response from wireserver doesn't contain a valid status for volume %s", diskRequest.DiskURI)
	}
	if diskStatus.Error != nil {
		grpcCode := mapDiskErrorToCode(diskStatus.Error.DiskErrorType, diskStatus.Error.SeverityHint)
		return nil, status.Errorf(grpcCode,
			"wireserver disk operation failed for %s: errorType=%d, severity=%s, message=%s",
			diskRequest.DiskURI, diskStatus.Error.DiskErrorType, diskStatus.Error.SeverityHint, diskStatus.Error.Message)
	}

	return diskStatus, nil
}

func attachOrDetachDiskInternal(ctx context.Context, client http.Client, diskRequest DiskOperationRequest, cred azcore.TokenCredential, operationType string) (WireserverDiskStatusResponse, error) {
	if diskRequest.DiskURI == "" {
		return WireserverDiskStatusResponse{}, status.Error(codes.InvalidArgument, "disk URI must not be empty")
	}
	if cred == nil {
		return WireserverDiskStatusResponse{}, status.Error(codes.FailedPrecondition, "Azure credential is required for QAD disk operations")
	}

	// Get a fresh token from the credential (SDK handles caching/refresh internally).
	// XFE validates the token audience as "https://management.azure.com/" (with trailing
	// slash); the double slash before /.default makes the resulting aud carry that slash.
	token, err := cred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{"https://management.azure.com//.default"},
	})
	if err != nil {
		return WireserverDiskStatusResponse{}, fmt.Errorf("failed to get VM access token: %v", err)
	}

	diskOps := map[string]*DiskOp{
		diskRequest.DiskURI: {
			BlobURL:         diskRequest.BlobURL,
			ClaimIdentifier: diskRequest.ClaimIdentifier,
			AttachSequence:  diskRequest.AttachSequence,
			Action:          operationType,
			CachePolicy:     diskRequest.CachePolicy,
		},
	}

	request := &WireserverRequest{
		VMAccessToken: token.Token,
		DiskOps:       diskOps,
	}

	requestBody, err := json.Marshal(request)
	if err != nil {
		return WireserverDiskStatusResponse{}, fmt.Errorf("failed to marshal wireserver request: %v", err)
	}

	headers := map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   "csi-node",
	}

	resp, err := makeHTTPRequest(ctx, client, http.MethodPost, consts.QADWireserverEndpoint, requestBody, headers)
	if err != nil {
		return WireserverDiskStatusResponse{}, fmt.Errorf("failed to make HTTP request to wireserver: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return WireserverDiskStatusResponse{}, fmt.Errorf("failed to read response body: %v", err)
	}

	// Log wireserver request and response for debugging (redact token)
	redactedRequest := &WireserverRequest{
		VMAccessToken: "[REDACTED]",
		DiskOps:       request.DiskOps,
	}
	if redactedBody, err := json.Marshal(redactedRequest); err == nil {
		klog.V(2).Infof("Wireserver request: %s", string(redactedBody))
	}
	klog.V(2).Infof("Wireserver response (HTTP %d): %s", resp.StatusCode, string(respBody))

	// Case 1: Request succeeded (HTTP 2XX) - may contain per-disk errors (partial failure)
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		var wireserverDiskStatusResponse WireserverDiskStatusResponse
		if err := json.Unmarshal(respBody, &wireserverDiskStatusResponse); err != nil {
			return WireserverDiskStatusResponse{}, fmt.Errorf("failed to unmarshal wireserver response: %v", err)
		}

		return wireserverDiskStatusResponse, nil
	}

	// Case 2 & 3: Request failed (HTTP non-2XX)
	// Try JSON parse first (Case 3: QAD agent failure)
	var qadError QADErrorResponse
	if err := json.Unmarshal(respBody, &qadError); err == nil && qadError.RequestErrorType > 0 || (err == nil && qadError.SeverityHint != "") {
		grpcCode := mapRequestErrorToCode(qadError.RequestErrorType, qadError.SeverityHint)
		return WireserverDiskStatusResponse{}, status.Errorf(grpcCode,
			"QAD agent error for disk %s: errorType=%d, severity=%s, message=%s",
			diskRequest.DiskURI, qadError.RequestErrorType, qadError.SeverityHint, qadError.Message)
	}

	// Case 2: WireServer XML failure - map HTTP status code
	grpcCode := mapHTTPStatusToCode(resp.StatusCode)
	return WireserverDiskStatusResponse{}, status.Errorf(grpcCode, "wireserver returned HTTP %d: %s", resp.StatusCode, string(respBody))
}

// mapDiskErrorToCode maps a DiskErrorType and SeverityHint to a gRPC status code.
// Used for HTTP 2XX partial failure responses.
func mapDiskErrorToCode(errorType DiskErrorType, severity SeverityHint) codes.Code {
	switch errorType {
	case QADDiskErrUnknown:
		return codes.Unknown
	case QADDiskErrInvalidArgs:
		return codes.InvalidArgument
	case QADDiskErrInternal:
		if severity == SeverityHintRetriable {
			return codes.Unavailable
		}
		return codes.Internal
	case QADDiskErrXFEAuthFailure:
		if severity == SeverityHintRetriable {
			return codes.Unavailable
		}
		return codes.Unauthenticated
	case QADDiskErrAttachSequenceMismatch:
		return codes.Aborted
	case QADDiskErrClaimIdentifierMismatch:
		if severity == SeverityHintRetriable {
			return codes.Unavailable
		}
		return codes.Internal
	case QADDiskErrFetchDSTSToken:
		if severity == SeverityHintRetriable {
			return codes.Unavailable
		}
		return codes.Internal
	case QADDiskErrDiskNotFound:
		return codes.NotFound
	case QADDiskErrNoLUNAvailable:
		return codes.ResourceExhausted
	default:
		return codes.Unknown
	}
}

// mapRequestErrorToCode maps a RequestErrorType and SeverityHint to a gRPC status code.
// Used for HTTP non-2XX QAD JSON error responses.
func mapRequestErrorToCode(errorType RequestErrorType, severity SeverityHint) codes.Code {
	switch errorType {
	case QADRequestErrUnknown:
		return codes.Unknown
	case QADRequestErrInvalidArgs:
		return codes.InvalidArgument
	case QADRequestErrInternal:
		if severity == SeverityHintRetriable {
			return codes.Unavailable
		}
		return codes.Internal
	case QADRequestErrFetchAttachedDisks:
		if severity == SeverityHintRetriable {
			return codes.Unavailable
		}
		return codes.Internal
	case QADRequestErrFetchMadariCGS:
		if severity == SeverityHintRetriable {
			return codes.Unavailable
		}
		return codes.Internal
	case QADRequestErrFetchVMMetadata:
		if severity == SeverityHintRetriable {
			return codes.Unavailable
		}
		return codes.Internal
	case QADRequestErrFetchDeployedVMs:
		if severity == SeverityHintRetriable {
			return codes.Unavailable
		}
		return codes.Internal
	case QADRequestErrVMNotInCCF:
		if severity == SeverityHintRetriable {
			return codes.Unavailable
		}
		return codes.Internal
	case QADRequestErrFetchVMAccessToken:
		if severity == SeverityHintRetriable {
			return codes.Unavailable
		}
		return codes.Unauthenticated
	case QADRequestErrMadariCGSPublish:
		if severity == SeverityHintRetriable {
			return codes.Unavailable
		}
		return codes.Internal
	default:
		return codes.Unknown
	}
}

// mapHTTPStatusToCode maps an HTTP status code to a gRPC status code.
// Used for WireServer XML failure responses (HTTP non-2XX).
func mapHTTPStatusToCode(httpStatus int) codes.Code {
	switch httpStatus {
	case http.StatusBadRequest: // 400
		return codes.InvalidArgument
	case http.StatusUnauthorized: // 401
		return codes.Unauthenticated
	case http.StatusForbidden: // 403
		return codes.Unauthenticated
	case http.StatusNotFound: // 404
		return codes.NotFound
	case http.StatusMethodNotAllowed: // 405
		return codes.InvalidArgument
	case http.StatusGone: // 410
		return codes.Unavailable
	case http.StatusLengthRequired: // 411
		return codes.Internal
	case http.StatusRequestEntityTooLarge: // 413
		return codes.Internal
	case http.StatusRequestURITooLong: // 415
		return codes.Internal
	case http.StatusUnsupportedMediaType: // 422
		return codes.Internal
	case http.StatusTooManyRequests: // 429
		return codes.ResourceExhausted
	case http.StatusBadGateway: // 502
		return codes.Unavailable
	case http.StatusServiceUnavailable: // 503
		return codes.Unavailable
	case http.StatusGatewayTimeout: // 504
		return codes.DeadlineExceeded
	default:
		if httpStatus >= 400 && httpStatus < 500 {
			return codes.InvalidArgument
		}
		return codes.Unknown // Default 5XX
	}
}

func getDiskState(ctx context.Context, client http.Client, diskURI string) (*DiskStatus, error) {
	// Set headers
	headers := map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   "csi-node",
	}

	resp, err := makeHTTPRequest(ctx, client, http.MethodGet, consts.QADWireserverEndpoint, nil, headers)
	if err != nil {
		return nil, fmt.Errorf("failed to make HTTP request to wireserver: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, status.Errorf(mapHTTPStatusToCode(resp.StatusCode), "wireserver returned HTTP %d: %s", resp.StatusCode, string(bytes))
	}

	var wireserverDiskStatusResponse WireserverDiskStatusResponse
	if err := json.Unmarshal(bytes, &wireserverDiskStatusResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal wireserver response: %v", err)
	}

	for responseDiskURI, diskStatus := range wireserverDiskStatusResponse {
		if strings.EqualFold(responseDiskURI, diskURI) {
			return diskStatus, nil
		}
	}

	return nil, status.Errorf(codes.NotFound, "wireserver response does not contain volume %s", diskURI)
}

func makeHTTPRequest(ctx context.Context, client http.Client, method, url string, body []byte, headers map[string]string) (*http.Response, error) {
	// Create the HTTP request
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	return client.Do(req)
}

// getPodFSGroup returns the pod's fsGroup settings for a writable filesystem volume.
func getPodFSGroup(pod *corev1.Pod, volumeCapability *csi.VolumeCapability, readOnly bool) (fsGroup *int64, fsGroupChangePolicy *corev1.PodFSGroupChangePolicy) {
	// fsGroup only applies to writable filesystem volumes.
	if readOnly || volumeCapability == nil || volumeCapability.GetMount() == nil {
		return nil, nil
	}

	if pod == nil || pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.FSGroup == nil {
		return nil, nil
	}

	return pod.Spec.SecurityContext.FSGroup, pod.Spec.SecurityContext.FSGroupChangePolicy
}
