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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/mount-utils"
	testingexec "k8s.io/utils/exec/testing"

	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
	csicommon "sigs.k8s.io/azuredisk-csi-driver/pkg/csi-common"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/mounter"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/optimization"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/optimization/mockoptimization"
	volumehelper "sigs.k8s.io/azuredisk-csi-driver/pkg/util"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient"
	azcache "sigs.k8s.io/cloud-provider-azure/pkg/cache"
	azure "sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

const (
	fakeDriverName    = "disk.csi.azure.com"
	fakeNodeID        = "fakeNodeID"
	fakeDriverVersion = "fakeDriverVersion"
)

var (
	stdVolumeCapability = &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}
	stdVolumeCapabilities = []*csi.VolumeCapability{
		stdVolumeCapability,
	}
	stdCapacityRange = &csi.CapacityRange{
		RequiredBytes: volumehelper.GiBToBytes(10),
		LimitBytes:    volumehelper.GiBToBytes(15),
	}
)

// FakeDriver defines an interface unit tests use to test the implementation of the Azure Disk CSI Driver.
type FakeDriver interface {
	CSIDriver

	GetSourceDiskSize(ctx context.Context, subsID, resourceGroup, diskName string, curDepth, maxDepth int) (*int32, *armcompute.Disk, error)
	SetWaitForSnapshotReady(bool)
	GetWaitForSnapshotReady() bool
	GetDiskController() *ManagedDiskController
	GetMigrationMonitor() *MigrationProgressMonitor
	SetMigrationMonitor(*MigrationProgressMonitor)
	RecoverMigrationMonitor(ctx context.Context) error

	setNextCommandOutputScripts(scripts ...testingexec.FakeAction)

	getVolumeLocks() *volumehelper.VolumeLocks
	setControllerCapabilities([]*csi.ControllerServiceCapability)
	setNodeCapabilities([]*csi.NodeServiceCapability)
	setName(string)
	setNodeID(string)
	setVersion(version string)
	getCloud() *azure.Cloud
	setCloud(*azure.Cloud)
	getClientFactory() azclient.ClientFactory
	getMounter() *mount.SafeFormatAndMount
	setMounter(*mount.SafeFormatAndMount)
	setPerfOptimizationEnabled(bool)
	getDeviceHelper() optimization.Interface
	getHostUtil() hostUtil

	checkDiskCapacity(context.Context, string, string, string, int) (bool, error)
	checkDiskExists(ctx context.Context, diskURI string) (*armcompute.Disk, error)
	waitForSnapshotReady(context.Context, string, string, string, time.Duration, time.Duration) error
	getSnapshotByID(context.Context, string, string, string, string) (*csi.Snapshot, error)
	getSnapshot(context.Context, string) (*armcompute.Snapshot, error)
	ensureMountPoint(string) (bool, error)
	ensureBlockTargetFile(string) error
	getDevicePathWithLUN(lunStr string) (string, error)
	setThrottlingCache(key string, value string)
	getUsedLunsFromVolumeAttachments(context.Context, string) ([]int, error)
	getUsedLunsFromNode(context.Context, types.NodeName) ([]int, error)
	validateBlockDeviceSize(devicePath string, requestGiB int64) (int64, error)
}

type fakeDriver struct {
	Driver
}

// NewFakeDriver returns a driver implementation suitable for use in unit tests.
func NewFakeDriver(ctrl *gomock.Controller) (FakeDriver, error) {
	driver := fakeDriver{}
	driver.Name = fakeDriverName
	driver.Version = fakeDriverVersion
	driver.NodeID = fakeNodeID
	driver.CSIDriver = *csicommon.NewFakeCSIDriver()
	driver.volumeLocks = volumehelper.NewVolumeLocks()
	driver.VolumeAttachLimit = -1
	driver.supportZone = true
	driver.ioHandler = azureutils.NewFakeIOHandler()
	driver.hostUtil = azureutils.NewFakeHostUtil()
	driver.useCSIProxyGAInterface = true
	driver.allowEmptyCloudConfig = true
	driver.shouldWaitForSnapshotReady = true
	driver.endpoint = "tcp://127.0.0.1:0"
	driver.disableAVSetNodes = true
	driver.kubeClient = fake.NewSimpleClientset()
	driver.enableMigrationMonitor = true

	driver.cloud = azure.GetTestCloud(ctrl)
	driver.diskController = NewManagedDiskController(driver.cloud)
	driver.clientFactory = driver.cloud.ComputeClientFactory

	mounter, err := mounter.NewSafeMounter(true, true, driver.useCSIProxyGAInterface, int(driver.maxConcurrentFormat), time.Duration(driver.concurrentFormatTimeout)*time.Second)
	if err != nil {
		return nil, err
	}

	driver.mounter = mounter

	cache, err := azcache.NewTimedCache(time.Minute, func(_ context.Context, _ string) (interface{}, error) {
		return nil, nil
	}, false)
	if err != nil {
		return nil, err
	}
	driver.throttlingCache = cache
	driver.checkDiskLunThrottlingCache = cache
	driver.deviceHelper = mockoptimization.NewMockInterface(ctrl)

	driver.AddControllerServiceCapabilities(
		[]csi.ControllerServiceCapability_RPC_Type{
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
			csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
			csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
			csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
			csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
			csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
			csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES,
			csi.ControllerServiceCapability_RPC_MODIFY_VOLUME,
		})
	driver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER})
	driver.AddNodeServiceCapabilities([]csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
	})

	return &driver, nil
}

func (d *fakeDriver) setNextCommandOutputScripts(scripts ...testingexec.FakeAction) {
	d.mounter.Exec.(*mounter.FakeSafeMounter).SetNextCommandOutputScripts(scripts...)
}

func (d *fakeDriver) setThrottlingCache(key string, value string) {
	d.throttlingCache.Set(key, value)
}
func (d *fakeDriver) getClientFactory() azclient.ClientFactory {
	return d.clientFactory
}

func createVolumeCapabilities(accessMode csi.VolumeCapability_AccessMode_Mode) []*csi.VolumeCapability {
	return []*csi.VolumeCapability{
		createVolumeCapability(accessMode),
	}
}

func createVolumeCapability(accessMode csi.VolumeCapability_AccessMode_Mode) *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: accessMode,
		},
	}
}

func (d *fakeDriver) SetWaitForSnapshotReady(shouldWait bool) {
	d.shouldWaitForSnapshotReady = shouldWait
}

func (d *fakeDriver) GetWaitForSnapshotReady() bool {
	return d.shouldWaitForSnapshotReady
}

func (d *fakeDriver) GetDiskController() *ManagedDiskController {
	if d.diskController == nil {
		d.diskController = NewManagedDiskController(d.cloud)
	}
	return d.diskController
}

func (d *fakeDriver) GetMigrationMonitor() *MigrationProgressMonitor {
	if d.migrationMonitor == nil {
		d.migrationMonitor = NewMigrationProgressMonitor(d.kubeClient, d.eventRecorder, d.GetDiskController())
	}
	return d.migrationMonitor
}

func (d *fakeDriver) SetMigrationMonitor(monitor *MigrationProgressMonitor) {
	if monitor == nil {
		d.migrationMonitor = NewMigrationProgressMonitor(d.kubeClient, d.eventRecorder, d.GetDiskController())
	} else {
		d.migrationMonitor = monitor
	}
}

func (d *fakeDriver) RecoverMigrationMonitor(ctx context.Context) error {
	return d.recoverMigrationMonitorsFromLabels(ctx)
}
