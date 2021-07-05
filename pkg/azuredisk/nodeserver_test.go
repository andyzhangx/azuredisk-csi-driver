/*
Copyright 2019 The Kubernetes Authors.

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
	"log"
	"os"
	"reflect"
	"runtime"
	"syscall"
	"testing"

	"sigs.k8s.io/azuredisk-csi-driver/test/utils/testutil"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	volumehelper "sigs.k8s.io/azuredisk-csi-driver/pkg/util"
)

var (
	sourceTest string
	targetTest string
)

func TestMain(m *testing.M) {
	var err error
	sourceTest, err = testutil.GetWorkDirPath("source_test")
	if err != nil {
		log.Printf("failed to get source test path: %v\n", err)
		os.Exit(1)
	}
	targetTest, err = testutil.GetWorkDirPath("target_test")
	if err != nil {
		log.Printf("failed to get target test path: %v\n", err)
		os.Exit(1)
	}

	_ = m.Run()

}

func TestNodeGetCapabilities(t *testing.T) {
	d, _ := NewFakeDriver(t)
	capType := &csi.NodeServiceCapability_Rpc{
		Rpc: &csi.NodeServiceCapability_RPC{
			Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		},
	}
	capList := []*csi.NodeServiceCapability{{
		Type: capType,
	}}
	d.setNodeCapabilities(capList)
	// Test valid request
	req := csi.NodeGetCapabilitiesRequest{}
	resp, err := d.NodeGetCapabilities(context.Background(), &req)
	assert.NotNil(t, resp)
	assert.Equal(t, resp.Capabilities[0].GetType(), capType)
	assert.NoError(t, err)
}

func TestGetFStype(t *testing.T) {
	tests := []struct {
		options  map[string]string
		expected string
	}{
		{
			nil,
			"",
		},
		{
			map[string]string{},
			"",
		},
		{
			map[string]string{"fstype": ""},
			"",
		},
		{
			map[string]string{"fstype": "xfs"},
			"xfs",
		},
		{
			map[string]string{"FSType": "xfs"},
			"xfs",
		},
		{
			map[string]string{"fstype": "EXT4"},
			"ext4",
		},
	}

	for _, test := range tests {
		result := getFStype(test.options)
		if result != test.expected {
			t.Errorf("input: %q, getFStype result: %s, expected: %s", test.options, result, test.expected)
		}
	}
}

func TestGetMaxDataDiskCount(t *testing.T) {
	tests := []struct {
		instanceType string
		expectResult int64
	}{
		{
			instanceType: "standard_d2_v2",
			expectResult: 8,
		},
		{
			instanceType: "Standard_DS14_V2",
			expectResult: 64,
		},
		{
			instanceType: "NOT_EXISTING",
			expectResult: defaultAzureVolumeLimit,
		},
		{
			instanceType: "",
			expectResult: defaultAzureVolumeLimit,
		},
	}

	for _, test := range tests {
		result := getMaxDataDiskCount(test.instanceType)
		assert.Equal(t, test.expectResult, result)
	}
}

func TestEnsureMountPoint(t *testing.T) {
	errorTarget, err := testutil.GetWorkDirPath("error_is_likely_target")
	assert.NoError(t, err)
	alreadyExistTarget, err := testutil.GetWorkDirPath("false_is_likely_exist_target")
	assert.NoError(t, err)
	azuredisk, err := testutil.GetWorkDirPath("azure.go")
	assert.NoError(t, err)

	tests := []struct {
		desc          string
		target        string
		skipOnWindows bool
		expectedMnt   bool
		expectedErr   testutil.TestError
	}{
		{
			desc:          "[Error] Mocked by IsLikelyNotMountPoint",
			target:        errorTarget,
			skipOnWindows: true, // no error reported in windows
			expectedErr: testutil.TestError{
				DefaultError: errors.New("fake IsLikelyNotMountPoint: fake error"),
			},
			expectedMnt: false,
		},
		{
			desc:          "[Error] Not a directory",
			target:        azuredisk,
			skipOnWindows: true, // no error reported in windows
			expectedErr: testutil.TestError{
				DefaultError: &os.PathError{Op: "mkdir", Path: azuredisk, Err: syscall.ENOTDIR},
			},
		},
		{
			desc:        "[Success] Successful run",
			target:      targetTest,
			expectedErr: testutil.TestError{},
			expectedMnt: false,
		},
		{
			desc:          "[Success] Already existing mount",
			target:        alreadyExistTarget,
			skipOnWindows: true, // not consistent result between Linux and Windows
			expectedErr:   testutil.TestError{},
			expectedMnt:   true,
		},
	}

	// Setup
	_ = makeDir(alreadyExistTarget)
	d, _ := NewFakeDriver(t)
	fakeMounter, err := NewFakeMounter()
	assert.NoError(t, err)
	d.setMounter(fakeMounter)

	for _, test := range tests {
		if !(runtime.GOOS == "windows" && test.skipOnWindows) {
			mnt, err := d.ensureMountPoint(test.target)
			if !testutil.AssertError(&test.expectedErr, err) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr.Error())
			}
			if err == nil {
				assert.Equal(t, test.expectedMnt, mnt)
			}
		}
	}

	// Clean up
	err = os.RemoveAll(alreadyExistTarget)
	assert.NoError(t, err)
	err = os.RemoveAll(targetTest)
	assert.NoError(t, err)
}

func TestNodeGetVolumeStats(t *testing.T) {
	nonexistedPath := "/not/a/real/directory"
	fakePath := "/tmp/fake-volume-path"
	tests := []struct {
		desc         string
		req          csi.NodeGetVolumeStatsRequest
		expectedErr  error
		skipOnDarwin bool
	}{
		{
			desc:        "Volume ID missing",
			req:         csi.NodeGetVolumeStatsRequest{VolumePath: targetTest},
			expectedErr: status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume ID was empty"),
		},
		{
			desc:        "VolumePath missing",
			req:         csi.NodeGetVolumeStatsRequest{VolumeId: "vol_1"},
			expectedErr: status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume path was empty"),
		},
		{
			desc:        "Not existed volume path",
			req:         csi.NodeGetVolumeStatsRequest{VolumePath: nonexistedPath, VolumeId: "vol_1"},
			expectedErr: status.Errorf(codes.NotFound, "path /not/a/real/directory does not exist"),
		},
		{
			desc:         "standard success",
			req:          csi.NodeGetVolumeStatsRequest{VolumePath: fakePath, VolumeId: "vol_1"},
			skipOnDarwin: true,
			expectedErr:  nil,
		},
	}

	// Setup
	_ = makeDir(fakePath)
	d, _ := NewFakeDriver(t)

	for _, test := range tests {
		if !(test.skipOnDarwin && runtime.GOOS == "darwin") {
			_, err := d.NodeGetVolumeStats(context.Background(), &test.req)
			if !reflect.DeepEqual(err, test.expectedErr) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
			}
		}
	}

	// Clean up
	err := os.RemoveAll(fakePath)
	assert.NoError(t, err)
}

func TestNodeStageVolume(t *testing.T) {
	d, _ := NewFakeDriver(t)

	stdVolCap := &csi.VolumeCapability_Mount{
		Mount: &csi.VolumeCapability_MountVolume{
			FsType: defaultLinuxFsType,
		},
	}
	mp := make(map[string]string)
	mp["fstype"] = defaultLinuxFsType
	stdVolCapBlock := &csi.VolumeCapability_Block{
		Block: &csi.VolumeCapability_BlockVolume{},
	}

	volumeCap := csi.VolumeCapability_AccessMode{Mode: 2}
	volumeCapWrong := csi.VolumeCapability_AccessMode{Mode: 10}
	publishContext := map[string]string{
		LUN: "/dev/01",
	}

	tests := []struct {
		desc         string
		setup        func()
		req          csi.NodeStageVolumeRequest
		expectedErr  error
		skipOnDarwin bool
		cleanup      func()
	}{
		{
			desc:        "Volume ID missing",
			req:         csi.NodeStageVolumeRequest{},
			expectedErr: status.Error(codes.InvalidArgument, "Volume ID not provided"),
		},
		{
			desc:        "Stage target path missing",
			req:         csi.NodeStageVolumeRequest{VolumeId: "vol_1"},
			expectedErr: status.Error(codes.InvalidArgument, "Staging target not provided"),
		},
		{
			desc:        "Volume capabilities missing",
			req:         csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest},
			expectedErr: status.Error(codes.InvalidArgument, "Volume capability not provided"),
		},
		{
			desc:        "Volume capabilities not supported",
			req:         csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest, VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCapWrong}},
			expectedErr: status.Error(codes.InvalidArgument, "Volume capability not supported"),
		},
		{
			desc: "Volume operation in progress",
			setup: func() {
				d.getVolumeLocks().TryAcquire("vol_1")
			},
			req: csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest, VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
				AccessType: stdVolCapBlock}},
			expectedErr: status.Error(codes.Aborted, fmt.Sprintf(volumeOperationAlreadyExistsFmt, "vol_1")),
			cleanup: func() {
				d.getVolumeLocks().Release("vol_1")
			},
		},
		{
			desc: "Lun not provided",
			req: csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest, VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
				AccessType: stdVolCap}},
			expectedErr: status.Error(codes.InvalidArgument, "lun not provided"),
		},
		{
			desc:         "Invalid Lun",
			skipOnDarwin: true,
			req: csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCap},
				PublishContext: publishContext,
				VolumeContext:  mp,
			},

			expectedErr: status.Error(codes.Internal, "Failed to find disk on lun /dev/01. cannot parse deviceInfo: /dev/01"),
		},
	}

	// Setup
	_ = makeDir(sourceTest)
	_ = makeDir(targetTest)
	fakeMounter, err := NewFakeMounter()
	assert.NoError(t, err)
	d.setMounter(fakeMounter)

	for _, test := range tests {
		if test.setup != nil {
			test.setup()
		}
		if !(test.skipOnDarwin && runtime.GOOS == "darwin") {
			_, err := d.NodeStageVolume(context.Background(), &test.req)
			if test.desc == "Failed volume mount" {
				assert.Error(t, err)
			} else if !reflect.DeepEqual(err, test.expectedErr) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
			}
		}
		if test.cleanup != nil {
			test.cleanup()
		}
	}

	// Clean up
	err = os.RemoveAll(sourceTest)
	assert.NoError(t, err)
	err = os.RemoveAll(targetTest)
	assert.NoError(t, err)
}

func TestNodeUnstageVolume(t *testing.T) {
	d, _ := NewFakeDriver(t)
	errorTarget, err := testutil.GetWorkDirPath("error_is_likely_target")
	assert.NoError(t, err)
	targetFile, err := testutil.GetWorkDirPath("abc.go")
	assert.NoError(t, err)

	tests := []struct {
		setup         func()
		desc          string
		req           csi.NodeUnstageVolumeRequest
		skipOnWindows bool
		skipOnDarwin  bool
		expectedErr   testutil.TestError
		cleanup       func()
	}{
		{
			desc: "Volume ID missing",
			req:  csi.NodeUnstageVolumeRequest{StagingTargetPath: targetTest},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Volume ID not provided"),
			},
		},
		{
			desc: "Staging target missing ",
			req:  csi.NodeUnstageVolumeRequest{VolumeId: "vol_1"},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Staging target not provided"),
			},
		},
		{
			desc:          "[Error] CleanupMountPoint error mocked by IsLikelyNotMountPoint",
			req:           csi.NodeUnstageVolumeRequest{StagingTargetPath: errorTarget, VolumeId: "vol_1"},
			skipOnWindows: true, // no error reported in windows
			skipOnDarwin:  true,
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.Internal, fmt.Sprintf("failed to unmount staging target \"%s\": "+
					"fake IsLikelyNotMountPoint: fake error", errorTarget)),
			},
		},
		{
			desc: "[Error] Volume operation in progress",
			setup: func() {
				d.getVolumeLocks().TryAcquire("vol_1")
			},
			req: csi.NodeUnstageVolumeRequest{StagingTargetPath: targetFile, VolumeId: "vol_1"},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.Aborted, fmt.Sprintf(volumeOperationAlreadyExistsFmt, "vol_1")),
			},
			cleanup: func() {
				d.getVolumeLocks().Release("vol_1")
			},
		},
		{
			desc:        "[Success] Valid request",
			req:         csi.NodeUnstageVolumeRequest{StagingTargetPath: targetFile, VolumeId: "vol_1"},
			expectedErr: testutil.TestError{},
		},
	}

	//Setup
	_ = makeDir(errorTarget)
	fakeMounter, err := NewFakeMounter()
	assert.NoError(t, err)
	d.setMounter(fakeMounter)

	for _, test := range tests {
		if test.setup != nil {
			test.setup()
		}
		if !(runtime.GOOS == "windows" && test.skipOnWindows) &&
			!(runtime.GOOS == "darwin" && test.skipOnDarwin) {
			_, err := d.NodeUnstageVolume(context.Background(), &test.req)
			if !testutil.AssertError(&test.expectedErr, err) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr.Error())
			}
		}
		if test.cleanup != nil {
			test.cleanup()
		}
	}

	// Clean up
	err = os.RemoveAll(errorTarget)
	assert.NoError(t, err)
}

func TestNodePublishVolume(t *testing.T) {
	d, _ := NewFakeDriver(t)

	volumeCap := csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER}
	publishContext := map[string]string{
		LUN: "/dev/01",
	}
	errorMountSource, err := testutil.GetWorkDirPath("error_mount_source")
	assert.NoError(t, err)
	alreadyMountedTarget, err := testutil.GetWorkDirPath("false_is_likely_exist_target")
	assert.NoError(t, err)

	azurediskPath := "azure.go"

	// ".\azure.go will get deleted on Windows"
	if runtime.GOOS == "windows" {
		azurediskPath = "testfiles\\azure.go"
	}
	azuredisk, err := testutil.GetWorkDirPath(azurediskPath)
	assert.NoError(t, err)

	stdVolCap := &csi.VolumeCapability_Mount{
		Mount: &csi.VolumeCapability_MountVolume{},
	}
	stdVolCapBlock := &csi.VolumeCapability_Block{
		Block: &csi.VolumeCapability_BlockVolume{},
	}

	tests := []struct {
		desc          string
		setup         func()
		req           csi.NodePublishVolumeRequest
		skipOnWindows bool
		expectedErr   testutil.TestError
		cleanup       func()
	}{
		{
			desc: "Volume capabilities missing",
			req:  csi.NodePublishVolumeRequest{},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Volume capability missing in request"),
			},
		},
		{
			desc: "Volume ID missing",
			req:  csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap}},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Volume ID missing in request"),
			},
		},
		{
			desc: "Staging target path missing",
			req: csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap},
				VolumeId: "vol_1"},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Staging target not provided"),
			},
		},
		{
			desc: "Target path missing",
			req: csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap},
				VolumeId:          "vol_1",
				StagingTargetPath: sourceTest},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Target path not provided"),
			},
		},
		{
			desc: "[Error] Not a directory",
			req: csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCap},
				VolumeId:          "vol_1",
				TargetPath:        azuredisk,
				StagingTargetPath: sourceTest,
				Readonly:          true},
			skipOnWindows: true, // permission issues
			expectedErr: testutil.TestError{
				DefaultError: status.Errorf(codes.Internal, fmt.Sprintf("Could not mount target \"%s\": "+
					"mkdir %s: not a directory", azuredisk, azuredisk)),
			},
		},
		{
			desc: "[Error] Lun not provided",
			req: csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapBlock},
				VolumeId:          "vol_1",
				TargetPath:        azuredisk,
				StagingTargetPath: sourceTest,
				Readonly:          true},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "lun not provided"),
			},
		},
		{
			desc: "[Error] Lun not valid",
			req: csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapBlock},
				VolumeId:          "vol_1",
				TargetPath:        azuredisk,
				StagingTargetPath: sourceTest,
				PublishContext:    publishContext,
				Readonly:          true},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.Internal, "Failed to find device path with lun /dev/01. cannot parse deviceInfo: /dev/01"),
			},
		},
		{
			desc: "[Error] Mount error mocked by Mount",
			req: csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap},
				VolumeId:          "vol_1",
				TargetPath:        targetTest,
				StagingTargetPath: errorMountSource,
				Readonly:          true},
			skipOnWindows: true, // permission issues
			expectedErr: testutil.TestError{
				DefaultError: status.Errorf(codes.Internal, fmt.Sprintf("Could not mount \"%s\" at \"%s\": "+
					"fake Mount: source error", errorMountSource, targetTest)),
			},
		},
		{
			desc: "[Success] Valid request already mounted",
			req: csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap},
				VolumeId:          "vol_1",
				TargetPath:        alreadyMountedTarget,
				StagingTargetPath: sourceTest,
				Readonly:          true},
			skipOnWindows: true, // permission issues
			expectedErr:   testutil.TestError{},
		},
		{
			desc: "[Success] Valid request",
			req: csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap},
				VolumeId:          "vol_1",
				TargetPath:        targetTest,
				StagingTargetPath: sourceTest,
				Readonly:          true},
			skipOnWindows: true, // permission issues
			expectedErr:   testutil.TestError{},
		},
	}

	// Setup
	_ = makeDir(alreadyMountedTarget)
	assert.NoError(t, err)
	fakeMounter, err := NewFakeMounter()
	assert.NoError(t, err)
	d.setMounter(fakeMounter)

	for _, test := range tests {
		if test.setup != nil {
			test.setup()
		}
		if !(test.skipOnWindows && runtime.GOOS == "windows") {
			var err error
			_, err = d.NodePublishVolume(context.Background(), &test.req)
			if !testutil.AssertError(&test.expectedErr, err) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr.Error())
			}
		}
		if test.cleanup != nil {
			test.cleanup()
		}
	}

	// Clean up
	err = os.RemoveAll(targetTest)
	assert.NoError(t, err)
	err = os.RemoveAll(alreadyMountedTarget)
	assert.NoError(t, err)
}

func TestNodeUnpublishVolume(t *testing.T) {
	d, _ := NewFakeDriver(t)
	errorTarget, err := testutil.GetWorkDirPath("error_is_likely_target")
	assert.NoError(t, err)
	targetFile, err := testutil.GetWorkDirPath("abc.go")
	assert.NoError(t, err)

	tests := []struct {
		setup         func()
		desc          string
		req           csi.NodeUnpublishVolumeRequest
		skipOnWindows bool
		skipOnDarwin  bool
		expectedErr   testutil.TestError
		cleanup       func()
	}{
		{
			desc: "Volume ID missing",
			req:  csi.NodeUnpublishVolumeRequest{TargetPath: targetTest},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Volume ID missing in request"),
			},
		},
		{
			desc: "Target missing",
			req:  csi.NodeUnpublishVolumeRequest{VolumeId: "vol_1"},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Target path missing in request"),
			},
		},
		{
			desc:          "[Error] Unmount error mocked by IsLikelyNotMountPoint",
			req:           csi.NodeUnpublishVolumeRequest{TargetPath: errorTarget, VolumeId: "vol_1"},
			skipOnWindows: true, // no error reported in windows
			skipOnDarwin:  true, // no error reported in darwin
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.Internal, fmt.Sprintf("failed to unmount target \"%s\": fake IsLikelyNotMountPoint: fake error", errorTarget)),
			},
		},
		{
			desc:        "[Success] Valid request",
			req:         csi.NodeUnpublishVolumeRequest{TargetPath: targetFile, VolumeId: "vol_1"},
			expectedErr: testutil.TestError{},
		},
	}

	// Setup
	_ = makeDir(errorTarget)
	fakeMounter, err := NewFakeMounter()
	assert.NoError(t, err)
	d.setMounter(fakeMounter)

	for _, test := range tests {
		if test.setup != nil {
			test.setup()
		}
		if !(test.skipOnWindows && runtime.GOOS == "windows") &&
			!(test.skipOnDarwin && runtime.GOOS == "darwin") {
			_, err := d.NodeUnpublishVolume(context.Background(), &test.req)
			if !testutil.AssertError(&test.expectedErr, err) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr.Error())
			}
		}
		if test.cleanup != nil {
			test.cleanup()
		}
	}

	// Clean up
	err = os.RemoveAll(errorTarget)
	assert.NoError(t, err)
}

func TestNodeExpandVolume(t *testing.T) {
	d, _ := NewFakeDriver(t)
	_ = makeDir(targetTest)
	notFoundErr := "exit status 1"
	volumeCapWrong := csi.VolumeCapability_AccessMode{Mode: 10}

	stdCapacityRange = &csi.CapacityRange{
		RequiredBytes: volumehelper.GiBToBytes(15),
		LimitBytes:    volumehelper.GiBToBytes(10),
	}

	invalidPathErr := testutil.TestError{
		DefaultError: status.Error(codes.NotFound, "failed to determine device path for volumePath [./test]: path \"./test\" does not exist"),
		WindowsError: status.Errorf(codes.NotFound, "Forward to GetVolumeIDFromTargetPath failed, err=error getting the volume for the mount .\test, internal error error getting volume from mount. cmd: (Get-Item -Path .\test).Target, output: , error: <nil>"),
	}
	volumeCapacityErr := testutil.TestError{
		DefaultError: status.Error(codes.InvalidArgument, "VolumeCapability is invalid."),
	}
	devicePathErr := testutil.TestError{
		DefaultError: status.Errorf(codes.NotFound, "could not determine device path(%s), error: %v", targetTest, notFoundErr),
		WindowsError: status.Errorf(codes.NotFound, "Forward to GetVolumeIDFromTargetPath failed, err=error getting the volume for the mount %s, internal error error getting volume from mount. cmd: (Get-Item -Path %s).Target, output: , error: <nil>", targetTest, targetTest),
	}
	blockSizeErr := testutil.TestError{
		DefaultError: status.Error(codes.Internal, "Could not get size of block volume at path test: error when getting size of block volume at path test: output: , err: exit status 1"),
	}

	if runtime.GOOS == "darwin" {
		invalidPathErr.DefaultError = status.Error(codes.NotFound, "failed to determine device path for volumePath [./test]: volume/util/hostutil on this platform is not supported")
		volumeCapacityErr.DefaultError = status.Errorf(codes.NotFound, "failed to determine device path for volumePath [%s]: volume/util/hostutil on this platform is not supported", targetTest)
		devicePathErr.DefaultError = status.Errorf(codes.NotFound, "failed to determine device path for volumePath [%s]: volume/util/hostutil on this platform is not supported", targetTest)
		blockSizeErr.DefaultError = status.Errorf(codes.NotFound, "failed to determine device path for volumePath [%s]: volume/util/hostutil on this platform is not supported", targetTest)
	}
	tests := []struct {
		desc        string
		req         csi.NodeExpandVolumeRequest
		expectedErr testutil.TestError
	}{
		{
			desc: "Volume ID missing",
			req:  csi.NodeExpandVolumeRequest{},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Volume ID not provided"),
			},
		},
		{
			desc: "could not find path",
			req: csi.NodeExpandVolumeRequest{
				CapacityRange: stdCapacityRange,
				VolumePath:    "./test",
				VolumeId:      "test",
			},
			expectedErr: invalidPathErr,
		},
		{
			desc: "volume path not provide",
			req: csi.NodeExpandVolumeRequest{
				CapacityRange:     stdCapacityRange,
				StagingTargetPath: "test",
				VolumeId:          "test",
			},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "volume path must be provided"),
			},
		},
		{
			desc: "Volume capacity invalid",
			req: csi.NodeExpandVolumeRequest{
				CapacityRange:     stdCapacityRange,
				VolumePath:        targetTest,
				VolumeId:          "test",
				StagingTargetPath: "test",
				VolumeCapability:  &csi.VolumeCapability{AccessMode: &volumeCapWrong},
			},
			expectedErr: volumeCapacityErr,
		},
		{
			desc: "Invalid device path",
			req: csi.NodeExpandVolumeRequest{
				CapacityRange:     stdCapacityRange,
				VolumePath:        targetTest,
				VolumeId:          "test",
				StagingTargetPath: "",
			},
			expectedErr: devicePathErr,
		},
		{
			desc: "No block size at path",
			req: csi.NodeExpandVolumeRequest{
				CapacityRange:     stdCapacityRange,
				VolumePath:        targetTest,
				VolumeId:          "test",
				StagingTargetPath: "test",
			},
			expectedErr: devicePathErr,
		},
	}
	for _, test := range tests {
		_, err := d.NodeExpandVolume(context.Background(), &test.req)
		if !testutil.AssertError(&test.expectedErr, err) {
			t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr.Error())
		}
	}
	err := os.RemoveAll(targetTest)
	assert.NoError(t, err)
}

func TestGetBlockSizeBytes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skipping test on Windows")
	}
	d, _ := NewFakeDriver(t)
	testTarget, err := testutil.GetWorkDirPath("test")
	assert.NoError(t, err)

	notFoundErr := "exit status 1"
	// exception in darwin
	if runtime.GOOS == "darwin" {
		notFoundErr = "executable file not found in $PATH"
	} else if runtime.GOOS == "windows" {
		notFoundErr = "executable file not found in %PATH%"
	}

	tests := []struct {
		desc        string
		req         string
		expectedErr testutil.TestError
	}{
		{
			desc: "no exist path",
			req:  "testpath",
			expectedErr: testutil.TestError{
				DefaultError: fmt.Errorf("error when getting size of block volume at path testpath: output: , err: %s", notFoundErr),
				WindowsError: fmt.Errorf("error when getting size of block volume at path testpath: output: , err: %s", notFoundErr),
			},
		},
		{
			desc: "invalid path",
			req:  testTarget,
			expectedErr: testutil.TestError{
				DefaultError: fmt.Errorf("error when getting size of block volume at path %s: "+
					"output: , err: %s", testTarget, notFoundErr),
				WindowsError: fmt.Errorf("error when getting size of block volume at path %s: "+
					"output: , err: %s", testTarget, notFoundErr),
			},
		},
	}
	for _, test := range tests {
		_, err := getBlockSizeBytes(test.req, d.getMounter())
		if !testutil.AssertError(&test.expectedErr, err) {
			t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr.Error())
		}
	}
	//Setup
	_ = makeDir(testTarget)

	err = os.RemoveAll(testTarget)
	assert.NoError(t, err)
}

func TestEnsureBlockTargetFile(t *testing.T) {
	// sip this test because `util/mount` not supported
	// on darwin
	if runtime.GOOS == "darwin" {
		t.Skip("Skipping tests on darwin")
	}
	testTarget, err := testutil.GetWorkDirPath("test")
	assert.NoError(t, err)
	testPath, err := testutil.GetWorkDirPath(fmt.Sprintf("test%ctest", os.PathSeparator))
	assert.NoError(t, err)
	d, err := NewFakeDriver(t)
	assert.NoError(t, err)

	tests := []struct {
		desc        string
		req         string
		expectedErr testutil.TestError
	}{
		{
			desc:        "valid test",
			req:         testTarget,
			expectedErr: testutil.TestError{},
		},
		{
			desc: "test if file exists",
			req:  testPath,
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.Internal, fmt.Sprintf("Could not mount target \"%s\": mkdir %s: not a directory", testTarget, testTarget)),
				WindowsError: status.Error(codes.Internal, fmt.Sprintf("Could not remove mount target %#v: remove %s: The system cannot find the path specified.", testPath, testPath)),
			},
		},
	}
	for _, test := range tests {
		err := d.ensureBlockTargetFile(test.req)
		if !testutil.AssertError(&test.expectedErr, err) {
			t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr.Error())
		}
	}
	err = os.RemoveAll(testTarget)
	assert.NoError(t, err)
}

func makeDir(pathname string) error {
	err := os.MkdirAll(pathname, os.FileMode(0755))
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}

func TestMakeDir(t *testing.T) {
	//Successfully create directory
	err := makeDir(targetTest)
	assert.NoError(t, err)

	//Failed case
	err = makeDir("./azure.go")
	var e *os.PathError
	if !errors.As(err, &e) {
		t.Errorf("Unexpected Error: %v", err)
	}

	// Remove the directory created
	err = os.RemoveAll(targetTest)
	assert.NoError(t, err)
}

func TestGetDevicePathWithLUN(t *testing.T) {
	d, _ := NewFakeDriver(t)
	tests := []struct {
		desc        string
		req         string
		expectedErr error
	}{
		{
			desc:        "valid test",
			req:         "unit-test",
			expectedErr: fmt.Errorf("cannot parse deviceInfo: unit-test"),
		},
	}
	for _, test := range tests {
		_, err := d.getDevicePathWithLUN(test.req)
		if !reflect.DeepEqual(err, test.expectedErr) {
			t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
		}
	}
}

func TestGetDevicePathWithMountPath(t *testing.T) {
	d, _ := NewFakeDriver(t)
	err := "exit status 1"

	if runtime.GOOS == "darwin" {
		err = "executable file not found in $PATH"
	}

	tests := []struct {
		desc          string
		req           string
		skipOnDarwin  bool
		skipOnWindows bool
		expectedErr   error
	}{
		{
			desc:        "Invalid device path",
			req:         "unit-test",
			expectedErr: fmt.Errorf("could not determine device path(unit-test), error: %v", err),
			// Skip negative tests on Windows because error messages from csi-proxy are not easily predictable.
			skipOnWindows: true,
		},
		{
			desc:          "[Success] Valid device path",
			req:           "/sys",
			skipOnDarwin:  true,
			skipOnWindows: true,
			expectedErr:   nil,
		},
	}

	for _, test := range tests {
		if !(test.skipOnDarwin && runtime.GOOS == "darwin") && !(test.skipOnWindows && runtime.GOOS == "windows") {
			_, err := getDevicePathWithMountPath(test.req, d.getMounter())
			if !reflect.DeepEqual(err, test.expectedErr) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
			}
		}
	}
}
