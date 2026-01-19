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
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	consts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/diskclient/mock_diskclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/snapshotclient/mock_snapshotclient"
	mockvmclient "sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachineclient/mock_virtualmachineclient"
	azure "sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

func TestNewDriver(t *testing.T) {
	d := NewDriver(&DriverOptions{
		NodeID:                 os.Getenv("nodeid"),
		DriverName:             consts.DefaultDriverName,
		VolumeAttachLimit:      16,
		EnablePerfOptimization: false,
		Kubeconfig:             "",
		AllowEmptyCloudConfig:  true,
	})
	assert.NotNil(t, d)
}

func TestCheckDiskCapacity(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
	size := int32(10)
	diskName := "unit-test"
	resourceGroup := "unit-test"
	disk := &armcompute.Disk{
		Properties: &armcompute.DiskProperties{
			DiskSizeGB: &size,
		},
	}
	diskClient := mock_diskclient.NewMockInterface(cntl)
	d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub("").Return(diskClient, nil).AnyTimes()
	diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
	flag, err := d.checkDiskCapacity(context.TODO(), "", resourceGroup, diskName, 10)
	assert.Equal(t, flag, true)
	assert.Nil(t, err)

	flag, err = d.checkDiskCapacity(context.TODO(), "", resourceGroup, diskName, 11)
	assert.Equal(t, flag, false)
	expectedErr := status.Errorf(6, "the request volume already exists, but its capacity(10) is different from (11)")
	assert.Equal(t, err, expectedErr)
}

func TestCheckDiskCapacityWithThrottling(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
	size := int32(10)
	diskName := "unit-test"
	resourceGroup := "unit-test"
	disk := &armcompute.Disk{
		Properties: &armcompute.DiskProperties{
			DiskSizeGB: &size,
		},
	}
	diskClient := mock_diskclient.NewMockInterface(cntl)
	d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub("").Return(diskClient, nil).AnyTimes()
	diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()

	d.setThrottlingCache(consts.GetDiskThrottlingKey, "")
	flag, _ := d.checkDiskCapacity(context.TODO(), "", resourceGroup, diskName, 11)
	assert.Equal(t, flag, true)
}

func TestRun(t *testing.T) {
	fakeCredFile := "fake-cred-file.json"
	fakeCredContent := `{
    "tenantId": "1234",
    "subscriptionId": "12345",
    "aadClientId": "123456",
    "aadClientSecret": "1234567",
    "resourceGroup": "rg1",
    "location": "loc"
}`

	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Successful run",
			testFunc: func(t *testing.T) {
				if err := os.WriteFile(fakeCredFile, []byte(fakeCredContent), 0666); err != nil {
					t.Error(err)
				}

				defer func() {
					if err := os.Remove(fakeCredFile); err != nil {
						t.Error(err)
					}
				}()

				t.Setenv(consts.DefaultAzureCredentialFileEnv, fakeCredFile)

				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				ctx, cancelFn := context.WithCancel(context.Background())
				var routines errgroup.Group
				routines.Go(func() error { return d.Run(ctx) })
				time.Sleep(time.Millisecond * 500)
				cancelFn()
				time.Sleep(time.Millisecond * 500)
				err := routines.Wait()
				assert.Nil(t, err)
			},
		},
		{
			name: "Successful run without cloud config",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				ctx, cancelFn := context.WithCancel(context.Background())
				var routines errgroup.Group
				routines.Go(func() error { return d.Run(ctx) })
				time.Sleep(time.Millisecond * 500)
				cancelFn()
				time.Sleep(time.Millisecond * 500)
				err := routines.Wait()
				assert.Nil(t, err)
			},
		},
		{
			name: "Successful run with node ID missing",
			testFunc: func(t *testing.T) {
				if err := os.WriteFile(fakeCredFile, []byte(fakeCredContent), 0666); err != nil {
					t.Error(err)
				}

				defer func() {
					if err := os.Remove(fakeCredFile); err != nil {
						t.Error(err)
					}
				}()

				t.Setenv(consts.DefaultAzureCredentialFileEnv, fakeCredFile)

				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				d.setCloud(&azure.Cloud{})
				d.setNodeID("")
				ctx, cancelFn := context.WithCancel(context.Background())
				var routines errgroup.Group
				routines.Go(func() error { return d.Run(ctx) })
				time.Sleep(time.Millisecond * 500)
				cancelFn()
				time.Sleep(time.Millisecond * 500)
				err := routines.Wait()
				assert.Nil(t, err)
			},
		},
		{
			name: "Successful run with vmss VMType",
			testFunc: func(t *testing.T) {
				if err := os.WriteFile(fakeCredFile, []byte(fakeCredContent), 0666); err != nil {
					t.Error(err)
				}

				defer func() {
					if err := os.Remove(fakeCredFile); err != nil {
						t.Error(err)
					}
				}()

				t.Setenv(consts.DefaultAzureCredentialFileEnv, fakeCredFile)

				d := NewDriver(&DriverOptions{
					NodeID:                 "",
					DriverName:             consts.DefaultDriverName,
					EnableListVolumes:      true,
					EnableListSnapshots:    true,
					EnablePerfOptimization: true,
					VMSSCacheTTLInSeconds:  10,
					VMType:                 "vmss",
					Endpoint:               "tcp://127.0.0.1:0",
				})
				ctx, cancelFn := context.WithCancel(context.Background())
				var routines errgroup.Group
				routines.Go(func() error { return d.Run(ctx) })
				time.Sleep(time.Millisecond * 500)
				cancelFn()
				time.Sleep(time.Millisecond * 500)
				err := routines.Wait()
				assert.Nil(t, err)
			},
		},
		{
			name: "Successful run with federated workload identity azure client",
			testFunc: func(t *testing.T) {
				if err := os.WriteFile(fakeCredFile, []byte(fakeCredContent), 0666); err != nil {
					t.Error(err)
				}

				defer func() {
					if err := os.Remove(fakeCredFile); err != nil {
						t.Error(err)
					}
				}()

				t.Setenv(consts.DefaultAzureCredentialFileEnv, fakeCredFile)
				t.Setenv("AZURE_TENANT_ID", "1234")
				t.Setenv("AZURE_CLIENT_ID", "123456")
				t.Setenv("AZURE_FEDERATED_TOKEN_FILE", "fake-token-file")

				d := NewDriver(&DriverOptions{
					NodeID:                 "",
					DriverName:             consts.DefaultDriverName,
					EnableListVolumes:      true,
					EnableListSnapshots:    true,
					EnablePerfOptimization: true,
					VMSSCacheTTLInSeconds:  10,
					VMType:                 "vmss",
					Endpoint:               "tcp://127.0.0.1:0",
				})

				ctx, cancel := context.WithCancel(context.Background())
				ch := make(chan error)
				go func() {
					err := d.Run(ctx)
					ch <- err
				}()
				cancel()
				assert.Nil(t, <-ch)
				assert.Equal(t, d.cloud.UseFederatedWorkloadIdentityExtension, true)
				assert.Equal(t, d.cloud.AADFederatedTokenFile, "fake-token-file")
				assert.Equal(t, d.cloud.AADClientID, "123456")
				assert.Equal(t, d.cloud.TenantID, "1234")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestDriver_checkDiskExists(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
	_, err := d.checkDiskExists(context.TODO(), "testurl/subscriptions/12/providers/Microsoft.Compute/disks/name")
	assert.NotEqual(t, err, nil)
}

func TestDriver_CheckDiskExists_Success(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
	d.setThrottlingCache(consts.GetDiskThrottlingKey, "")
	_, err := d.checkDiskExists(context.TODO(), "testurl/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/disks/name")
	assert.Equal(t, err, nil)
}

func TestGetNodeInfoFromLabels(t *testing.T) {
	tests := []struct {
		nodeName      string
		kubeClient    clientset.Interface
		expectedError error
	}{
		{
			nodeName:      "",
			kubeClient:    nil,
			expectedError: fmt.Errorf("kubeClient is nil"),
		},
	}

	for _, test := range tests {
		_, _, err := GetNodeInfoFromLabels(context.TODO(), test.nodeName, test.kubeClient)
		if !reflect.DeepEqual(err, test.expectedError) {
			t.Errorf("Unexpected result: %v, expected result: %v", err, test.expectedError)
		}
	}
}

func TestGetDefaultDiskIOPSReadWrite(t *testing.T) {
	tests := []struct {
		requestGiB int
		expected   int
	}{
		{
			requestGiB: 1,
			expected:   500,
		},
		{
			requestGiB: 512,
			expected:   512,
		},
		{
			requestGiB: 51200000,
			expected:   160000,
		},
	}

	for _, test := range tests {
		result := getDefaultDiskIOPSReadWrite(test.requestGiB)
		if result != test.expected {
			t.Errorf("Unexpected result: %v, expected result: %v, input: %d", result, test.expected, test.requestGiB)
		}
	}
}

func TestGetDefaultDiskMBPSReadWrite(t *testing.T) {
	tests := []struct {
		requestGiB int
		expected   int
	}{
		{
			requestGiB: 1,
			expected:   100,
		},
		{
			requestGiB: 512,
			expected:   100,
		},
		{
			requestGiB: 51200,
			expected:   200,
		},
		{
			requestGiB: 51200000,
			expected:   625,
		},
		{
			requestGiB: 512000000,
			expected:   625,
		},
		{
			requestGiB: 65535,
			expected:   256,
		},
	}

	for _, test := range tests {
		result := getDefaultDiskMBPSReadWrite(test.requestGiB)
		if result != test.expected {
			t.Errorf("Unexpected result: %v, expected result: %v, input: %d", result, test.expected, test.requestGiB)
		}
	}
}

func TestWaitForSnapshot(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "snapshotID not valid",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				subID := "subs"
				resourceGroup := "rg"
				intervel := 1 * time.Millisecond
				timeout := 10 * time.Millisecond
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{},
					ID:         &snapshotID}
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(subID).Return(mockSnapshotClient, nil).AnyTimes()

				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, fmt.Errorf("invalid snapshotID")).AnyTimes()
				err := d.waitForSnapshotReady(context.Background(), subID, resourceGroup, snapshotID, intervel, timeout)

				wantErr := true
				subErrMsg := "invalid snapshotID"
				if (err != nil) != wantErr {
					t.Errorf("waitForSnapshotReady() error = %v, wantErr %v", err, wantErr)
				}
				if err != nil && !strings.Contains(err.Error(), subErrMsg) {
					t.Errorf("waitForSnapshotReady() error = %v, wantErr %v", err, subErrMsg)
				}
			},
		},
		//nolint:dupl
		{
			name: "timeout for waiting snapshot copy cross region",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				subID := "subs"
				resourceGroup := "rg"
				intervel := 1 * time.Millisecond
				timeout := 10 * time.Millisecond
				volumeID := "test"
				DiskSize := int32(10)
				snapshotID := "test"
				location := "loc"
				provisioningState := "succeeded"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
						CreationData:      &armcompute.CreationData{SourceResourceID: &volumeID},
						CompletionPercent: ptr.To(float32(0.0)),
					},
					Location: &location,
					ID:       &snapshotID}
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(subID).Return(mockSnapshotClient, nil).AnyTimes()

				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).AnyTimes()
				err := d.waitForSnapshotReady(context.Background(), subID, resourceGroup, snapshotID, intervel, timeout)

				wantErr := true
				subErrMsg := "timeout"
				if (err != nil) != wantErr {
					t.Errorf("waitForSnapshotReady() error = %v, wantErr %v", err, wantErr)
				}
				if err != nil && !strings.Contains(err.Error(), subErrMsg) {
					t.Errorf("waitForSnapshotReady() error = %v, wantErr %v", err, subErrMsg)
				}
			},
		},
		//nolint:dupl
		{
			name: "succeed for waiting snapshot copy cross region",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				subID := "subs"
				resourceGroup := "rg"
				intervel := 1 * time.Millisecond
				timeout := 10 * time.Millisecond
				volumeID := "test"
				DiskSize := int32(10)
				snapshotID := "test"
				location := "loc"
				provisioningState := "succeeded"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
						CreationData:      &armcompute.CreationData{SourceResourceID: &volumeID},
						CompletionPercent: ptr.To(float32(100.0)),
					},
					Location: &location,
					ID:       &snapshotID}
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(subID).Return(mockSnapshotClient, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).AnyTimes()
				err := d.waitForSnapshotReady(context.Background(), subID, resourceGroup, snapshotID, intervel, timeout)

				wantErr := false
				subErrMsg := ""
				if (err != nil) != wantErr {
					t.Errorf("waitForSnapshotReady() error = %v, wantErr %v", err, wantErr)
				}
				if err != nil && !strings.Contains(err.Error(), subErrMsg) {
					t.Errorf("waitForSnapshotReady() error = %v, wantErr %v", err, subErrMsg)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestGetVMSSInstanceName(t *testing.T) {
	tests := []struct {
		computeName   string
		expected      string
		expectedError error
	}{
		{
			computeName:   "aks-agentpool-20657377-vmss_2",
			expected:      "aks-agentpool-20657377-vmss000002",
			expectedError: nil,
		},
		{
			computeName:   "aks-agentpool-20657377-vmss_37",
			expected:      "aks-agentpool-20657377-vmss000011",
			expectedError: nil,
		},
		{
			computeName:   "akswin_1",
			expected:      "akswin000001",
			expectedError: nil,
		},
		{
			computeName:   "akswin_a",
			expected:      "",
			expectedError: fmt.Errorf("parsing vmss compute name(%s) failed with strconv.Atoi: parsing \"a\": invalid syntax", "akswin_a"),
		},
		{
			computeName:   "aks-agentpool-20657377-vmss37",
			expected:      "",
			expectedError: fmt.Errorf("invalid vmss compute name: %s", "aks-agentpool-20657377-vmss37"),
		},
	}
	for _, test := range tests {
		result, err := getVMSSInstanceName(test.computeName)
		if result != test.expected {
			t.Errorf("Unexpected result: %s, expected result: %s, input: %s", result, test.expected, test.computeName)
		}
		if !reflect.DeepEqual(err, test.expectedError) {
			t.Errorf("Unexpected error: %v, expected error: %v, input: %s", err, test.expectedError, test.computeName)
		}
	}
}

func TestGetUsedLunsFromVolumeAttachments(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
	tests := []struct {
		name                string
		nodeName            string
		expectedUsedLunList []int
		expectedErr         error
	}{
		{
			name:                "nil kubeClient",
			nodeName:            "test-node",
			expectedUsedLunList: nil,
			expectedErr:         fmt.Errorf("kubeClient or kubeClient.StorageV1() or kubeClient.StorageV1().VolumeAttachments() is nil"),
		},
	}
	for _, test := range tests {
		result, err := d.getUsedLunsFromVolumeAttachments(context.Background(), test.nodeName)
		if !reflect.DeepEqual(err, test.expectedErr) {
			t.Errorf("test(%s): err(%v) != expected err(%v)", test.name, err, test.expectedErr)
		}
		if !reflect.DeepEqual(result, test.expectedUsedLunList) {
			t.Errorf("test(%s): result(%v) != expected result(%v)", test.name, result, test.expectedUsedLunList)
		}
	}
}

func TestGetUsedLunsFromNode(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
	vm := armcompute.VirtualMachine{}
	dataDisks := make([]*armcompute.DataDisk, 2)
	dataDisks[0] = &armcompute.DataDisk{Lun: ptr.To(int32(0)), Name: &testVolumeName}
	dataDisks[1] = &armcompute.DataDisk{Lun: ptr.To(int32(2)), Name: &testVolumeName}
	vm.Properties = &armcompute.VirtualMachineProperties{
		StorageProfile: &armcompute.StorageProfile{
			DataDisks: dataDisks,
		},
	}
	mockVMClient := d.getCloud().ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
	mockVMClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&vm, nil).AnyTimes()

	tests := []struct {
		name                string
		nodeName            string
		expectedUsedLunList []int
		expectedErr         error
	}{
		{
			name:                "lun 0 and 2 are used",
			nodeName:            "test-node",
			expectedUsedLunList: []int{0, 2},
			expectedErr:         nil,
		},
	}
	for _, test := range tests {
		result, err := d.getUsedLunsFromNode(context.Background(), types.NodeName(test.nodeName))
		if !reflect.DeepEqual(err, test.expectedErr) {
			t.Errorf("test(%s): err(%v) != expected err(%v)", test.name, err, test.expectedErr)
		}
		if !reflect.DeepEqual(result, test.expectedUsedLunList) {
			t.Errorf("test(%s): result(%v) != expected result(%v)", test.name, result, test.expectedUsedLunList)
		}
	}
}
