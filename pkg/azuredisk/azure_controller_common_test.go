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
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	armcompute "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	autorestmocks "github.com/Azure/go-autorest/autorest/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	mockvmclient "sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachineclient/mock_virtualmachineclient"
	azcache "sigs.k8s.io/cloud-provider-azure/pkg/cache"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

func TestCommonAttachDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	initVM := func(testCloud *provider.Cloud, expectedVMs []armcompute.VirtualMachine) {
		mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
		for _, vm := range expectedVMs {
			mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, *vm.Name, gomock.Any()).Return(&vm, nil).AnyTimes()
		}
		if len(expectedVMs) == 0 {
			mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, gomock.Any(), gomock.Any()).Return(&armcompute.VirtualMachine{}, errors.New("instance not found")).AnyTimes()
		}
	}

	defaultSetup := func(testCloud *provider.Cloud, expectedVMs []armcompute.VirtualMachine, _ int, _ error) {
		initVM(testCloud, expectedVMs)
		mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
		mockVMClient.EXPECT().CreateOrUpdate(gomock.Any(), testCloud.ResourceGroup, gomock.Any(), gomock.Any()).Return(nil, nil).MaxTimes(1)
	}

	maxShare := int32(1)
	goodInstanceID := fmt.Sprintf("/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/%s", "vm1")
	diskEncryptionSetID := fmt.Sprintf("/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/diskEncryptionSets/%s", "diskEncryptionSet-name")
	testTags := make(map[string]*string)
	testTags[WriteAcceleratorEnabled] = ptr.To("true")
	testCases := []struct {
		desc                 string
		diskName             string
		existedDisk          *armcompute.Disk
		nodeName             types.NodeName
		vmList               map[string]string
		isDataDisksFull      bool
		isBadDiskURI         bool
		isDiskUsed           bool
		setup                func(testCloud *provider.Cloud, expectedVMs []armcompute.VirtualMachine, statusCode int, result error)
		expectErr            bool
		isContextDeadlineErr bool
		statusCode           int
		waitResult           error
		expectedLun          int32
		contextDuration      time.Duration
	}{
		{
			desc:        "correct LUN and no error shall be returned if disk is nil",
			vmList:      map[string]string{"vm1": "PowerState/Running"},
			nodeName:    "vm1",
			diskName:    "disk-name",
			existedDisk: nil,
			expectedLun: 3,
			expectErr:   false,
			statusCode:  200,
		},
		{
			desc:        "LUN -1 and error shall be returned if there's no such instance corresponding to given nodeName",
			nodeName:    "vm1",
			diskName:    "disk-name",
			existedDisk: &armcompute.Disk{Name: ptr.To("disk-name")},
			expectedLun: -1,
			expectErr:   true,
		},
		{
			desc:            "LUN -1 and error shall be returned if there's no available LUN for instance",
			vmList:          map[string]string{"vm1": "PowerState/Running"},
			nodeName:        "vm1",
			isDataDisksFull: true,
			diskName:        "disk-name",
			existedDisk:     &armcompute.Disk{Name: ptr.To("disk-name")},
			expectedLun:     -1,
			expectErr:       true,
		},
		{
			desc:     "correct LUN and no error shall be returned if everything is good",
			vmList:   map[string]string{"vm1": "PowerState/Running"},
			nodeName: "vm1",
			diskName: "disk-name",
			existedDisk: &armcompute.Disk{Name: ptr.To("disk-name"),
				Properties: &armcompute.DiskProperties{
					Encryption: &armcompute.Encryption{DiskEncryptionSetID: &diskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)},
					DiskSizeGB: ptr.To(int32(4096)),
					DiskState:  to.Ptr(armcompute.DiskStateUnattached),
				},
				Tags: testTags},
			expectedLun: 3,
			expectErr:   false,
			statusCode:  200,
		},
		{
			desc:     "an error shall be returned if disk state is not Unattached",
			vmList:   map[string]string{"vm1": "PowerState/Running"},
			nodeName: "vm1",
			diskName: "disk-name",
			existedDisk: &armcompute.Disk{Name: ptr.To("disk-name"),
				Properties: &armcompute.DiskProperties{
					Encryption: &armcompute.Encryption{DiskEncryptionSetID: &diskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)},
					DiskSizeGB: ptr.To(int32(4096)),
					DiskState:  to.Ptr(armcompute.DiskStateAttached),
				},
				Tags: testTags},
			expectedLun: -1,
			expectErr:   true,
		},
		{
			desc:        "an error shall be returned if attach an already attached disk with good ManagedBy instance id",
			vmList:      map[string]string{"vm1": "PowerState/Running"},
			nodeName:    "vm1",
			diskName:    "disk-name",
			existedDisk: &armcompute.Disk{Name: ptr.To("disk-name"), ManagedBy: ptr.To(goodInstanceID), Properties: &armcompute.DiskProperties{MaxShares: &maxShare}},
			expectedLun: -1,
			expectErr:   true,
		},
	}

	for i, test := range testCases {
		tt := test
		t.Run(tt.desc, func(t *testing.T) {
			testCloud := provider.GetTestCloud(ctrl)
			diskURI := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s",
				testCloud.SubscriptionID, testCloud.ResourceGroup, test.diskName)
			if tt.isBadDiskURI {
				diskURI = fmt.Sprintf("/baduri/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s",
					testCloud.SubscriptionID, testCloud.ResourceGroup, tt.diskName)
			}
			expectedVMs := setTestVirtualMachines(testCloud, tt.vmList, tt.isDataDisksFull)
			if tt.isDiskUsed {
				vm0 := setTestVirtualMachines(testCloud, map[string]string{"vm0": "PowerState/Running"}, tt.isDataDisksFull)[0]
				expectedVMs = append(expectedVMs, vm0)
			}
			if tt.setup == nil {
				defaultSetup(testCloud, expectedVMs, tt.statusCode, tt.waitResult)
			} else {
				tt.setup(testCloud, expectedVMs, tt.statusCode, tt.waitResult)
			}

			if tt.contextDuration > 0 {
				oldCtx := ctx
				ctx, cancel = context.WithTimeout(oldCtx, tt.contextDuration)
				defer cancel()
				defer func() {
					ctx = oldCtx
				}()
			}
			testdiskController := &controllerCommon{
				cloud:               testCloud,
				lockMap:             newLockMap(),
				DisableDiskLunCheck: true,
				WaitForDetach:       true,
			}
			getter := func(_ context.Context, _ string) (interface{}, error) { return nil, nil }
			testdiskController.hitMaxDataDiskCountCache, _ = azcache.NewTimedCache(5*time.Minute, getter, false)
			lun, err := testdiskController.AttachDisk(ctx, test.diskName, diskURI, tt.nodeName, armcompute.CachingTypesReadOnly, tt.existedDisk, nil)

			assert.Equal(t, tt.expectedLun, lun, "TestCase[%d]: %s", i, tt.desc)
			assert.Equal(t, tt.expectErr, err != nil, "TestCase[%d]: %s, return error: %v", i, tt.desc, err)

			assert.Equal(t, tt.isContextDeadlineErr, errors.Is(err, context.DeadlineExceeded))
		})
	}
}

func TestForceDetach(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := []struct {
		desc                   string
		vmList                 map[string]string
		nodeName               types.NodeName
		diskName               string
		forceDetachBackoff     bool
		detachOperationTimeout int
		contextTimeout         time.Duration
		firstDetachError       error
		forceDetachError       error
		expectedErr            bool
		expectForceDetach      bool
	}{
		{
			desc:                   "force detach should be called when regular detach times out",
			vmList:                 map[string]string{"vm1": "PowerState/Running"},
			nodeName:               "vm1",
			diskName:               "disk1",
			forceDetachBackoff:     true,
			detachOperationTimeout: 1,               // 1s timeout
			contextTimeout:         2 * time.Second, // not more than double the detach timeout
			firstDetachError:       context.DeadlineExceeded,
			forceDetachError:       nil,
			expectedErr:            false,
			expectForceDetach:      true,
		},
		{
			desc:                   "detach operation timeout of more than half the context timeout should be respected",
			vmList:                 map[string]string{"vm1": "PowerState/Running"},
			nodeName:               "vm1",
			diskName:               "disk1",
			forceDetachBackoff:     true,
			detachOperationTimeout: 2,               // 2s timeout
			contextTimeout:         3 * time.Second, // not more than double the detach timeout
			firstDetachError:       context.DeadlineExceeded,
			forceDetachError:       nil,
			expectedErr:            false,
			expectForceDetach:      true,
		},
		{
			desc:                   "force detach should be called with half context timeout when min detach timeout is less than half context timeout",
			vmList:                 map[string]string{"vm1": "PowerState/Running"},
			nodeName:               "vm1",
			diskName:               "disk1",
			forceDetachBackoff:     true,
			detachOperationTimeout: 1,               // 1s timeout
			contextTimeout:         3 * time.Second, // more than double the detach timeout
			firstDetachError:       context.DeadlineExceeded,
			forceDetachError:       nil,
			expectedErr:            false,
			expectForceDetach:      true,
		},
		{
			desc:                   "force detach should be called when regular detach fails",
			vmList:                 map[string]string{"vm1": "PowerState/Running"},
			nodeName:               "vm1",
			diskName:               "disk1",
			forceDetachBackoff:     true,
			detachOperationTimeout: 1,
			contextTimeout:         2 * time.Second,
			firstDetachError:       errors.New("detach failed"),
			forceDetachError:       nil,
			expectedErr:            false,
			expectForceDetach:      true,
		},
		{
			desc:                   "should return error when force detach also fails",
			vmList:                 map[string]string{"vm1": "PowerState/Running"},
			nodeName:               "vm1",
			diskName:               "disk1",
			forceDetachBackoff:     true,
			detachOperationTimeout: 1,
			contextTimeout:         2 * time.Second,
			firstDetachError:       context.DeadlineExceeded,
			forceDetachError:       errors.New("force detach failed"),
			expectedErr:            true,
			expectForceDetach:      true,
		},
		{
			desc:                   "force detach should not be called when forceDetachBackoff is false",
			vmList:                 map[string]string{"vm1": "PowerState/Running"},
			nodeName:               "vm1",
			diskName:               "disk1",
			forceDetachBackoff:     false,
			detachOperationTimeout: 1,
			contextTimeout:         2 * time.Second,
			firstDetachError:       errors.New("detach failed"),
			forceDetachError:       nil,
			expectedErr:            true,
			expectForceDetach:      false,
		},
		{
			desc:                   "successful regular detach should not trigger force detach",
			vmList:                 map[string]string{"vm1": "PowerState/Running"},
			nodeName:               "vm1",
			diskName:               "disk1",
			forceDetachBackoff:     true,
			detachOperationTimeout: 1,
			contextTimeout:         2 * time.Second,
			firstDetachError:       nil,
			forceDetachError:       nil,
			expectedErr:            false,
			expectForceDetach:      false,
		},
	}

	for i, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), test.contextTimeout)
			defer cancel()
			testCloud := provider.GetTestCloud(ctrl)
			common := &controllerCommon{
				cloud:                              testCloud,
				lockMap:                            newLockMap(),
				ForceDetachBackoff:                 test.forceDetachBackoff,
				DetachOperationMinTimeoutInSeconds: test.detachOperationTimeout,
				DisableDiskLunCheck:                true, // Disable lun check to simplify test
			}

			diskURI := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s",
				testCloud.SubscriptionID, testCloud.ResourceGroup, test.diskName)

			expectedVMs := setTestVirtualMachines(testCloud, test.vmList, false)
			mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)

			for _, vm := range expectedVMs {
				mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, *vm.Name, gomock.Any()).Return(&vm, nil).AnyTimes()
			}

			// Set up expectations for CreateOrUpdate calls
			callCount := 0
			if test.expectForceDetach {
				// Expect two calls: regular detach and force detach
				mockVMClient.EXPECT().CreateOrUpdate(gomock.Any(), testCloud.ResourceGroup, gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, _ string, _ string, vm armcompute.VirtualMachine) (*armcompute.VirtualMachine, error) {
						callCount++
						if callCount == 1 {
							// First call is regular detach
							// Verify that context timeout is at least the min detach timeout
							contextDeadline, ok := ctx.Deadline()
							assert.True(t, ok, "Context should have a deadline")
							assert.True(t, contextDeadline.After(time.Now()), "Context deadline should be in the future")
							assert.True(t, time.Until(contextDeadline) >= time.Duration(test.detachOperationTimeout)*time.Millisecond-100*time.Millisecond, "Context deadline should exceed min detach timeout.")
							assert.True(t, time.Until(contextDeadline) >= test.contextTimeout/2-100*time.Millisecond, "Context deadline should be at least half of context timeout.")
							// Simulate timeout by sleeping longer than context deadline
							if test.firstDetachError == context.DeadlineExceeded {
								time.Sleep(time.Until(contextDeadline.Add(50 * time.Millisecond)))
							}
							return nil, test.firstDetachError
						} else if callCount == 2 {
							// Second call is force detach
							// Verify force detach parameter is set
							if vm.Properties != nil && vm.Properties.StorageProfile != nil {
								for _, disk := range vm.Properties.StorageProfile.DataDisks {
									if disk.Name != nil && *disk.Name == test.diskName {
										assert.NotNil(t, disk.DetachOption, "DetachOption should be set for force detach")
										if disk.DetachOption != nil {
											assert.Equal(t, armcompute.DiskDetachOptionTypesForceDetach, *disk.DetachOption, "DetachOption should be ForceDetach")
										}
									}
								}
							}
							return nil, test.forceDetachError
						}
						return nil, errors.New("unexpected call")
					}).Times(2)
			} else {
				// Expect only one call for regular detach
				mockVMClient.EXPECT().CreateOrUpdate(gomock.Any(), testCloud.ResourceGroup, gomock.Any(), gomock.Any()).Return(nil, test.firstDetachError).Times(1)
			}

			// Create context with custom timeout if specified
			testCtx := ctx
			if test.contextTimeout > 0 {
				testCtx, cancel = context.WithTimeout(ctx, test.contextTimeout)
				defer cancel()
			}

			err := common.DetachDisk(testCtx, test.diskName, diskURI, test.nodeName)

			if test.expectedErr {
				assert.Error(t, err, "TestCase[%d]: %s", i, test.desc)
			} else {
				assert.NoError(t, err, "TestCase[%d]: %s", i, test.desc)
			}

			if test.expectForceDetach {
				assert.Equal(t, 2, callCount, "TestCase[%d]: %s - Expected force detach to be called", i, test.desc)
			} else {
				assert.LessOrEqual(t, callCount, 1, "TestCase[%d]: %s - Force detach should not be called", i, test.desc)
			}
		})
	}
}

func TestCommonDetachDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCases := []struct {
		desc        string
		vmList      map[string]string
		nodeName    types.NodeName
		diskName    string
		expectedErr bool
	}{
		{
			desc:        "error should not be returned if there's no such instance corresponding to given nodeName",
			nodeName:    "vm1",
			expectedErr: true,
		},
		{
			desc:        "no error shall be returned if there's no matching disk according to given diskName",
			vmList:      map[string]string{"vm1": "PowerState/Running"},
			nodeName:    "vm1",
			diskName:    "diskx",
			expectedErr: false,
		},
		{
			desc:        "error shall be returned if the disk exists",
			vmList:      map[string]string{"vm1": "PowerState/Running"},
			nodeName:    "vm1",
			diskName:    "disk1",
			expectedErr: true,
		},
	}

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)
		common := &controllerCommon{
			cloud:              testCloud,
			lockMap:            newLockMap(),
			ForceDetachBackoff: true,
		}
		diskURI := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/disk-name",
			testCloud.SubscriptionID, testCloud.ResourceGroup)
		expectedVMs := setTestVirtualMachines(testCloud, test.vmList, false)
		mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
		for _, vm := range expectedVMs {
			mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, *vm.Name, gomock.Any()).Return(&vm, nil).AnyTimes()
		}
		if len(expectedVMs) == 0 {
			mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, gomock.Any(), gomock.Any()).Return(&armcompute.VirtualMachine{}, errors.New("instance not found")).AnyTimes()
		}
		mockVMClient.EXPECT().CreateOrUpdate(gomock.Any(), testCloud.ResourceGroup, gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

		err := common.DetachDisk(ctx, test.diskName, diskURI, test.nodeName)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s, err: %v", i, test.desc, err)
	}
}

func TestCommonUpdateVM(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCases := []struct {
		desc             string
		vmList           map[string]string
		nodeName         types.NodeName
		diskName         string
		isErrorRetriable bool
		expectedErr      bool
	}{
		{
			desc:        "error should not be returned if there's no such instance corresponding to given nodeName",
			nodeName:    "vm1",
			expectedErr: false,
		},
		{
			desc:             "an error should be returned if vmset detach failed with isErrorRetriable error",
			vmList:           map[string]string{"vm1": "PowerState/Running"},
			nodeName:         "vm1",
			isErrorRetriable: true,
			expectedErr:      true,
		},
		{
			desc:        "no error shall be returned if there's no matching disk according to given diskName",
			vmList:      map[string]string{"vm1": "PowerState/Running"},
			nodeName:    "vm1",
			diskName:    "diskx",
			expectedErr: false,
		},
		{
			desc:        "no error shall be returned if the disk exists",
			vmList:      map[string]string{"vm1": "PowerState/Running"},
			nodeName:    "vm1",
			diskName:    "disk1",
			expectedErr: false,
		},
	}

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)
		common := &controllerCommon{
			cloud:   testCloud,
			lockMap: newLockMap(),
		}
		expectedVMs := setTestVirtualMachines(testCloud, test.vmList, false)
		mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
		for _, vm := range expectedVMs {
			mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, *vm.Name, gomock.Any()).Return(&vm, nil).AnyTimes()
		}
		if len(expectedVMs) == 0 {
			mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, gomock.Any(), gomock.Any()).Return(&armcompute.VirtualMachine{}, nil).AnyTimes()
		}
		r := autorestmocks.NewResponseWithStatus("200", 200)
		r.Body.Close()
		r.Request.Method = http.MethodPut

		if test.isErrorRetriable {
			testCloud.CloudProviderBackoff = true
			testCloud.ResourceRequestBackoff = wait.Backoff{Steps: 1}
			mockVMClient.EXPECT().CreateOrUpdate(ctx, testCloud.ResourceGroup, gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("retriable error")).AnyTimes()
		} else {
			mockVMClient.EXPECT().CreateOrUpdate(ctx, testCloud.ResourceGroup, gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		}

		err := common.UpdateVM(ctx, test.nodeName)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s, err: %v", i, test.desc, err)
	}
}

func TestGetDiskLun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := []struct {
		desc        string
		diskName    string
		diskURI     string
		expectedLun int32
		expectedErr bool
	}{
		{
			desc:        "LUN -1 and error shall be returned if diskName != disk.Name or diskURI != disk.Vhd.URI",
			diskName:    "diskx",
			expectedLun: -1,
			expectedErr: true,
		},
		{
			desc:        "correct LUN and no error shall be returned if diskName = disk.Name",
			diskName:    "disk1",
			expectedLun: 0,
			expectedErr: false,
		},
	}

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)
		common := &controllerCommon{
			cloud:   testCloud,
			lockMap: newLockMap(),
		}
		expectedVMs := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
		mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
		for _, vm := range expectedVMs {
			mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, *vm.Name, gomock.Any()).Return(&vm, nil).AnyTimes()
		}

		lun, _, err := common.GetDiskLun(context.Background(), test.diskName, test.diskURI, "vm1")
		assert.Equal(t, test.expectedLun, lun, "TestCase[%d]: %s", i, test.desc)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s", i, test.desc)
	}
}

func TestSetDiskLun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := []struct {
		desc            string
		nodeName        string
		diskURI         string
		diskMap         map[string]*provider.AttachDiskOptions
		occupiedLuns    []int
		isDataDisksFull bool
		expectedErr     bool
		expectedLun     int32
	}{
		{
			desc:         "the minimal LUN shall be returned if there's enough room for extra disks",
			nodeName:     "nodeName",
			diskURI:      "diskURI",
			occupiedLuns: []int{0, 1, 2},
			diskMap:      map[string]*provider.AttachDiskOptions{"diskURI": {}},
			expectedLun:  3,
			expectedErr:  false,
		},
		{
			desc:         "occupied LUNs shall be skipped",
			nodeName:     "nodeName",
			diskURI:      "diskURI",
			occupiedLuns: []int{0, 1, 2, 3},
			diskMap:      map[string]*provider.AttachDiskOptions{"diskURI": {}},
			expectedLun:  4,
			expectedErr:  false,
		},
		{
			desc:            "LUN -1 and error shall be returned if there's no available LUN",
			nodeName:        "nodeName",
			diskURI:         "diskURI",
			diskMap:         map[string]*provider.AttachDiskOptions{"diskURI": {}},
			isDataDisksFull: true,
			expectedLun:     -1,
			expectedErr:     true,
		},
		{
			desc:        "diskURI1 is not in VM data disk list nor in diskMap",
			nodeName:    "nodeName",
			diskURI:     "diskURI1",
			diskMap:     map[string]*provider.AttachDiskOptions{"diskURI2": {}},
			expectedLun: -1,
			expectedErr: true,
		},
	}

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)
		common := &controllerCommon{
			cloud:   testCloud,
			lockMap: newLockMap(),
		}
		expectedVMs := setTestVirtualMachines(testCloud, map[string]string{test.nodeName: "PowerState/Running"}, test.isDataDisksFull)
		mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
		for _, vm := range expectedVMs {
			mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, *vm.Name, gomock.Any()).Return(&vm, nil).AnyTimes()
		}

		lun, err := common.SetDiskLun(context.Background(), types.NodeName(test.nodeName), test.diskURI, test.diskMap, test.occupiedLuns)
		assert.Equal(t, test.expectedLun, lun, "TestCase[%d]: %s", i, test.desc)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s", i, test.desc)
	}
}

func TestGetValidCreationData(t *testing.T) {
	sourceResourceSnapshotID := "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/snapshots/xxx"
	sourceResourceVolumeID := "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/disks/xxx"
	upperSourceResourceSnapshotID := strings.ToUpper(sourceResourceSnapshotID)
	upperSourceResourceVolumeID := strings.ToUpper(sourceResourceVolumeID)

	tests := []struct {
		subscriptionID   string
		resourceGroup    string
		sourceResourceID string
		sourceType       string
		expected1        armcompute.CreationData
		expected2        error
	}{
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: "",
			sourceType:       "",
			expected1: armcompute.CreationData{
				CreateOption: to.Ptr(armcompute.DiskCreateOptionEmpty),
			},
			expected2: nil,
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/snapshots/xxx",
			sourceType:       sourceSnapshot,
			expected1: armcompute.CreationData{
				CreateOption:     to.Ptr(armcompute.DiskCreateOptionCopy),
				SourceResourceID: &sourceResourceSnapshotID,
			},
			expected2: nil,
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: upperSourceResourceSnapshotID,
			sourceType:       sourceSnapshot,
			expected1: armcompute.CreationData{
				CreateOption:     to.Ptr(armcompute.DiskCreateOptionCopy),
				SourceResourceID: &upperSourceResourceSnapshotID,
			},
			expected2: nil,
		},
		{
			subscriptionID:   "xxx",
			resourceGroup:    "xxx",
			sourceResourceID: "xxx",
			sourceType:       sourceSnapshot,
			expected1: armcompute.CreationData{
				CreateOption:     to.Ptr(armcompute.DiskCreateOptionCopy),
				SourceResourceID: &sourceResourceSnapshotID,
			},
			expected2: nil,
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: "/subscriptions/23/providers/Microsoft.Compute/disks/name",
			sourceType:       sourceSnapshot,
			expected1:        armcompute.CreationData{},
			expected2:        fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", "/subscriptions//resourceGroups//providers/Microsoft.Compute/snapshots//subscriptions/23/providers/Microsoft.Compute/disks/name", azureconstants.DiskSnapshotPathRE),
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: "http://test.com/vhds/name",
			sourceType:       sourceSnapshot,
			expected1:        armcompute.CreationData{},
			expected2:        fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", "/subscriptions//resourceGroups//providers/Microsoft.Compute/snapshots/http://test.com/vhds/name", azureconstants.DiskSnapshotPathRE),
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: "/subscriptions/xxx/snapshots/xxx",
			sourceType:       sourceSnapshot,
			expected1:        armcompute.CreationData{},
			expected2:        fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", "/subscriptions//resourceGroups//providers/Microsoft.Compute/snapshots//subscriptions/xxx/snapshots/xxx", azureconstants.DiskSnapshotPathRE),
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/snapshots/xxx/snapshots/xxx/snapshots/xxx",
			sourceType:       sourceSnapshot,
			expected1:        armcompute.CreationData{},
			expected2:        fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/snapshots/xxx/snapshots/xxx/snapshots/xxx", azureconstants.DiskSnapshotPathRE),
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: "xxx",
			sourceType:       "",
			expected1: armcompute.CreationData{
				CreateOption: to.Ptr(armcompute.DiskCreateOptionEmpty),
			},
			expected2: nil,
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/disks/xxx",
			sourceType:       sourceVolume,
			expected1: armcompute.CreationData{
				CreateOption:     to.Ptr(armcompute.DiskCreateOptionCopy),
				SourceResourceID: &sourceResourceVolumeID,
			},
			expected2: nil,
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: upperSourceResourceVolumeID,
			sourceType:       sourceVolume,
			expected1: armcompute.CreationData{
				CreateOption:     to.Ptr(armcompute.DiskCreateOptionCopy),
				SourceResourceID: &upperSourceResourceVolumeID,
			},
			expected2: nil,
		},
		{
			subscriptionID:   "xxx",
			resourceGroup:    "xxx",
			sourceResourceID: "xxx",
			sourceType:       sourceVolume,
			expected1: armcompute.CreationData{
				CreateOption:     to.Ptr(armcompute.DiskCreateOptionCopy),
				SourceResourceID: &sourceResourceVolumeID,
			},
			expected2: nil,
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/snapshots/xxx",
			sourceType:       sourceVolume,
			expected1:        armcompute.CreationData{},
			expected2:        fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", "/subscriptions//resourceGroups//providers/Microsoft.Compute/disks//subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/snapshots/xxx", azureconstants.ManagedDiskPathRE),
		},
	}

	for _, test := range tests {
		options := ManagedDiskOptions{
			SourceResourceID: test.sourceResourceID,
			SourceType:       test.sourceType,
		}
		result, err := getValidCreationData(test.subscriptionID, test.resourceGroup, &options)
		if !reflect.DeepEqual(result, test.expected1) || !reflect.DeepEqual(err, test.expected2) {
			t.Errorf("input sourceResourceID: %v, sourceType: %v, getValidCreationData result: %v, expected1 : %v, err: %v, expected2: %v", test.sourceResourceID, test.sourceType, result, test.expected1, err, test.expected2)
		}
	}
}

func TestIsInstanceNotFoundError(t *testing.T) {
	testCases := []struct {
		errMsg         string
		expectedResult bool
	}{
		{
			errMsg:         "",
			expectedResult: false,
		},
		{
			errMsg:         "other error",
			expectedResult: false,
		},
		{
			errMsg:         "The provided instanceId 857 is not an active Virtual Machine Scale Set VM instanceId.",
			expectedResult: true,
		},
		{
			errMsg:         `compute.VirtualMachineScaleSetVMsClient#Update: Failure sending request: StatusCode=400 -- Original Error: Code="InvalidParameter" Message="The provided instanceId 1181 is not an active Virtual Machine Scale Set VM instanceId." Target="instanceIds"`,
			expectedResult: true,
		},
	}

	for i, test := range testCases {
		result := isInstanceNotFoundError(fmt.Errorf("%v", test.errMsg))
		assert.Equal(t, test.expectedResult, result, "TestCase[%d]", i, result)
	}
}

func TestAttachDiskRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := []struct {
		desc                 string
		diskURI              string
		nodeName             string
		diskName             string
		diskNum              int
		duplicateDiskRequest bool
		expectedErr          bool
	}{
		{
			desc:        "one disk request in queue",
			diskURI:     "diskURI",
			nodeName:    "nodeName",
			diskName:    "diskName",
			diskNum:     1,
			expectedErr: false,
		},
		{
			desc:        "multiple disk requests in queue",
			diskURI:     "diskURI",
			nodeName:    "nodeName",
			diskName:    "diskName",
			diskNum:     10,
			expectedErr: false,
		},
		{
			desc:        "zero disk request in queue",
			diskURI:     "diskURI",
			nodeName:    "nodeName",
			diskName:    "diskName",
			diskNum:     0,
			expectedErr: false,
		},
		{
			desc:                 "multiple disk requests in queue",
			diskURI:              "diskURI",
			nodeName:             "nodeName",
			diskName:             "diskName",
			duplicateDiskRequest: true,
			diskNum:              10,
			expectedErr:          false,
		},
	}

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)
		common := &controllerCommon{
			cloud:   testCloud,
			lockMap: newLockMap(),
		}
		for i := 1; i <= test.diskNum; i++ {
			diskURI := fmt.Sprintf("%s%d", test.diskURI, i)
			diskName := fmt.Sprintf("%s%d", test.diskName, i)
			ops := &provider.AttachDiskOptions{DiskName: diskName}
			_, err := common.batchAttachDiskRequest(diskURI, test.nodeName, ops)
			assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s", i, test.desc)
			if test.duplicateDiskRequest {
				_, err := common.batchAttachDiskRequest(diskURI, test.nodeName, ops)
				assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s", i, test.desc)
			}
		}

		diskURI := fmt.Sprintf("%s%d", test.diskURI, test.diskNum)
		diskMap, err := common.retrieveAttachBatchedDiskRequests(test.nodeName, diskURI)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s", i, test.desc)
		assert.Equal(t, test.diskNum, len(diskMap), "TestCase[%d]: %s", i, test.desc)
		for diskURI, opt := range diskMap {
			assert.Equal(t, strings.Contains(diskURI, test.diskURI), true, "TestCase[%d]: %s", i, test.desc)
			assert.Equal(t, strings.Contains(opt.DiskName, test.diskName), true, "TestCase[%d]: %s", i, test.desc)
		}
	}
}

func TestDetachDiskRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := []struct {
		desc                 string
		diskURI              string
		nodeName             string
		diskName             string
		diskNum              int
		duplicateDiskRequest bool
		expectedErr          bool
	}{
		{
			desc:        "one disk request in queue",
			diskURI:     "diskURI",
			nodeName:    "nodeName",
			diskName:    "diskName",
			diskNum:     1,
			expectedErr: false,
		},
		{
			desc:        "multiple disk requests in queue",
			diskURI:     "diskURI",
			nodeName:    "nodeName",
			diskName:    "diskName",
			diskNum:     10,
			expectedErr: false,
		},
		{
			desc:        "zero disk request in queue",
			diskURI:     "diskURI",
			nodeName:    "nodeName",
			diskName:    "diskName",
			diskNum:     0,
			expectedErr: false,
		},
		{
			desc:                 "multiple disk requests in queue",
			diskURI:              "diskURI",
			nodeName:             "nodeName",
			diskName:             "diskName",
			duplicateDiskRequest: true,
			diskNum:              10,
			expectedErr:          false,
		},
	}

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)
		common := &controllerCommon{
			cloud:   testCloud,
			lockMap: newLockMap(),
		}
		diskURI := ""
		for i := 1; i <= test.diskNum; i++ {
			diskURI = fmt.Sprintf("%s%d", test.diskURI, i)
			diskName := fmt.Sprintf("%s%d", test.diskName, i)
			_, err := common.batchDetachDiskRequest(diskName, diskURI, test.nodeName)
			assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s", i, test.desc)
			if test.duplicateDiskRequest {
				_, err := common.batchDetachDiskRequest(diskName, diskURI, test.nodeName)
				assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s", i, test.desc)
			}
		}

		diskMap, err := common.retrieveDetachBatchedDiskRequests(test.nodeName, diskURI)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s", i, test.desc)
		assert.Equal(t, test.diskNum, len(diskMap), "TestCase[%d]: %s", i, test.desc)
		for diskURI, diskName := range diskMap {
			assert.Equal(t, strings.Contains(diskURI, test.diskURI), true, "TestCase[%d]: %s", i, test.desc)
			assert.Equal(t, strings.Contains(diskName, test.diskName), true, "TestCase[%d]: %s", i, test.desc)
		}
	}
}

func TestGetDetachDiskRequestNum(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := []struct {
		desc                 string
		diskURI              string
		nodeName             string
		diskName             string
		diskNum              int
		duplicateDiskRequest bool
		expectedErr          bool
	}{
		{
			desc:        "one disk request in queue",
			diskURI:     "diskURI",
			nodeName:    "nodeName",
			diskName:    "diskName",
			diskNum:     1,
			expectedErr: false,
		},
		{
			desc:        "multiple disk requests in queue",
			diskURI:     "diskURI",
			nodeName:    "nodeName",
			diskName:    "diskName",
			diskNum:     10,
			expectedErr: false,
		},
		{
			desc:        "zero disk request in queue",
			diskURI:     "diskURI",
			nodeName:    "nodeName",
			diskName:    "diskName",
			diskNum:     0,
			expectedErr: false,
		},
		{
			desc:                 "multiple disk requests in queue",
			diskURI:              "diskURI",
			nodeName:             "nodeName",
			diskName:             "diskName",
			duplicateDiskRequest: true,
			diskNum:              10,
			expectedErr:          false,
		},
	}

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)
		common := &controllerCommon{
			cloud:   testCloud,
			lockMap: newLockMap(),
		}
		for i := 1; i <= test.diskNum; i++ {
			diskURI := fmt.Sprintf("%s%d", test.diskURI, i)
			diskName := fmt.Sprintf("%s%d", test.diskName, i)
			_, err := common.batchDetachDiskRequest(diskName, diskURI, test.nodeName)
			assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s", i, test.desc)
			if test.duplicateDiskRequest {
				_, err := common.batchDetachDiskRequest(diskName, diskURI, test.nodeName)
				assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s", i, test.desc)
			}
		}

		detachDiskReqeustNum, err := common.getDetachDiskRequestNum(test.nodeName)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s", i, test.desc)
		assert.Equal(t, test.diskNum, detachDiskReqeustNum, "TestCase[%d]: %s", i, test.desc)
	}
}

func TestVerifyAttach(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name           string
		diskName       string
		diskURI        string
		nodeName       types.NodeName
		expectedVM     map[string]string
		expectDisk     bool
		wantLun        int32
		wantErr        bool
		wantErrContent string
	}{
		{
			name:       "successfully finds disk lun",
			diskName:   "disk1",
			diskURI:    "diskuri1",
			nodeName:   "node1",
			expectedVM: map[string]string{"node1": "PowerState/Running"},
			expectDisk: true,
			wantLun:    2,
			wantErr:    false,
		},
		{
			name:           "returns error when disk not found",
			diskName:       "diskNotFound",
			diskURI:        "diskuri2",
			nodeName:       "node2",
			expectedVM:     map[string]string{"node2": "PowerState/Stopped"},
			wantLun:        -1,
			wantErr:        true,
			wantErrContent: "could not be found",
		},
		{
			name:           "returns error with vm not found",
			diskName:       "disk1",
			diskURI:        "diskuri3",
			nodeName:       "node3",
			expectedVM:     nil,
			wantLun:        -1,
			wantErr:        true,
			wantErrContent: "could not be found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCloud := provider.GetTestCloud(ctrl)
			mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
			common := &controllerCommon{
				cloud:   testCloud,
				lockMap: newLockMap(),
			}
			if tt.expectedVM == nil {
				mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, gomock.Any(), gomock.Any()).Return(&armcompute.VirtualMachine{}, errors.New("could not be found")).AnyTimes()
			} else {

				expectedVMs := setTestVirtualMachines(testCloud, tt.expectedVM, false)
				for _, vm := range expectedVMs {
					if tt.expectDisk {
						vm.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{
							{
								Lun:  ptr.To(int32(tt.wantLun)),
								Name: ptr.To(tt.diskName),
							},
						}
					}
					mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, *vm.Name, gomock.Any()).Return(&vm, nil).AnyTimes()
				}
			}

			lun, err := common.verifyAttach(context.Background(), tt.diskName, tt.diskURI, tt.nodeName)
			assert.Equal(t, tt.wantLun, lun)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.wantErrContent != "" {
					assert.Contains(t, err.Error(), tt.wantErrContent)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestVerifyDetach(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name           string
		diskName       string
		diskURI        string
		nodeName       types.NodeName
		expectedVM     map[string]string
		expectDisk     bool
		wantLun        int32
		expectedErr    bool
		expectedErrMsg string
	}{
		{
			name:        "successfully detached - error contains CannotFindDiskLUN",
			diskName:    "diskNotFound",
			diskURI:     "diskuri1",
			nodeName:    "node1",
			expectedVM:  map[string]string{"node1": "PowerState/Running"},
			expectedErr: false,
		},
		{
			name:           "error returned from GetDiskLun for VM does not exist",
			diskName:       "disk2",
			diskURI:        "diskuri2",
			nodeName:       "node2",
			expectedVM:     nil,
			expectedErr:    true,
			expectedErrMsg: "could not be found",
		},
		{
			name:           "disk still attached",
			expectDisk:     true,
			diskName:       "disk1",
			diskURI:        "diskuri",
			wantLun:        2,
			nodeName:       "node3",
			expectedErr:    true,
			expectedVM:     map[string]string{"node3": "PowerState/Running"},
			expectedErrMsg: "disk(diskuri) is still attached to node(node3) on lun(2), vmState: Succeeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCloud := provider.GetTestCloud(ctrl)
			mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
			common := &controllerCommon{
				cloud:   testCloud,
				lockMap: newLockMap(),
			}
			if tt.expectedVM == nil {
				mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, gomock.Any(), gomock.Any()).Return(&armcompute.VirtualMachine{}, errors.New("could not be found")).AnyTimes()
			} else {

				expectedVMs := setTestVirtualMachines(testCloud, tt.expectedVM, false)
				for _, vm := range expectedVMs {
					if tt.expectDisk {
						vm.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{
							{
								Lun:  ptr.To(int32(tt.wantLun)),
								Name: ptr.To(tt.diskName),
							},
						}
					}
					mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, *vm.Name, gomock.Any()).Return(&vm, nil).AnyTimes()
				}
			}

			err := common.verifyDetach(context.Background(), tt.diskName, tt.diskURI, tt.nodeName)
			if tt.expectedErr {
				assert.Error(t, err)
				klog.Info("Expected error occurred", "error", err)
				if tt.expectedErrMsg != "" {
					assert.Contains(t, err.Error(), tt.expectedErrMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConcurrentDetachDisk(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Skip test case on Windows")
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testCloud := provider.GetTestCloud(ctrl)
	common := &controllerCommon{
		cloud:                        testCloud,
		lockMap:                      newLockMap(),
		ForceDetachBackoff:           true,
		AttachDetachInitialDelayInMs: 1000,
		DisableDiskLunCheck:          true,
	}
	expectedVM := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
	mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)

	// Mock Get to always return the expected VM
	mockVMClient.EXPECT().
		Get(gomock.Any(), testCloud.ResourceGroup, *expectedVM[0].Name, gomock.Any()).
		Return(&expectedVM[0], nil).
		AnyTimes()

	// Use a counter to simulate different return values for CreateOrUpdate
	// First call should succeed, subsequent calls should fail
	callCount := int32(0)
	mockVMClient.EXPECT().
		CreateOrUpdate(gomock.Any(), testCloud.ResourceGroup, gomock.Any(), gomock.Any()).
		DoAndReturn(
			func(_ context.Context, _ string, name string, params armcompute.VirtualMachine) (*armcompute.VirtualMachine, error) {
				if atomic.AddInt32(&callCount, 1) == 1 {
					klog.Info("First call to CreateOrUpdate succeeded", "VM Name:", name, "Params:", params)
					time.Sleep(100 * time.Millisecond) // Simulate some processing time to hold the node lock while the 3rd detach request is made
					return nil, nil                    // First call succeeds
				}
				return nil, errors.New("internal error") // Subsequent calls fail
			}).
		AnyTimes()

	// Simulate concurrent detach requests that would be batched
	wg := &sync.WaitGroup{}
	errorChan := make(chan error, 1)
	for i := range 2 {
		diskURI := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s",
			testCloud.SubscriptionID, testCloud.ResourceGroup, fmt.Sprintf("disk-batched-%d", i))
		wg.Add(1)
		go func(i int, diskURI string) {
			defer wg.Done()
			err := common.DetachDisk(ctx, fmt.Sprintf("disk-batched-%d", i), diskURI, "vm1")
			if err != nil {
				errorChan <- err
			}
		}(i, diskURI)
	}

	time.Sleep(1005 * time.Millisecond) // Wait for the batching timeout
	diskURI := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s",
		testCloud.SubscriptionID, testCloud.ResourceGroup, "disk-not-batched")
	klog.Info("Calling DetachDisk for non-batched disk detach", expectedVM)
	// This should trigger a second CreateOrUpdate call that fails
	err := common.DetachDisk(ctx, "disk-not-batched", diskURI, "vm1")

	wg.Wait()
	select {
	case err := <-errorChan:
		// Will only fail if it triggered the second CreateOrUpdate call which could only be for the wrong disk - "disk-not-batched"
		// because the first CreateOrUpdate call was successful for the batched disks
		assert.NoError(t, err, "DetachDisk should not return an error for the batched disk detach")
	default:
		klog.Info("No error received from detach disk requests")
	}
	// Should fail due to the second CreateOrUpdate call returning an error
	assert.Error(t, err, "DetachDisk should return an error for the non-batched disk detach")
}

// setTestVirtualMachines sets test virtual machine with powerstate.
func setTestVirtualMachines(c *provider.Cloud, vmList map[string]string, isDataDisksFull bool) []armcompute.VirtualMachine {
	expectedVMs := make([]armcompute.VirtualMachine, 0)

	for nodeName, powerState := range vmList {
		nodeName := nodeName
		instanceID := fmt.Sprintf("/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/%s", nodeName)
		vm := armcompute.VirtualMachine{
			Name:     &nodeName,
			ID:       &instanceID,
			Location: &c.Location,
		}
		status := []*armcompute.InstanceViewStatus{
			{
				Code: ptr.To(powerState),
			},
			{
				Code: ptr.To("ProvisioningState/succeeded"),
			},
		}
		vm.Properties = &armcompute.VirtualMachineProperties{
			ProvisioningState: ptr.To(string(consts.ProvisioningStateSucceeded)),
			HardwareProfile: &armcompute.HardwareProfile{
				VMSize: ptr.To(armcompute.VirtualMachineSizeTypesStandardA0),
			},
			InstanceView: &armcompute.VirtualMachineInstanceView{
				Statuses: status,
			},
			StorageProfile: &armcompute.StorageProfile{
				DataDisks: []*armcompute.DataDisk{},
			},
		}
		if !isDataDisksFull {
			vm.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{
				{
					Lun:  ptr.To(int32(0)),
					Name: ptr.To("disk1"),
				},
				{
					Lun:  ptr.To(int32(1)),
					Name: ptr.To("disk2"),
				},
				{
					Lun:  ptr.To(int32(2)),
					Name: ptr.To("disk3"),
				},
			}
		} else {
			dataDisks := make([]*armcompute.DataDisk, maxLUN)
			for i := 0; i < maxLUN; i++ {
				dataDisks[i] = &armcompute.DataDisk{Lun: ptr.To(int32(i))}
			}
			vm.Properties.StorageProfile.DataDisks = dataDisks
		}

		expectedVMs = append(expectedVMs, vm)
	}

	return expectedVMs
}

func TestDetachDiskWithVMSSTimeoutConfiguration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := []struct {
		desc                   string
		vmssDetachTimeout      int
		detachOperationTimeout int
		expectEarlyExit        bool
	}{
		{
			desc:                   "vmssDetachTimeout less than detachOperationTimeout creates early exit",
			vmssDetachTimeout:      5,
			detachOperationTimeout: 10,
			expectEarlyExit:        true,
		},
		{
			desc:                   "vmssDetachTimeout zero uses detachOperationTimeout",
			vmssDetachTimeout:      0,
			detachOperationTimeout: 10,
			expectEarlyExit:        false,
		},
		{
			desc:                   "vmssDetachTimeout greater than detachOperationTimeout uses detachOperationTimeout",
			vmssDetachTimeout:      20,
			detachOperationTimeout: 10,
			expectEarlyExit:        false,
		},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			testCloud := provider.GetTestCloud(ctrl)
			common := &controllerCommon{
				cloud:                              testCloud,
				lockMap:                            newLockMap(),
				VMSSDetachTimeoutInSeconds:         test.vmssDetachTimeout,
				DetachOperationMinTimeoutInSeconds: test.detachOperationTimeout,
				ForceDetachBackoff:                 false,
				DisableDiskLunCheck:                true,
			}

			diskName := "disk1"
			diskURI := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s",
				testCloud.SubscriptionID, testCloud.ResourceGroup, diskName)

			expectedVMs := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
			mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)

			for _, vm := range expectedVMs {
				mockVMClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, *vm.Name, gomock.Any()).Return(&vm, nil).AnyTimes()
			}

			// Capture the context passed to CreateOrUpdate to verify timeout
			var capturedCtxDeadline time.Time
			mockVMClient.EXPECT().
				CreateOrUpdate(gomock.Any(), testCloud.ResourceGroup, gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, _ string, _ string, _ armcompute.VirtualMachine) (*armcompute.VirtualMachine, error) {
					deadline, ok := ctx.Deadline()
					if ok {
						capturedCtxDeadline = deadline
					}
					return nil, nil
				}).
				Times(1)

			err := common.DetachDisk(ctx, diskName, diskURI, "vm1")
			assert.NoError(t, err)

			// Verify timeout configuration
			if test.expectEarlyExit {
				// With early exit, the context should have timeout close to vmssDetachTimeout
				expectedTimeout := time.Duration(test.vmssDetachTimeout) * time.Second
				actualTimeout := time.Until(capturedCtxDeadline)
				assert.InDelta(t, expectedTimeout.Seconds(), actualTimeout.Seconds(), 1.0, "Expected early exit timeout")
			}
		})
	}
}

func TestPollForDetachCompletion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := []struct {
		desc              string
		diskName          string
		diskURI           string
		nodeName          types.NodeName
		setupMocks        func(*provider.Cloud, *mockvmclient.MockInterface)
		expectedErr       bool
		expectedErrString string
		contextTimeout    time.Duration
	}{
		{
			desc:           "successful detach - disk not found",
			diskName:       "disk1",
			diskURI:        "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1",
			nodeName:       "vm1",
			contextTimeout: 30 * time.Second,
			setupMocks: func(testCloud *provider.Cloud, mockVMClient *mockvmclient.MockInterface) {
				vms := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
				vm := vms[0]
				// Remove all data disks to simulate disk not found
				vm.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{}
				mockVMClient.EXPECT().
					Get(gomock.Any(), testCloud.ResourceGroup, "vm1", gomock.Any()).
					Return(&vm, nil).
					AnyTimes()
			},
			expectedErr: false,
		},
		{
			desc:           "detach in progress - eventually succeeds",
			diskName:       "disk1",
			diskURI:        "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1",
			nodeName:       "vm1",
			contextTimeout: 30 * time.Second,
			setupMocks: func(testCloud *provider.Cloud, mockVMClient *mockvmclient.MockInterface) {
				// First call: disk still attached with detach in progress
				vms1 := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
				vm1 := vms1[0]
				vm1.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{
					{
						Name:         to.Ptr("disk1"),
						Lun:          to.Ptr(int32(0)),
						ManagedDisk:  &armcompute.ManagedDiskParameters{ID: to.Ptr("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1")},
						ToBeDetached: to.Ptr(true),
					},
				}
				mockVMClient.EXPECT().
					Get(gomock.Any(), testCloud.ResourceGroup, "vm1", gomock.Any()).
					Return(&vm1, nil).
					Times(1)

				// Second call: disk successfully detached
				vms2 := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
				vm2 := vms2[0]
				vm2.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{} // No disks
				mockVMClient.EXPECT().
					Get(gomock.Any(), testCloud.ResourceGroup, "vm1", gomock.Any()).
					Return(&vm2, nil).
					AnyTimes()
			},
			expectedErr: false,
		},
		{
			desc:           "detach still in progress after multiple polls - eventually succeeds",
			diskName:       "disk1",
			diskURI:        "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1",
			nodeName:       "vm1",
			contextTimeout: 30 * time.Second,
			setupMocks: func(testCloud *provider.Cloud, mockVMClient *mockvmclient.MockInterface) {
				// First 3 calls: disk still attached (no ToBeDetached flag)
				vms1 := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
				vm1 := vms1[0]
				vm1.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{
					{
						Name:        to.Ptr("disk1"),
						Lun:         to.Ptr(int32(0)),
						ManagedDisk: &armcompute.ManagedDiskParameters{ID: to.Ptr("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1")},
					},
				}
				mockVMClient.EXPECT().
					Get(gomock.Any(), testCloud.ResourceGroup, "vm1", gomock.Any()).
					Return(&vm1, nil).
					Times(3)

				// Fourth call: disk successfully detached
				vms2 := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
				vm2 := vms2[0]
				vm2.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{} // No disks
				mockVMClient.EXPECT().
					Get(gomock.Any(), testCloud.ResourceGroup, "vm1", gomock.Any()).
					Return(&vm2, nil).
					AnyTimes()
			},
			expectedErr: false,
		},
		{
			desc:           "throttled request - error returned",
			diskName:       "disk1",
			diskURI:        "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1",
			nodeName:       "vm1",
			contextTimeout: 30 * time.Second,
			setupMocks: func(testCloud *provider.Cloud, mockVMClient *mockvmclient.MockInterface) {
				throttleErr := &azcore.ResponseError{
					StatusCode: http.StatusTooManyRequests,
					RawResponse: &http.Response{
						StatusCode: http.StatusTooManyRequests,
					},
				}
				mockVMClient.EXPECT().
					Get(gomock.Any(), testCloud.ResourceGroup, "vm1", gomock.Any()).
					Return(nil, throttleErr).
					Times(1)
			},
			expectedErr:       true,
			expectedErrString: "429",
		},
		{
			desc:           "vm not found - error returned",
			diskName:       "disk1",
			diskURI:        "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1",
			nodeName:       "vm1",
			contextTimeout: 30 * time.Second,
			setupMocks: func(testCloud *provider.Cloud, mockVMClient *mockvmclient.MockInterface) {
				notFoundErr := &azcore.ResponseError{
					StatusCode: http.StatusNotFound,
					RawResponse: &http.Response{
						StatusCode: http.StatusNotFound,
					},
				}
				mockVMClient.EXPECT().
					Get(gomock.Any(), testCloud.ResourceGroup, "vm1", gomock.Any()).
					Return(nil, notFoundErr).
					Times(1)
			},
			expectedErr:       true,
			expectedErrString: "not found",
		},
		{
			desc:           "context timeout - error returned",
			diskName:       "disk1",
			diskURI:        "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1",
			nodeName:       "vm1",
			contextTimeout: 100 * time.Millisecond,
			setupMocks: func(testCloud *provider.Cloud, mockVMClient *mockvmclient.MockInterface) {
				// Keep returning disk as attached, causing timeout
				vms := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
				vm := vms[0]
				vm.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{
					{
						Name:        to.Ptr("disk1"),
						Lun:         to.Ptr(int32(0)),
						ManagedDisk: &armcompute.ManagedDiskParameters{ID: to.Ptr("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1")},
					},
				}
				mockVMClient.EXPECT().
					Get(gomock.Any(), testCloud.ResourceGroup, "vm1", gomock.Any()).
					Return(&vm, nil).
					AnyTimes()
			},
			expectedErr:       true,
			expectedErrString: "context deadline exceeded",
		},
		{
			desc:           "detach with ToBeDetached flag - eventually succeeds",
			diskName:       "disk1",
			diskURI:        "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1",
			nodeName:       "vm1",
			contextTimeout: 30 * time.Second,
			setupMocks: func(testCloud *provider.Cloud, mockVMClient *mockvmclient.MockInterface) {
				// First call: disk with ToBeDetached=true
				vms1 := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
				vm1 := vms1[0]
				vm1.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{
					{
						Name:         to.Ptr("disk1"),
						Lun:          to.Ptr(int32(0)),
						ManagedDisk:  &armcompute.ManagedDiskParameters{ID: to.Ptr("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1")},
						ToBeDetached: to.Ptr(true),
					},
				}
				mockVMClient.EXPECT().
					Get(gomock.Any(), testCloud.ResourceGroup, "vm1", gomock.Any()).
					Return(&vm1, nil).
					Times(1)

				// Second call: disk removed
				vms2 := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
				vm2 := vms2[0]
				vm2.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{} // No disks
				mockVMClient.EXPECT().
					Get(gomock.Any(), testCloud.ResourceGroup, "vm1", gomock.Any()).
					Return(&vm2, nil).
					AnyTimes()
			},
			expectedErr: false,
		},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), test.contextTimeout)
			defer cancel()

			testCloud := provider.GetTestCloud(ctrl)
			mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)

			// Setup mocks based on test case
			test.setupMocks(testCloud, mockVMClient)

			// Create controller common
			common := &controllerCommon{
				cloud:   testCloud,
				lockMap: newLockMap(),
			}

			// Call pollForDetachCompletion
			err := common.pollForDetachCompletion(ctx, test.diskName, test.diskURI, test.nodeName, testCloud.VMSet)

			// Assert results
			if test.expectedErr {
				assert.Error(t, err)
				if test.expectedErrString != "" {
					assert.Contains(t, strings.ToLower(err.Error()), strings.ToLower(test.expectedErrString))
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPollForDetachCompletionWithCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("cache is cleared between polls", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		testCloud := provider.GetTestCloud(ctrl)
		mockVMClient := testCloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)

		diskName := "disk1"
		diskURI := "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1"
		nodeName := types.NodeName("vm1")

		// Track how many times Get is called
		var getCallCount int32

		// First call: disk still attached
		vms1 := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
		vm1 := vms1[0]
		vm1.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{
			{
				Name:        to.Ptr(diskName),
				Lun:         to.Ptr(int32(0)),
				ManagedDisk: &armcompute.ManagedDiskParameters{ID: to.Ptr(diskURI)},
			},
		}

		// Second call: disk detached
		vms2 := setTestVirtualMachines(testCloud, map[string]string{"vm1": "PowerState/Running"}, false)
		vm2 := vms2[0]
		vm2.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{} // No disks

		mockVMClient.EXPECT().
			Get(gomock.Any(), testCloud.ResourceGroup, "vm1", gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ string, _ *string) (*armcompute.VirtualMachine, error) {
				count := atomic.AddInt32(&getCallCount, 1)
				if count == 1 {
					return &vm1, nil
				}
				return &vm2, nil
			}).
			AnyTimes()

		common := &controllerCommon{
			cloud:   testCloud,
			lockMap: newLockMap(),
		}

		err := common.pollForDetachCompletion(ctx, diskName, diskURI, nodeName, testCloud.VMSet)
		assert.NoError(t, err)
		// Verify that Get was called at least twice (once with disk, once without)
		assert.GreaterOrEqual(t, atomic.LoadInt32(&getCallCount), int32(2), "Expected at least 2 GET calls to verify cache refresh")
	})
}

func TestPreemptedByForceOperation(t *testing.T) {
	testCases := []struct {
		desc     string
		err      error
		expected bool
	}{
		{
			desc:     "DataDisksForceDetached error - lowercase",
			err:      fmt.Errorf("operation failed with error: datadisksforcedetached"),
			expected: true,
		},
		{
			desc:     "DataDisksForceDetached error - uppercase",
			err:      fmt.Errorf("operation failed with error: DATADISKSFORCEDETACHED"),
			expected: true,
		},
		{
			desc:     "DataDisksForceDetached error - mixed case",
			err:      fmt.Errorf("operation failed with error: DataDisksForceDetached"),
			expected: true,
		},
		{
			desc:     "DataDisksForceDetached error - in middle of message",
			err:      fmt.Errorf("The disk operation was cancelled because DataDisksForceDetached was in progress"),
			expected: true,
		},
		{
			desc:     "DataDisksForceDetached error - Azure specific message",
			err:      fmt.Errorf("Compute.VirtualMachineScaleSetsClient#Update: Failure responding to request: StatusCode=409 -- Original Error: autorest/azure: Service returned an error. Status=<nil> Code=\"DataDisksForceDetached\" Message=\"The disks were force detached during the operation.\""),
			expected: true,
		},
		{
			desc:     "Different error - disk not found",
			err:      fmt.Errorf("disk not found"),
			expected: false,
		},
		{
			desc:     "Different error - timeout",
			err:      fmt.Errorf("context deadline exceeded"),
			expected: false,
		},
		{
			desc:     "Different error - throttled",
			err:      fmt.Errorf("too many requests"),
			expected: false,
		},
		{
			desc:     "Different error - similar but not matching",
			err:      fmt.Errorf("DataDisks operation failed"),
			expected: false,
		},
		{
			desc:     "Different error - partial match",
			err:      fmt.Errorf("ForceDetached failed"),
			expected: false,
		},
		{
			desc:     "Wrapped error with DataDisksForceDetached",
			err:      fmt.Errorf("detach failed: %w", fmt.Errorf("DataDisksForceDetached")),
			expected: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			result := preemptedByForceOperation(test.err)
			assert.Equal(t, test.expected, result, "Test case: %s", test.desc)
		})
	}
}
