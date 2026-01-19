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
	"fmt"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/diskclient/mock_diskclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

const (
	fakeGetDiskFailed = "fakeGetDiskFailed"
	disk1Name         = "disk1"
	disk1ID           = "diskid1"
)

func TestCreateManagedDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxShare := int32(2)
	goodDiskEncryptionSetID := fmt.Sprintf("/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/diskEncryptionSets/%s", "diskEncryptionSet-name")
	badDiskEncryptionSetID := "badDiskEncryptionSetID"
	testTags := make(map[string]*string)
	testTags[WriteAcceleratorEnabled] = ptr.To("true")
	testCases := []struct {
		desc                string
		diskID              string
		diskName            string
		storageAccountType  armcompute.DiskStorageAccountTypes
		diskIOPSReadWrite   string
		diskMBPSReadWrite   string
		diskEncryptionSetID string
		diskEncryptionType  string
		subscriptionID      string
		resouceGroup        string
		publicNetworkAccess armcompute.PublicNetworkAccess
		networkAccessPolicy armcompute.NetworkAccessPolicy
		diskAccessID        *string
		expectedDiskID      string
		existedDisk         *armcompute.Disk
		expectedErr         bool
		expectedErrMsg      error
	}{
		{
			desc:                "disk Id and no error shall be returned if everything is good with DiskStorageAccountTypesUltraSSDLRS storage account",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesUltraSSDLRS,
			diskIOPSReadWrite:   "100",
			diskMBPSReadWrite:   "100",
			diskEncryptionSetID: goodDiskEncryptionSetID,
			expectedDiskID:      disk1ID,
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         false,
		},
		{
			desc:                "disk Id and no error shall be returned if everything is good with PremiumV2LRS storage account",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesPremiumV2LRS,
			diskIOPSReadWrite:   "100",
			diskMBPSReadWrite:   "100",
			diskEncryptionSetID: goodDiskEncryptionSetID,
			expectedDiskID:      disk1ID,
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         false,
		},
		{
			desc:                "disk Id and no error shall be returned if everything is good with PremiumV2LRS storage account",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesPremiumV2LRS,
			diskIOPSReadWrite:   "",
			diskMBPSReadWrite:   "",
			diskEncryptionSetID: goodDiskEncryptionSetID,
			expectedDiskID:      disk1ID,
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         false,
		},
		{
			desc:                "disk Id and no error shall be returned if everything is good with DiskStorageAccountTypesStandardLRS storage account",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesStandardLRS,
			diskIOPSReadWrite:   "",
			diskMBPSReadWrite:   "",
			diskEncryptionSetID: goodDiskEncryptionSetID,
			expectedDiskID:      disk1ID,
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         false,
		},
		{
			desc:                "empty diskid and an error shall be returned if everything is good with DiskStorageAccountTypesUltraSSDLRS storage account but DiskIOPSReadWrite is invalid",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesUltraSSDLRS,
			diskIOPSReadWrite:   "invalid",
			diskMBPSReadWrite:   "100",
			diskEncryptionSetID: goodDiskEncryptionSetID,
			expectedDiskID:      "",
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         true,
			expectedErrMsg:      fmt.Errorf("AzureDisk - failed to parse DiskIOPSReadWrite: strconv.Atoi: parsing \"invalid\": invalid syntax"),
		},
		{
			desc:                "empty diskid and an error shall be returned if everything is good with DiskStorageAccountTypesUltraSSDLRS storage account but DiskMBPSReadWrite is invalid",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesUltraSSDLRS,
			diskIOPSReadWrite:   "100",
			diskMBPSReadWrite:   "invalid",
			diskEncryptionSetID: goodDiskEncryptionSetID,
			expectedDiskID:      "",
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         true,
			expectedErrMsg:      fmt.Errorf("AzureDisk - failed to parse DiskMBpsReadWrite: strconv.Atoi: parsing \"invalid\": invalid syntax"),
		},
		{
			desc:                "empty diskid and an error shall be returned if everything is good with DiskStorageAccountTypesUltraSSDLRS storage account with bad Disk EncryptionSetID",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesUltraSSDLRS,
			diskIOPSReadWrite:   "100",
			diskMBPSReadWrite:   "100",
			diskEncryptionSetID: badDiskEncryptionSetID,
			expectedDiskID:      "",
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         true,
			expectedErrMsg:      fmt.Errorf("AzureDisk - format of DiskEncryptionSetID(%s) is incorrect, correct format: %s", badDiskEncryptionSetID, consts.DiskEncryptionSetIDFormat),
		},
		{
			desc:                "DiskEncryptionType should be empty when DiskEncryptionSetID is not set",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesStandardLRS,
			diskEncryptionSetID: "",
			diskEncryptionType:  "EncryptionAtRestWithCustomerKey",
			expectedDiskID:      "",
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         true,
			expectedErrMsg:      fmt.Errorf("AzureDisk - DiskEncryptionType(EncryptionAtRestWithCustomerKey) should be empty when DiskEncryptionSetID is not set"),
		},
		{
			desc:                "disk Id and no error shall be returned if everything is good with DiskStorageAccountTypesStandardLRS storage account with not empty diskIOPSReadWrite",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesStandardLRS,
			diskIOPSReadWrite:   "100",
			diskMBPSReadWrite:   "",
			diskEncryptionSetID: goodDiskEncryptionSetID,
			expectedDiskID:      "",
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         true,
			expectedErrMsg:      fmt.Errorf("AzureDisk - DiskIOPSReadWrite parameter is only applicable in UltraSSD_LRS disk type"),
		},
		{
			desc:                "disk Id and no error shall be returned if everything is good with DiskStorageAccountTypesStandardLRS storage account with not empty diskMBPSReadWrite",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesStandardLRS,
			diskIOPSReadWrite:   "",
			diskMBPSReadWrite:   "100",
			diskEncryptionSetID: goodDiskEncryptionSetID,
			expectedDiskID:      "",
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         true,
			expectedErrMsg:      fmt.Errorf("AzureDisk - DiskMBpsReadWrite parameter is only applicable in UltraSSD_LRS disk type"),
		},
		{
			desc:                "correct NetworkAccessPolicy(DenyAll) setting",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesStandardLRS,
			diskEncryptionSetID: goodDiskEncryptionSetID,
			networkAccessPolicy: armcompute.NetworkAccessPolicyDenyAll,
			publicNetworkAccess: armcompute.PublicNetworkAccessDisabled,
			expectedDiskID:      disk1ID,
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         false,
		},
		{
			desc:                "correct NetworkAccessPolicy(AllowAll) setting",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesStandardLRS,
			diskEncryptionSetID: goodDiskEncryptionSetID,
			diskEncryptionType:  "EncryptionAtRestWithCustomerKey",
			networkAccessPolicy: armcompute.NetworkAccessPolicyAllowAll,
			publicNetworkAccess: armcompute.PublicNetworkAccessEnabled,
			expectedDiskID:      disk1ID,
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         false,
		},
		{
			desc:                "DiskAccessID should not be empty when NetworkAccessPolicy is AllowPrivate",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesStandardLRS,
			diskEncryptionSetID: goodDiskEncryptionSetID,
			networkAccessPolicy: armcompute.NetworkAccessPolicyAllowPrivate,
			expectedDiskID:      "",
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         true,
			expectedErrMsg:      fmt.Errorf("DiskAccessID should not be empty when NetworkAccessPolicy is AllowPrivate"),
		},
		{
			desc:                "DiskAccessID(%s) must be empty when NetworkAccessPolicy(%s) is not AllowPrivate",
			diskID:              disk1ID,
			diskName:            disk1Name,
			storageAccountType:  armcompute.DiskStorageAccountTypesStandardLRS,
			diskEncryptionSetID: goodDiskEncryptionSetID,
			networkAccessPolicy: armcompute.NetworkAccessPolicyAllowAll,
			diskAccessID:        ptr.To("diskAccessID"),
			expectedDiskID:      "",
			existedDisk:         &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:         true,
			expectedErrMsg:      fmt.Errorf("DiskAccessID(diskAccessID) must be empty when NetworkAccessPolicy(AllowAll) is not AllowPrivate"),
		},
		{
			desc:           "resourceGroup must be specified when subscriptionID is not empty",
			diskID:         "",
			diskName:       disk1Name,
			subscriptionID: "abc",
			resouceGroup:   "",
			expectedDiskID: "",
			existedDisk:    &armcompute.Disk{ID: ptr.To(disk1ID), Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{Encryption: &armcompute.Encryption{DiskEncryptionSetID: &goodDiskEncryptionSetID, Type: to.Ptr(armcompute.EncryptionTypeEncryptionAtRestWithCustomerKey)}, ProvisioningState: ptr.To("Succeeded")}, Tags: testTags},
			expectedErr:    true,
			expectedErrMsg: fmt.Errorf("resourceGroup must be specified when subscriptionID(abc) is not empty"),
		},
	}

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)

		common := &controllerCommon{
			cloud:                              testCloud,
			lockMap:                            newLockMap(),
			AttachDetachInitialDelayInMs:       defaultAttachDetachInitialDelayInMs,
			DetachOperationMinTimeoutInSeconds: defaultDetachOperationMinTimeoutInSeconds,
			clientFactory:                      testCloud.ComputeClientFactory,
		}

		managedDiskController := &ManagedDiskController{common}
		volumeOptions := &ManagedDiskOptions{
			DiskName:            test.diskName,
			StorageAccountType:  test.storageAccountType,
			ResourceGroup:       test.resouceGroup,
			SizeGB:              1,
			Tags:                map[string]string{"tag1": "azure-tag1"},
			AvailabilityZone:    "westus-testzone",
			DiskIOPSReadWrite:   test.diskIOPSReadWrite,
			DiskMBpsReadWrite:   test.diskMBPSReadWrite,
			DiskEncryptionSetID: test.diskEncryptionSetID,
			DiskEncryptionType:  test.diskEncryptionType,
			MaxShares:           maxShare,
			NetworkAccessPolicy: test.networkAccessPolicy,
			PublicNetworkAccess: test.publicNetworkAccess,
			DiskAccessID:        test.diskAccessID,
			SubscriptionID:      test.subscriptionID,
		}

		mockDisksClient := mock_diskclient.NewMockInterface(ctrl)
		common.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(testCloud.SubscriptionID).Return(mockDisksClient, nil).AnyTimes()
		//disk := getTestDisk(test.diskName)
		mockDisksClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), test.diskName, gomock.Any()).Return(test.existedDisk, nil).AnyTimes()
		mockDisksClient.EXPECT().Get(gomock.Any(), gomock.Any(), test.diskName).Return(test.existedDisk, nil).AnyTimes()

		actualDiskID, err := managedDiskController.CreateManagedDisk(ctx, volumeOptions)
		assert.Equal(t, test.expectedDiskID, actualDiskID, "TestCase[%d]: %s", i, test.desc)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s, return error: %v", i, test.desc, err)
		if test.expectedErr {
			assert.EqualError(t, test.expectedErrMsg, err.Error(), "TestCase[%d]: %s, expected error: %v, return error: %v", i, test.desc, test.expectedErrMsg, err)
		}
	}
}

func TestCreateManagedDiskWithExtendedLocation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCloud := provider.GetTestCloudWithExtendedLocation(ctrl)
	diskName := disk1Name
	expectedDiskID := disk1ID
	el := &armcompute.ExtendedLocation{
		Name: ptr.To("microsoftlosangeles1"),
		Type: to.Ptr(armcompute.ExtendedLocationTypesEdgeZone),
	}

	diskreturned := armcompute.Disk{
		ID:               ptr.To(expectedDiskID),
		Name:             ptr.To(diskName),
		ExtendedLocation: el,
		Properties: &armcompute.DiskProperties{
			ProvisioningState: ptr.To("Succeeded"),
		},
	}

	common := &controllerCommon{
		cloud:                              testCloud,
		lockMap:                            newLockMap(),
		AttachDetachInitialDelayInMs:       defaultAttachDetachInitialDelayInMs,
		DetachOperationMinTimeoutInSeconds: defaultDetachOperationMinTimeoutInSeconds,
		clientFactory:                      testCloud.ComputeClientFactory,
	}

	managedDiskController := &ManagedDiskController{common}
	volumeOptions := &ManagedDiskOptions{
		DiskName:           diskName,
		StorageAccountType: armcompute.DiskStorageAccountTypes(armcompute.EdgeZoneStorageAccountTypePremiumLRS),
		ResourceGroup:      "",
		SizeGB:             1,
		AvailabilityZone:   "westus-testzone",
	}

	mockDisksClient := mock_diskclient.NewMockInterface(ctrl)
	common.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(testCloud.SubscriptionID).Return(mockDisksClient, nil).AnyTimes()
	mockDisksClient.EXPECT().CreateOrUpdate(gomock.Any(), testCloud.ResourceGroup, diskName, gomock.Any()).Return(to.Ptr(diskreturned), nil)

	mockDisksClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, diskName).Return(&diskreturned, nil).AnyTimes()

	actualDiskID, err := managedDiskController.CreateManagedDisk(ctx, volumeOptions)
	assert.Equal(t, expectedDiskID, actualDiskID, "Disk ID does not match.")
	assert.Nil(t, err, "There should not be an error.")
}

func TestDeleteManagedDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := []struct {
		desc           string
		diskName       string
		diskState      string
		existedDisk    *armcompute.Disk
		expectedErr    bool
		expectedErrMsg error
	}{
		{
			desc:           "an error shall be returned if delete an attaching disk",
			diskName:       disk1Name,
			diskState:      "attaching",
			existedDisk:    &armcompute.Disk{Name: ptr.To(disk1Name)},
			expectedErr:    true,
			expectedErrMsg: fmt.Errorf("failed to delete disk(/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/disks/disk1) since it's in attaching state"),
		},
		{
			desc:        "no error shall be returned if everything is good",
			diskName:    disk1Name,
			existedDisk: &armcompute.Disk{Name: ptr.To(disk1Name)},
			expectedErr: false,
		},
		{
			desc:           "an error shall be returned if get disk failed",
			diskName:       fakeGetDiskFailed,
			existedDisk:    &armcompute.Disk{Name: ptr.To(fakeGetDiskFailed)},
			expectedErr:    true,
			expectedErrMsg: fmt.Errorf("Get Disk failed"),
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)

		common := &controllerCommon{
			cloud:                              testCloud,
			lockMap:                            newLockMap(),
			AttachDetachInitialDelayInMs:       defaultAttachDetachInitialDelayInMs,
			DetachOperationMinTimeoutInSeconds: defaultDetachOperationMinTimeoutInSeconds,
			clientFactory:                      testCloud.ComputeClientFactory,
		}

		managedDiskController := &ManagedDiskController{common}
		diskURI := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s",
			testCloud.SubscriptionID, testCloud.ResourceGroup, *test.existedDisk.Name)
		if test.diskState == "attaching" {
			managedDiskController.diskStateMap.Store(strings.ToLower(diskURI), test.diskState)
		}

		mockDisksClient := mock_diskclient.NewMockInterface(ctrl)
		common.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(testCloud.SubscriptionID).Return(mockDisksClient, nil).AnyTimes()
		if test.diskName == fakeGetDiskFailed {
			mockDisksClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, test.diskName).Return(test.existedDisk, fmt.Errorf("Get Disk failed")).AnyTimes()
		} else {
			mockDisksClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, test.diskName).Return(test.existedDisk, nil).AnyTimes()
		}
		mockDisksClient.EXPECT().Delete(gomock.Any(), testCloud.ResourceGroup, test.diskName).Return(nil).AnyTimes()

		err := managedDiskController.DeleteManagedDisk(ctx, diskURI)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s, return error: %v", i, test.desc, err)
		if test.expectedErr {
			assert.EqualError(t, test.expectedErrMsg, err.Error(), "TestCase[%d]: %s, expected: %v, return: %v", i, test.desc, test.expectedErrMsg, err)
		}
	}
}

func TestGetDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCases := []struct {
		desc           string
		diskName       string
		existedDisk    *armcompute.Disk
		expectedErr    bool
		expectedErrMsg error
	}{
		{
			desc:        "no error shall be returned if get a normal disk without DiskProperties",
			diskName:    disk1Name,
			existedDisk: &armcompute.Disk{Name: ptr.To(disk1Name)},
			expectedErr: false,
		},
		{
			desc:           "an error shall be returned if get disk failed",
			diskName:       fakeGetDiskFailed,
			existedDisk:    &armcompute.Disk{Name: ptr.To(fakeGetDiskFailed)},
			expectedErr:    true,
			expectedErrMsg: fmt.Errorf("Get Disk failed"),
		},
	}

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)
		managedDiskController := &ManagedDiskController{
			controllerCommon: &controllerCommon{
				cloud:               testCloud,
				lockMap:             newLockMap(),
				DisableDiskLunCheck: true,
				clientFactory:       testCloud.ComputeClientFactory,
			},
		}

		mockDisksClient := mock_diskclient.NewMockInterface(ctrl)
		managedDiskController.controllerCommon.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub("").Return(mockDisksClient, nil).AnyTimes()
		if test.diskName == fakeGetDiskFailed {
			mockDisksClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, test.diskName).Return(test.existedDisk, fmt.Errorf("Get Disk failed")).AnyTimes()
		} else {
			mockDisksClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, test.diskName).Return(test.existedDisk, nil).AnyTimes()
		}

		disk, err := managedDiskController.GetDisk(ctx, "", testCloud.ResourceGroup, test.diskName)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s, return error: %v", i, test.desc, err)
		if test.expectedErr {
			assert.Equal(t, test.existedDisk, disk, "TestCase[%d]: %s, expected: %v, return: %v", i, test.desc, test.existedDisk, disk)
			assert.EqualError(t, test.expectedErrMsg, err.Error(), "TestCase[%d]: %s, expected: %v, return: %v", i, test.desc, test.expectedErrMsg, err)
		} else {
			assert.Equal(t, *test.existedDisk, *disk, "TestCase[%d]: %s, expected: %v, return: %v", i, test.desc, test.existedDisk, disk)
		}
	}
}

func TestGetDiskByURI(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCases := []struct {
		desc           string
		diskURI        string
		existedDisk    *armcompute.Disk
		expectedErr    bool
		expectedErrMsg error
	}{
		{
			desc:        "no error shall be returned if get a normal disk without DiskProperties",
			diskURI:     fmt.Sprintf("/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/disks/%s", disk1Name),
			existedDisk: &armcompute.Disk{Name: ptr.To(disk1Name)},
			expectedErr: false,
		},
		{
			desc:           "an error shall be returned if get disk failed",
			diskURI:        fmt.Sprintf("/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/disks/%s", fakeGetDiskFailed),
			existedDisk:    &armcompute.Disk{Name: ptr.To(fakeGetDiskFailed)},
			expectedErr:    true,
			expectedErrMsg: fmt.Errorf("Get Disk failed"),
		},
	}

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)
		managedDiskController := &ManagedDiskController{
			controllerCommon: &controllerCommon{
				cloud:               testCloud,
				lockMap:             newLockMap(),
				DisableDiskLunCheck: true,
				clientFactory:       testCloud.ComputeClientFactory,
			},
		}

		mockDisksClient := mock_diskclient.NewMockInterface(ctrl)
		managedDiskController.controllerCommon.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub("subscription").Return(mockDisksClient, nil).AnyTimes()
		if test.diskURI == fmt.Sprintf("/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/disks/%s", fakeGetDiskFailed) {
			mockDisksClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, fakeGetDiskFailed).Return(test.existedDisk, fmt.Errorf("Get Disk failed")).AnyTimes()
		} else {
			mockDisksClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, disk1Name).Return(test.existedDisk, nil).AnyTimes()
		}

		disk, err := managedDiskController.GetDiskByURI(ctx, test.diskURI)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s, return error: %v", i, test.desc, err)
		if test.expectedErr {
			assert.Equal(t, test.existedDisk, disk, "TestCase[%d]: %s, expected: %v, return: %v", i, test.desc, test.existedDisk, disk)
			assert.EqualError(t, test.expectedErrMsg, err.Error(), "TestCase[%d]: %s, expected: %v, return: %v", i, test.desc, test.expectedErrMsg, err)
		} else {
			assert.Equal(t, *test.existedDisk, *disk, "TestCase[%d]: %s, expected: %v, return: %v", i, test.desc, test.existedDisk, disk)
		}
	}
}

func TestResizeDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	diskSizeGB := int32(2)
	diskName := disk1Name
	fakeCreateDiskFailed := "fakeCreateDiskFailed"
	testCases := []struct {
		desc             string
		diskName         string
		oldSize          resource.Quantity
		newSize          resource.Quantity
		existedDisk      *armcompute.Disk
		expectedQuantity resource.Quantity
		expectedErr      bool
		expectedErrMsg   error
	}{
		{
			desc:             "new quantity and no error shall be returned if everything is good",
			diskName:         diskName,
			oldSize:          *resource.NewQuantity(2*(1024*1024*1024), resource.BinarySI),
			newSize:          *resource.NewQuantity(3*(1024*1024*1024), resource.BinarySI),
			existedDisk:      &armcompute.Disk{Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{DiskSizeGB: &diskSizeGB, DiskState: to.Ptr(armcompute.DiskStateUnattached)}},
			expectedQuantity: *resource.NewQuantity(3*(1024*1024*1024), resource.BinarySI),
			expectedErr:      false,
		},
		{
			desc:             "new quantity and no error shall be returned if everything is good with DiskProperties is null",
			diskName:         diskName,
			oldSize:          *resource.NewQuantity(2*(1024*1024*1024), resource.BinarySI),
			newSize:          *resource.NewQuantity(3*(1024*1024*1024), resource.BinarySI),
			existedDisk:      &armcompute.Disk{Name: ptr.To(disk1Name)},
			expectedQuantity: *resource.NewQuantity(2*(1024*1024*1024), resource.BinarySI),
			expectedErr:      true,
			expectedErrMsg:   fmt.Errorf("DiskProperties of disk(%s) is nil", diskName),
		},
		{
			desc:             "new quantity and no error shall be returned if everything is good with disk already of greater or equal size than requested",
			diskName:         diskName,
			oldSize:          *resource.NewQuantity(1*(1024*1024*1024), resource.BinarySI),
			newSize:          *resource.NewQuantity(2*(1024*1024*1024), resource.BinarySI),
			existedDisk:      &armcompute.Disk{Name: ptr.To(disk1Name), Properties: &armcompute.DiskProperties{DiskSizeGB: &diskSizeGB, DiskState: to.Ptr(armcompute.DiskStateUnattached)}},
			expectedQuantity: *resource.NewQuantity(2*(1024*1024*1024), resource.BinarySI),
			expectedErr:      false,
		},
		{
			desc:             "an error shall be returned if everything is good but get disk failed",
			diskName:         fakeGetDiskFailed,
			oldSize:          *resource.NewQuantity(2*(1024*1024*1024), resource.BinarySI),
			newSize:          *resource.NewQuantity(3*(1024*1024*1024), resource.BinarySI),
			existedDisk:      &armcompute.Disk{Name: ptr.To(fakeGetDiskFailed), Properties: &armcompute.DiskProperties{DiskSizeGB: &diskSizeGB, DiskState: to.Ptr(armcompute.DiskStateUnattached)}},
			expectedQuantity: *resource.NewQuantity(2*(1024*1024*1024), resource.BinarySI),
			expectedErr:      true,
			expectedErrMsg:   fmt.Errorf("Get Disk failed"),
		},
		{
			desc:             "an error shall be returned if everything is good but create disk failed",
			diskName:         fakeCreateDiskFailed,
			oldSize:          *resource.NewQuantity(2*(1024*1024*1024), resource.BinarySI),
			newSize:          *resource.NewQuantity(3*(1024*1024*1024), resource.BinarySI),
			existedDisk:      &armcompute.Disk{Name: ptr.To(fakeCreateDiskFailed), Properties: &armcompute.DiskProperties{DiskSizeGB: &diskSizeGB, DiskState: to.Ptr(armcompute.DiskStateUnattached)}},
			expectedQuantity: *resource.NewQuantity(2*(1024*1024*1024), resource.BinarySI),
			expectedErr:      true,
			expectedErrMsg:   fmt.Errorf("Create Disk failed"),
		},
		{
			desc:             "an error shall be returned if disk is not in Unattached state",
			diskName:         fakeCreateDiskFailed,
			oldSize:          *resource.NewQuantity(2*(1024*1024*1024), resource.BinarySI),
			newSize:          *resource.NewQuantity(3*(1024*1024*1024), resource.BinarySI),
			existedDisk:      &armcompute.Disk{Name: ptr.To(fakeCreateDiskFailed), Properties: &armcompute.DiskProperties{DiskSizeGB: &diskSizeGB, DiskState: to.Ptr(armcompute.DiskStateAttached)}},
			expectedQuantity: *resource.NewQuantity(2*(1024*1024*1024), resource.BinarySI),
			expectedErr:      true,
			expectedErrMsg:   fmt.Errorf("azureDisk - disk resize is only supported on Unattached disk, current disk state: Attached, already attached to "),
		},
	}

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)
		managedDiskController := &ManagedDiskController{
			controllerCommon: &controllerCommon{
				cloud:               testCloud,
				lockMap:             newLockMap(),
				DisableDiskLunCheck: true,
				clientFactory:       testCloud.ComputeClientFactory,
			},
		}
		diskURI := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s",
			testCloud.SubscriptionID, testCloud.ResourceGroup, *test.existedDisk.Name)

		mockDisksClient := mock_diskclient.NewMockInterface(ctrl)
		managedDiskController.controllerCommon.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(testCloud.SubscriptionID).Return(mockDisksClient, nil).AnyTimes()
		if test.diskName == fakeGetDiskFailed {
			mockDisksClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, test.diskName).Return(test.existedDisk, fmt.Errorf("Get Disk failed")).AnyTimes()
		} else {
			mockDisksClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, test.diskName).Return(test.existedDisk, nil).AnyTimes()
		}
		if test.diskName == fakeCreateDiskFailed {
			mockDisksClient.EXPECT().Patch(gomock.Any(), testCloud.ResourceGroup, test.diskName, gomock.Any()).Return(test.existedDisk, fmt.Errorf("Create Disk failed")).AnyTimes()
		} else {
			mockDisksClient.EXPECT().Patch(gomock.Any(), testCloud.ResourceGroup, test.diskName, gomock.Any()).Return(test.existedDisk, nil).AnyTimes()
		}

		result, err := managedDiskController.ResizeDisk(ctx, diskURI, test.oldSize, test.newSize, false)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s, return error: %v", i, test.desc, err)
		if test.expectedErr {
			assert.EqualError(t, test.expectedErrMsg, err.Error(), "TestCase[%d]: %s, expected: %v, return: %v", i, test.desc, test.expectedErrMsg, err)
		}
		assert.Equal(t, test.expectedQuantity.Value(), result.Value(), "TestCase[%d]: %s, expected Quantity: %v, return Quantity: %v", i, test.desc, test.expectedQuantity, result)
	}
}

func TestCreateManagedDiskAzureStackHub(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCloud := provider.GetTestCloud(ctrl)
	testCloud.Config.Cloud = "AZURESTACKCLOUD"

	common := &controllerCommon{
		cloud:                              testCloud,
		lockMap:                            newLockMap(),
		AttachDetachInitialDelayInMs:       defaultAttachDetachInitialDelayInMs,
		DetachOperationMinTimeoutInSeconds: defaultDetachOperationMinTimeoutInSeconds,
		clientFactory:                      testCloud.ComputeClientFactory,
	}

	managedDiskController := &ManagedDiskController{common}

	volumeOptions := &ManagedDiskOptions{
		DiskName:            disk1Name,
		StorageAccountType:  armcompute.DiskStorageAccountTypesStandardLRS,
		SizeGB:              1,
		NetworkAccessPolicy: "", // Empty - should remain nil for Azure Stack Hub
		PublicNetworkAccess: "", // Empty - should remain nil for Azure Stack Hub
	}

	mockDisksClient := mock_diskclient.NewMockInterface(ctrl)
	common.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(testCloud.SubscriptionID).Return(mockDisksClient, nil)

	mockDisksClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), disk1Name, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ string, disk armcompute.Disk) (*armcompute.Disk, error) {
			assert.Nil(t, disk.Properties.NetworkAccessPolicy, "NetworkAccessPolicy should be nil for Azure Stack Hub")
			assert.Nil(t, disk.Properties.PublicNetworkAccess, "PublicNetworkAccess should be nil for Azure Stack Hub")
			return &armcompute.Disk{
				ID:         ptr.To(disk1ID),
				Properties: &armcompute.DiskProperties{ProvisioningState: ptr.To("Succeeded")},
			}, nil
		})

	_, err := managedDiskController.CreateManagedDisk(ctx, volumeOptions)
	assert.NoError(t, err)
}

func TestModifyDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	diskName := disk1Name
	fakeCreateDiskFailed := "fakeCreateDiskFailed"
	storageAccountTypeUltraSSDLRS := armcompute.DiskStorageAccountTypesUltraSSDLRS
	storageAccountTypePremiumLRS := armcompute.DiskStorageAccountTypesPremiumLRS
	testCases := []struct {
		desc               string
		diskName           string
		diskIOPSReadWrite  string
		diskMBpsReadWrite  string
		storageAccountType armcompute.DiskStorageAccountTypes
		existedDisk        *armcompute.Disk
		expectedErr        bool
		expectedErrMsg     error
	}{
		{
			desc:               "new sku and no error shall be returned if everything is good",
			diskName:           diskName,
			storageAccountType: armcompute.DiskStorageAccountTypesStandardLRS,
			existedDisk:        &armcompute.Disk{Name: ptr.To(disk1Name), SKU: &armcompute.DiskSKU{Name: &storageAccountTypePremiumLRS}, Properties: &armcompute.DiskProperties{DiskIOPSReadWrite: ptr.To(int64(100))}},
			expectedErr:        false,
		},
		{
			desc:               "new diskIOPSReadWrite and no error shall be returned if everything is good",
			diskName:           diskName,
			diskIOPSReadWrite:  "200",
			storageAccountType: armcompute.DiskStorageAccountTypesUltraSSDLRS,
			existedDisk:        &armcompute.Disk{Name: ptr.To(disk1Name), SKU: &armcompute.DiskSKU{Name: &storageAccountTypeUltraSSDLRS}, Properties: &armcompute.DiskProperties{DiskIOPSReadWrite: ptr.To(int64(100))}},
			expectedErr:        false,
		},
		{
			desc:               "new diskMBpsReadWrite and no error shall be returned if everything is good",
			diskName:           diskName,
			diskMBpsReadWrite:  "200",
			storageAccountType: armcompute.DiskStorageAccountTypesUltraSSDLRS,
			existedDisk:        &armcompute.Disk{Name: ptr.To(disk1Name), SKU: &armcompute.DiskSKU{Name: &storageAccountTypeUltraSSDLRS}, Properties: &armcompute.DiskProperties{DiskMBpsReadWrite: ptr.To(int64(100))}},
			expectedErr:        false,
		},
		{
			desc:               "new diskIOPSReadWrite and diskMBpsReadWrite and no error shall be returned if everything is good",
			diskName:           diskName,
			diskIOPSReadWrite:  "200",
			diskMBpsReadWrite:  "200",
			storageAccountType: armcompute.DiskStorageAccountTypesUltraSSDLRS,
			existedDisk:        &armcompute.Disk{Name: ptr.To(disk1Name), SKU: &armcompute.DiskSKU{Name: &storageAccountTypeUltraSSDLRS}, Properties: &armcompute.DiskProperties{DiskIOPSReadWrite: ptr.To(int64(100)), DiskMBpsReadWrite: ptr.To(int64(100))}},
			expectedErr:        false,
		},
		{
			desc:               "nothing to modify and no error shall be returned if everything is good",
			diskName:           diskName,
			storageAccountType: armcompute.DiskStorageAccountTypesPremiumLRS,
			existedDisk:        &armcompute.Disk{Name: ptr.To(disk1Name), SKU: &armcompute.DiskSKU{Name: &storageAccountTypePremiumLRS}, Properties: &armcompute.DiskProperties{DiskIOPSReadWrite: ptr.To(int64(100))}},
			expectedErr:        false,
		},
		{
			desc:               "an error shall be returned when disk SKU is nil",
			diskName:           diskName,
			diskIOPSReadWrite:  "200",
			diskMBpsReadWrite:  "200",
			storageAccountType: armcompute.DiskStorageAccountTypesUltraSSDLRS,
			existedDisk:        &armcompute.Disk{Name: ptr.To(diskName), Properties: &armcompute.DiskProperties{DiskIOPSReadWrite: ptr.To(int64(100)), DiskMBpsReadWrite: ptr.To(int64(100))}},
			expectedErr:        true,
			expectedErrMsg:     fmt.Errorf("DiskProperties or SKU of disk(disk1) is nil"),
		},
		{
			desc:              "new diskIOPSReadWrite but wrong disk type error shall be returned",
			diskName:          diskName,
			diskIOPSReadWrite: "200",
			existedDisk:       &armcompute.Disk{Name: ptr.To(disk1Name), SKU: &armcompute.DiskSKU{Name: &storageAccountTypePremiumLRS}, Properties: &armcompute.DiskProperties{DiskIOPSReadWrite: ptr.To(int64(100))}},
			expectedErr:       true,
			expectedErrMsg:    fmt.Errorf("AzureDisk - DiskIOPSReadWrite parameter is only applicable in UltraSSD_LRS or PremiumV2_LRS disk type"),
		},
		{
			desc:              "new diskMBpsReadWrite but wrong disk type error shall be returned",
			diskName:          diskName,
			diskMBpsReadWrite: "200",
			existedDisk:       &armcompute.Disk{Name: ptr.To(disk1Name), SKU: &armcompute.DiskSKU{Name: &storageAccountTypePremiumLRS}, Properties: &armcompute.DiskProperties{DiskMBpsReadWrite: ptr.To(int64(100))}},
			expectedErr:       true,
			expectedErrMsg:    fmt.Errorf("AzureDisk - DiskMBpsReadWrite parameter is only applicable in UltraSSD_LRS or PremiumV2_LRS disk type"),
		},
		{
			desc:               "new diskIOPSReadWrite but failed to parse",
			diskName:           diskName,
			diskIOPSReadWrite:  "error",
			storageAccountType: armcompute.DiskStorageAccountTypesUltraSSDLRS,
			existedDisk:        &armcompute.Disk{Name: ptr.To(disk1Name), SKU: &armcompute.DiskSKU{Name: &storageAccountTypeUltraSSDLRS}, Properties: &armcompute.DiskProperties{DiskIOPSReadWrite: ptr.To(int64(100))}},
			expectedErr:        true,
			expectedErrMsg:     fmt.Errorf("AzureDisk - failed to parse DiskIOPSReadWrite: strconv.Atoi: parsing \"error\": invalid syntax"),
		},
		{
			desc:               "new diskMBpsReadWrite but failed to parse",
			diskName:           diskName,
			diskMBpsReadWrite:  "error",
			storageAccountType: armcompute.DiskStorageAccountTypesUltraSSDLRS,
			existedDisk:        &armcompute.Disk{Name: ptr.To(disk1Name), SKU: &armcompute.DiskSKU{Name: &storageAccountTypeUltraSSDLRS}, Properties: &armcompute.DiskProperties{DiskMBpsReadWrite: ptr.To(int64(100))}},
			expectedErr:        true,
			expectedErrMsg:     fmt.Errorf("AzureDisk - failed to parse DiskMBpsReadWrite: strconv.Atoi: parsing \"error\": invalid syntax"),
		},
		{
			desc:               "an error shall be returned if everything is good but get disk failed",
			diskName:           fakeGetDiskFailed,
			diskIOPSReadWrite:  "200",
			diskMBpsReadWrite:  "200",
			storageAccountType: armcompute.DiskStorageAccountTypesUltraSSDLRS,
			existedDisk:        &armcompute.Disk{Name: ptr.To(fakeGetDiskFailed), SKU: &armcompute.DiskSKU{Name: &storageAccountTypeUltraSSDLRS}, Properties: &armcompute.DiskProperties{DiskIOPSReadWrite: ptr.To(int64(100)), DiskMBpsReadWrite: ptr.To(int64(100))}},
			expectedErr:        true,
			expectedErrMsg:     fmt.Errorf("Get Disk failed"),
		},
		{
			desc:               "an error shall be returned if everything is good but patch disk failed",
			diskName:           fakeCreateDiskFailed,
			diskIOPSReadWrite:  "200",
			diskMBpsReadWrite:  "200",
			storageAccountType: armcompute.DiskStorageAccountTypesUltraSSDLRS,
			existedDisk:        &armcompute.Disk{Name: ptr.To(fakeCreateDiskFailed), SKU: &armcompute.DiskSKU{Name: &storageAccountTypeUltraSSDLRS}, Properties: &armcompute.DiskProperties{DiskIOPSReadWrite: ptr.To(int64(100)), DiskMBpsReadWrite: ptr.To(int64(100))}},
			expectedErr:        true,
			expectedErrMsg:     fmt.Errorf("Patch Disk failed"),
		},
	}

	for i, test := range testCases {
		testCloud := provider.GetTestCloud(ctrl)
		managedDiskController := &ManagedDiskController{
			controllerCommon: &controllerCommon{
				cloud:               testCloud,
				lockMap:             newLockMap(),
				DisableDiskLunCheck: true,
				clientFactory:       testCloud.ComputeClientFactory,
			},
		}
		diskURI := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s",
			testCloud.SubscriptionID, testCloud.ResourceGroup, *test.existedDisk.Name)
		diskOptions := &ManagedDiskOptions{
			DiskName:           test.diskName,
			DiskIOPSReadWrite:  test.diskIOPSReadWrite,
			DiskMBpsReadWrite:  test.diskMBpsReadWrite,
			StorageAccountType: test.storageAccountType,
			ResourceGroup:      testCloud.ResourceGroup,
			SubscriptionID:     testCloud.SubscriptionID,
			SourceResourceID:   diskURI,
		}

		mockDisksClient := mock_diskclient.NewMockInterface(ctrl)
		managedDiskController.controllerCommon.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(testCloud.SubscriptionID).Return(mockDisksClient, nil).AnyTimes()
		if test.diskName == fakeGetDiskFailed {
			mockDisksClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, test.diskName).Return(test.existedDisk, fmt.Errorf("Get Disk failed")).AnyTimes()
		} else {
			mockDisksClient.EXPECT().Get(gomock.Any(), testCloud.ResourceGroup, test.diskName).Return(test.existedDisk, nil).AnyTimes()
		}
		if test.diskName == fakeCreateDiskFailed {
			mockDisksClient.EXPECT().Patch(gomock.Any(), testCloud.ResourceGroup, test.diskName, gomock.Any()).Return(test.existedDisk, fmt.Errorf("Patch Disk failed")).AnyTimes()
		} else {
			mockDisksClient.EXPECT().Patch(gomock.Any(), testCloud.ResourceGroup, test.diskName, gomock.Any()).Return(test.existedDisk, nil).AnyTimes()
		}

		err := managedDiskController.ModifyDisk(ctx, diskOptions)
		assert.Equal(t, test.expectedErr, err != nil, "TestCase[%d]: %s, return error: %v", i, test.desc, err)
		if test.expectedErr {
			assert.EqualError(t, test.expectedErrMsg, err.Error(), "TestCase[%d]: %s, expected: %v, return: %v", i, test.desc, test.expectedErrMsg, err)
		}
	}
}
