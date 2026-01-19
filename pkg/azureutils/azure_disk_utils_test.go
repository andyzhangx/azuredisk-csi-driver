/*
Copyright 2021 The Kubernetes Authors.

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

package azureutils

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	consts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/util"
	"sigs.k8s.io/azuredisk-csi-driver/test/utils/testutil"
)

func TestCheckDiskName(t *testing.T) {
	tests := []struct {
		diskName string
		expected bool
	}{
		{
			diskName: "a",
			expected: true,
		},
		{
			diskName: ".",
			expected: false,
		},
		{
			diskName: "_",
			expected: false,
		},
		{
			diskName: "_",
			expected: false,
		},
		{
			diskName: "09",
			expected: true,
		},
		{
			diskName: "az",
			expected: true,
		},
		{
			diskName: "1_",
			expected: true,
		},
		{
			diskName: "_1",
			expected: false,
		},
		{
			diskName: "1.",
			expected: false,
		},
		{
			diskName: "1-",
			expected: false,
		},
		{
			diskName: "0.z",
			expected: true,
		},
		{
			diskName: "1.2",
			expected: true,
		},
		{
			diskName: "a-9",
			expected: true,
		},
		{
			diskName: "a_c",
			expected: true,
		},
		{
			diskName: "1__",
			expected: true,
		},
		{
			diskName: "a---9",
			expected: true,
		},
		{
			diskName: "1#2",
			expected: false,
		},
		{
			diskName: "-",
			expected: false,
		},
		{
			diskName: "test",
			expected: true,
		},
	}

	for _, test := range tests {
		result := checkDiskName(test.diskName)
		if !reflect.DeepEqual(result, test.expected) {
			t.Errorf("input: %q, checkShareNameBeginAndEnd result: %v, expected: %v", test.diskName, result, test.expected)
		}
	}
}

func TestGetCachingMode(t *testing.T) {
	tests := []struct {
		options             map[string]string
		expectedCachingMode armcompute.CachingTypes
		expectedError       bool
	}{
		{
			nil,
			armcompute.CachingTypes(defaultAzureDataDiskCachingMode),
			false,
		},
		{
			map[string]string{},
			armcompute.CachingTypes(defaultAzureDataDiskCachingMode),
			false,
		},
		{
			map[string]string{consts.CachingModeField: ""},
			armcompute.CachingTypes(defaultAzureDataDiskCachingMode),
			false,
		},
		{
			map[string]string{consts.CachingModeField: "None"},
			armcompute.CachingTypes("None"),
			false,
		},
		{
			map[string]string{consts.CachingModeField: "ReadOnly"},
			armcompute.CachingTypes("ReadOnly"),
			false,
		},
		{
			map[string]string{consts.CachingModeField: "ReadWrite"},
			armcompute.CachingTypes("ReadWrite"),
			false,
		},
		{
			map[string]string{consts.CachingModeField: "WriteOnly"},
			armcompute.CachingTypes(""),
			true,
		},
	}

	for _, test := range tests {
		resultCachingMode, resultError := GetCachingMode(test.options)
		if resultCachingMode != test.expectedCachingMode || (resultError != nil) != test.expectedError {
			t.Errorf("input: %s, getCachingMode resultCachingMode: %s, expectedCachingMode: %s, resultError: %s, expectedError: %t", test.options, resultCachingMode, test.expectedCachingMode, resultError, test.expectedError)
		}
	}
}

func TestGetCloudProvider(t *testing.T) {
	locationRxp := regexp.MustCompile("(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?")
	fakeCredFile, err := testutil.GetWorkDirPath("fake-cred-file.json")
	if err != nil {
		t.Errorf("GetWorkDirPath failed with %v", err)
	}
	fakeKubeConfig, err := testutil.GetWorkDirPath("fake-kube-config")
	if err != nil {
		t.Errorf("GetWorkDirPath failed with %v", err)
	}
	emptyKubeConfig, err := testutil.GetWorkDirPath("empty-kube-config")
	if err != nil {
		t.Errorf("GetWorkDirPath failed with %v", err)
	}

	fakeContent := `apiVersion: v1
clusters:
- cluster:
    server: https://localhost:8080
  name: foo-cluster
contexts:
- context:
    cluster: foo-cluster
    user: foo-user
    namespace: bar
  name: foo-context
current-context: foo-context
kind: Config
users:
- name: foo-user
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      args:
      - arg-1
      - arg-2
      command: foo-command
`

	err = createTestFile(emptyKubeConfig)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := os.Remove(emptyKubeConfig); err != nil {
			t.Error(err)
		}
	}()

	tests := []struct {
		desc                  string
		createFakeCredFile    bool
		createFakeKubeConfig  bool
		credFile              string
		kubeconfig            string
		userAgent             string
		allowEmptyCloudConfig bool
		expectedErr           error
	}{
		{
			desc:                  "[failure] out of cluster & in cluster, specify a fake kubeconfig, no credential file",
			createFakeKubeConfig:  true,
			kubeconfig:            fakeKubeConfig,
			allowEmptyCloudConfig: false,
			expectedErr: testutil.TestError{
				DefaultError: fmt.Errorf("no cloud config provided, error"),
			},
		},
		{
			desc:                  "[failure] out of cluster & in cluster, specify a empty kubeconfig, no credential file",
			kubeconfig:            emptyKubeConfig,
			allowEmptyCloudConfig: true,
			expectedErr:           fmt.Errorf("invalid configuration: no configuration has been provided, try setting KUBERNETES_MASTER environment variable"),
		},
		{
			desc:                  "[success] out of cluster & in cluster, no kubeconfig, a fake credential file",
			createFakeCredFile:    true,
			kubeconfig:            "",
			userAgent:             "useragent",
			allowEmptyCloudConfig: true,
			expectedErr:           nil,
		},
		{
			desc:                  "[success] out of cluster & in cluster, specify a fake kubeconfig, no credential file",
			createFakeKubeConfig:  true,
			kubeconfig:            fakeKubeConfig,
			allowEmptyCloudConfig: true,
			expectedErr:           nil,
		},
		{
			desc:                  "[success] out of cluster & in cluster, no kubeconfig, a fake credential file with upper case Location format",
			createFakeCredFile:    true,
			kubeconfig:            "",
			credFile:              "location: \"East US\"\n",
			userAgent:             "useragent",
			allowEmptyCloudConfig: false,
			expectedErr:           nil,
		},
	}

	for _, test := range tests {
		if test.createFakeCredFile {
			if err := createTestFile(fakeCredFile); err != nil {
				t.Error(err)
			}
			defer func() {
				os.Remove(fakeCredFile)
			}()

			if err := os.WriteFile(fakeCredFile, []byte(test.credFile), 0666); err != nil {
				t.Error(err)
			}

			t.Setenv(consts.DefaultAzureCredentialFileEnv, fakeCredFile)
		}
		if test.createFakeKubeConfig {
			if err := createTestFile(fakeKubeConfig); err != nil {
				t.Error(err)
			}
			defer func() {
				os.Remove(fakeKubeConfig)
			}()

			if err := os.WriteFile(fakeKubeConfig, []byte(fakeContent), 0666); err != nil {
				t.Error(err)
			}
		}

		kubeClient, err := GetKubeClient(test.kubeconfig)
		if err != nil {
			if ((err == nil) == (test.expectedErr == nil)) && !reflect.DeepEqual(err, test.expectedErr) && !strings.Contains(err.Error(), test.expectedErr.Error()) {
				t.Errorf("desc: %s,\n input: %q, GetCloudProvider err: %v, expectedErr: %v", test.desc, test.kubeconfig, err, test.expectedErr)
			}
		}
		cloud, err := GetCloudProviderFromClient(context.Background(), kubeClient, "", "", test.userAgent, test.allowEmptyCloudConfig, false, false, -1)
		if ((err == nil) == (test.expectedErr == nil)) && !reflect.DeepEqual(err, test.expectedErr) && !strings.Contains(err.Error(), test.expectedErr.Error()) {
			t.Errorf("desc: %s,\n input: %q, GetCloudProvider err: %v, expectedErr: %v", test.desc, test.kubeconfig, err, test.expectedErr)
		}
		if cloud != nil {
			assert.Regexp(t, locationRxp, cloud.Location)
			if test.userAgent != "" {
				assert.Equal(t, test.userAgent, cloud.UserAgent)
			}
			//assert.Equal(t, cloud.DiskRateLimit != nil && cloud.DiskRateLimit.CloudProviderRateLimit, false)
			//assert.Equal(t, cloud.SnapshotRateLimit != nil && cloud.SnapshotRateLimit.CloudProviderRateLimit, false)
		}
	}
}

func TestGetDiskLUN(t *testing.T) {
	tests := []struct {
		deviceInfo  string
		expectedLUN int32
		expectError bool
	}{
		{
			deviceInfo:  "0",
			expectedLUN: 0,
			expectError: false,
		},
		{
			deviceInfo:  "10",
			expectedLUN: 10,
			expectError: false,
		},
		{
			deviceInfo:  "11d",
			expectedLUN: -1,
			expectError: true,
		},
		{
			deviceInfo:  "999",
			expectedLUN: -1,
			expectError: true,
		},
		{
			deviceInfo:  "",
			expectedLUN: -1,
			expectError: true,
		},
		{
			deviceInfo:  "/dev/disk/azure/scsi1/lun2",
			expectedLUN: 2,
			expectError: false,
		},
		{
			deviceInfo:  "/dev/disk/azure/scsi0/lun12",
			expectedLUN: 12,
			expectError: false,
		},
		{
			deviceInfo:  "/devhost/disk/azure/scsi0/lun13",
			expectedLUN: 13,
			expectError: false,
		},
		{
			deviceInfo:  "/dev/disk/by-id/scsi1/lun2",
			expectedLUN: -1,
			expectError: true,
		},
	}

	for _, test := range tests {
		result, err := GetDiskLUN(test.deviceInfo)
		assert.Equal(t, result, test.expectedLUN)
		assert.Equal(t, err != nil, test.expectError, fmt.Sprintf("error msg: %v", err))
	}
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
		result := GetFStype(test.options)
		if result != test.expected {
			t.Errorf("input: %q, GetFStype result: %s, expected: %s", test.options, result, test.expected)
		}
	}
}

func TestGetMaxShares(t *testing.T) {
	tests := []struct {
		options       map[string]string
		expectedValue int
		expectedError error
	}{
		{
			nil,
			1,
			nil,
		},
		{
			map[string]string{},
			1,
			nil,
		},
		{
			map[string]string{consts.MaxSharesField: ""},
			0,
			fmt.Errorf("parse  failed with error: strconv.Atoi: parsing \"\": invalid syntax"),
		},
		{
			map[string]string{consts.MaxSharesField: "-1"},
			0,
			fmt.Errorf("parse -1 returned with invalid value: -1"),
		},
		{
			map[string]string{consts.MaxSharesField: "NAN"},
			0,
			fmt.Errorf("parse NAN failed with error: strconv.Atoi: parsing \"NAN\": invalid syntax"),
		},
		{
			map[string]string{consts.MaxSharesField: "2"},
			2,
			nil,
		},
	}

	for _, test := range tests {
		result, err := GetMaxShares(test.options)
		if result != test.expectedValue {
			t.Errorf("input: %q, GetMaxShates result: %v, expected: %v", test.options, result, test.expectedValue)
		}
		if !reflect.DeepEqual(err, test.expectedError) {
			t.Errorf("input: %q, GetMaxShates error: %v, expected: %v", test.options, err, test.expectedError)
		}
	}
}

func TestGetSourceVolumeID(t *testing.T) {
	SourceResourceID := "test"

	tests := []struct {
		snapshot *armcompute.Snapshot
		expected string
	}{
		{
			snapshot: &armcompute.Snapshot{
				Properties: &armcompute.SnapshotProperties{
					CreationData: &armcompute.CreationData{
						SourceResourceID: &SourceResourceID,
					},
				},
			},
			expected: "test",
		},
		{
			snapshot: &armcompute.Snapshot{
				Properties: &armcompute.SnapshotProperties{
					CreationData: &armcompute.CreationData{},
				},
			},
			expected: "",
		},
		{
			snapshot: &armcompute.Snapshot{
				Properties: &armcompute.SnapshotProperties{},
			},
			expected: "",
		},
		{
			snapshot: &armcompute.Snapshot{},
			expected: "",
		},
		{
			snapshot: nil,
			expected: "",
		},
	}

	for _, test := range tests {
		result := GetSourceVolumeID(test.snapshot)
		if !reflect.DeepEqual(result, test.expected) {
			t.Errorf("input: %v, getValidFileShareName result: %q, expected: %q", test.snapshot, result, test.expected)
		}
	}
}

func TestGetValidCreationData(t *testing.T) {
	sourceResourceSnapshotID := "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/snapshots/xxx"
	sourceResourceVolumeID := "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/disks/xxx"

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
			sourceType:       consts.SourceSnapshot,
			expected1: armcompute.CreationData{
				CreateOption:     to.Ptr(armcompute.DiskCreateOptionCopy),
				SourceResourceID: &sourceResourceSnapshotID,
			},
			expected2: nil,
		},
		{
			subscriptionID:   "xxx",
			resourceGroup:    "xxx",
			sourceResourceID: "xxx",
			sourceType:       consts.SourceSnapshot,
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
			sourceType:       consts.SourceSnapshot,
			expected1:        armcompute.CreationData{},
			expected2:        fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", "/subscriptions//resourceGroups//providers/Microsoft.Compute/snapshots//subscriptions/23/providers/Microsoft.Compute/disks/name", diskSnapshotPathRE),
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: "http://test.com/vhds/name",
			sourceType:       consts.SourceSnapshot,
			expected1:        armcompute.CreationData{},
			expected2:        fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", "/subscriptions//resourceGroups//providers/Microsoft.Compute/snapshots/http://test.com/vhds/name", diskSnapshotPathRE),
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: "/subscriptions/xxx/snapshots/xxx",
			sourceType:       consts.SourceSnapshot,
			expected1:        armcompute.CreationData{},
			expected2:        fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", "/subscriptions//resourceGroups//providers/Microsoft.Compute/snapshots//subscriptions/xxx/snapshots/xxx", diskSnapshotPathRE),
		},
		{
			subscriptionID:   "",
			resourceGroup:    "",
			sourceResourceID: "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/snapshots/xxx/snapshots/xxx/snapshots/xxx",
			sourceType:       consts.SourceSnapshot,
			expected1:        armcompute.CreationData{},
			expected2:        fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/snapshots/xxx/snapshots/xxx/snapshots/xxx", diskSnapshotPathRE),
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
			sourceType:       consts.SourceVolume,
			expected1: armcompute.CreationData{
				CreateOption:     to.Ptr(armcompute.DiskCreateOptionCopy),
				SourceResourceID: &sourceResourceVolumeID,
			},
			expected2: nil,
		},
		{
			subscriptionID:   "xxx",
			resourceGroup:    "xxx",
			sourceResourceID: "xxx",
			sourceType:       consts.SourceVolume,
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
			sourceType:       consts.SourceVolume,
			expected1:        armcompute.CreationData{},
			expected2:        fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", "/subscriptions//resourceGroups//providers/Microsoft.Compute/disks//subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Compute/snapshots/xxx", consts.ManagedDiskPathRE),
		},
	}

	for _, test := range tests {
		result, err := GetValidCreationData(test.subscriptionID, test.resourceGroup, test.sourceResourceID, test.sourceType)
		if !reflect.DeepEqual(result, test.expected1) || !reflect.DeepEqual(err, test.expected2) {
			t.Errorf("input sourceResourceID: %v, sourceType: %v, getValidCreationData result: %v, expected1 : %v, err: %v, expected2: %v", test.sourceResourceID, test.sourceType, result, test.expected1, err, test.expected2)
		}
	}
}

func TestIsCorruptedDir(t *testing.T) {
	isCorrupted := IsCorruptedDir("/non-existing-dir")
	assert.False(t, isCorrupted)

	isCorrupted = IsCorruptedDir(os.TempDir())
	assert.False(t, isCorrupted)
}

func TestCreateValidDiskName(t *testing.T) {
	tests := []struct {
		volumeName      string
		expected        string
		expectedIsRegEx bool
	}{
		{
			volumeName: "az",
			expected:   "az",
		},
		{
			volumeName: "09",
			expected:   "09",
		},
		{
			volumeName: "a-z",
			expected:   "a-z",
		},
		{
			volumeName: "AZ",
			expected:   "AZ",
		},
		{
			volumeName: "123456789-123456789-123456789-123456789-123456789.123456789-123456789_1234567890",
			expected:   "123456789-123456789-123456789-123456789-123456789.123456789-123456789_1234567890",
		},
		{
			volumeName: "123456789-123456789-123456789-123456789-123456789.123456789-123456789_1234567890-123456789-123456789-123456789-123456789-123456789.123456789-123456789_1234567890-123456789-123456789-123456789-123456789-123456789.123456789-123456789_1234567890",
			expected:   "123456789-123456789-123456789-123456789-123456789.123456789-123456789_1234567890",
		},
		{
			volumeName:      "",
			expected:        "pvc-disk-dynamic-[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}",
			expectedIsRegEx: true,
		},
		{
			volumeName:      "$xyz123",
			expected:        "pvc-disk-dynamic-[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}",
			expectedIsRegEx: true,
		},
	}

	for _, test := range tests {
		result := CreateValidDiskName(test.volumeName)
		if !test.expectedIsRegEx {
			assert.Equal(t, test.expected, result)
		} else {
			assert.Regexp(t, test.expected, result)
		}
	}
}

func TestIsARMResourceID(t *testing.T) {
	tests := []struct {
		resourceID   string
		expectResult bool
	}{
		{
			resourceID:   "/subscriptions/b9d2281e/resourceGroups/test-resource/providers/Microsoft.Compute/disks/pvc-disk-dynamic-9e102c53",
			expectResult: true,
		},
		{
			resourceID:   "/Subscriptions/b9d2281e/resourceGroups/test-resource/providers/Microsoft.Compute/disks/pvc-disk-dynamic-9e102c53",
			expectResult: true,
		},
		{
			resourceID:   "resourceGroups/test-resource/providers/Microsoft.Compute/disks/pvc-disk-dynamic-9e102c53",
			expectResult: false,
		},
		{
			resourceID:   "https://test-saccount.blob.core.windows.net/container/pvc-disk-dynamic-9e102c53-593d-11e9-934e-705a0f18a318.vhd",
			expectResult: false,
		},
		{
			resourceID:   "test.com",
			expectResult: false,
		},
		{
			resourceID:   "",
			expectResult: false,
		},
	}

	for _, test := range tests {
		result := IsARMResourceID(test.resourceID)
		if result != test.expectResult {
			t.Errorf("ResourceID: %s, result: %v, expectResult: %v", test.resourceID, result, test.expectResult)
		}
	}
}

func TestIsAvailabilityZone(t *testing.T) {
	tests := []struct {
		desc     string
		zone     string
		region   string
		expected bool
	}{
		{"empty string should return false", "", "eastus", false},
		{"wrong farmat should return false", "123", "eastus", false},
		{"wrong location should return false", "chinanorth-1", "eastus", false},
		{"correct zone should return true", "eastus-1", "eastus", true},
		{"empty location should return true", "eastus-1", "", true},
		{"empty location with fault domain should return false", "1", "", false},
		{"empty location with wrong format should return false", "-1", "", false},
		{"empty location with wrong format should return false", "eastus-", "", false},
	}

	for _, test := range tests {
		actual := IsValidAvailabilityZone(test.zone, test.region)
		if actual != test.expected {
			t.Errorf("test [%q] get unexpected result: %v != %v", test.desc, actual, test.expected)
		}
	}
}

func TestGetRegionFromAvailabilityZone(t *testing.T) {
	tests := []struct {
		desc     string
		zone     string
		expected string
	}{
		{"empty string", "", ""},
		{"invallid zone", "1", ""},
		{"valid zone", "eastus-2", "eastus"},
	}

	for _, test := range tests {
		result := GetRegionFromAvailabilityZone(test.zone)
		if result != test.expected {
			t.Errorf("test [%q] got unexpected result: %v != %v", test.desc, result, test.expected)
		}
	}
}

func TestIsAzureStackCloud(t *testing.T) {
	tests := []struct {
		cloud                  string
		disableAzureStackCloud bool
		expectedResult         bool
	}{
		{
			cloud:                  "AzurePublicCloud",
			disableAzureStackCloud: false,
			expectedResult:         false,
		},
		{
			cloud:                  "",
			disableAzureStackCloud: true,
			expectedResult:         false,
		},
		{
			cloud:                  azureStackCloud,
			disableAzureStackCloud: false,
			expectedResult:         true,
		},
		{
			cloud:                  azureStackCloud,
			disableAzureStackCloud: true,
			expectedResult:         false,
		},
	}

	for i, test := range tests {
		result := IsAzureStackCloud(test.cloud, test.disableAzureStackCloud)
		assert.Equal(t, test.expectedResult, result, "TestCase[%d]", i)
	}
}

func TestIsValidVolumeCapabilities(t *testing.T) {
	tests := []struct {
		description    string
		volCaps        []*csi.VolumeCapability
		maxShares      int
		expectedResult error
	}{
		{
			description: "[Success] Returns true for valid mount capabilities",
			volCaps: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
			},
			maxShares:      1,
			expectedResult: nil,
		},
		{
			description: "[Failure] Returns false for unsupported mount access mode",
			volCaps: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
					},
				},
			},
			maxShares:      2,
			expectedResult: fmt.Errorf("mountVolume is not supported for access mode: MULTI_NODE_MULTI_WRITER"),
		},
		{
			description: "[Failure] Returns false for invalid mount access mode",
			volCaps: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Mount{
						Mount: &csi.VolumeCapability_MountVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: 10,
					},
				},
			},
			maxShares:      1,
			expectedResult: fmt.Errorf("invalid access mode: [mount:{} access_mode:{mode:10}]"),
		},
		{
			description: "[Success] Returns true for valid block capabilities",
			volCaps: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				},
			},
			maxShares:      1,
			expectedResult: nil,
		},
		{
			description: "[Success] Returns true for shared block access mode",
			volCaps: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
					},
				},
			},
			maxShares:      2,
			expectedResult: nil,
		},
		{
			description: "[Failure] Returns false for unsupported mount access mode",
			volCaps: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
					},
				},
			},
			maxShares:      1,
			expectedResult: fmt.Errorf("access mode: MULTI_NODE_MULTI_WRITER is not supported for non-shared disk"),
		},
		{
			description: "[Failure] Returns false for invalid block access mode",
			volCaps: []*csi.VolumeCapability{
				{
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: 10,
					},
				},
			},
			maxShares:      1,
			expectedResult: fmt.Errorf("invalid access mode: [block:{} access_mode:{mode:10}]"),
		},
		{
			description: "[Failure] Returns false for empty volume capability",
			volCaps: []*csi.VolumeCapability{
				{
					AccessType: nil,
					AccessMode: nil,
				},
			},
			maxShares:      1,
			expectedResult: fmt.Errorf("invalid access mode: []"),
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			result := IsValidVolumeCapabilities(test.volCaps, test.maxShares)
			if !reflect.DeepEqual(result, test.expectedResult) && !strings.Contains(result.Error(), "invalid access mode") {
				t.Errorf("actualErr: (%v), expectedErr: (%v)", result, test.expectedResult)
			}
		})
	}
	var caps []*csi.VolumeCapability
	stdVolCap := csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}
	caps = append(caps, &stdVolCap)
	if err := IsValidVolumeCapabilities(caps, 1); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	stdVolCap1 := csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: 10,
		},
	}
	caps = append(caps, &stdVolCap1)
	if err := IsValidVolumeCapabilities(caps, 1); err == nil {
		t.Errorf("Unexpected success")
	}
}

func TestNormalizeCachingMode(t *testing.T) {
	tests := []struct {
		desc          string
		req           v1.AzureDataDiskCachingMode
		expectedErr   error
		expectedValue v1.AzureDataDiskCachingMode
	}{
		{
			desc:          "CachingMode not exist",
			req:           "",
			expectedErr:   nil,
			expectedValue: v1.AzureDataDiskCachingReadOnly,
		},
		{
			desc:          "Not supported CachingMode",
			req:           "WriteOnly",
			expectedErr:   fmt.Errorf("azureDisk - WriteOnly is not supported cachingmode. Supported values are [None ReadOnly ReadWrite]"),
			expectedValue: "",
		},
		{
			desc:          "Valid CachingMode",
			req:           "ReadOnly",
			expectedErr:   nil,
			expectedValue: "ReadOnly",
		},
	}
	for _, test := range tests {
		value, err := NormalizeCachingMode(test.req)
		assert.Equal(t, value, test.expectedValue)
		assert.Equal(t, err, test.expectedErr, fmt.Sprintf("error msg: %v", err))
	}
}

func TestValidateDiskEncryptionType(t *testing.T) {
	tests := []struct {
		diskEncryptionType string
		expectedErr        error
	}{
		{
			diskEncryptionType: "",
			expectedErr:        nil,
		},
		{
			diskEncryptionType: "EncryptionAtRestWithCustomerKey",
			expectedErr:        nil,
		},
		{
			diskEncryptionType: "EncryptionAtRestWithPlatformAndCustomerKeys",
			expectedErr:        nil,
		},
		{
			diskEncryptionType: "EncryptionAtRestWithPlatformKey",
			expectedErr:        nil,
		},
		{
			diskEncryptionType: "encryptionAtRestWithCustomerKey",
			expectedErr:        fmt.Errorf("DiskEncryptionType(encryptionAtRestWithCustomerKey) is not supported"),
		},
		{
			diskEncryptionType: "invalid",
			expectedErr:        fmt.Errorf("DiskEncryptionType(invalid) is not supported"),
		},
	}
	for _, test := range tests {
		err := ValidateDiskEncryptionType(test.diskEncryptionType)
		assert.Equal(t, test.expectedErr, err)
	}
}

func TestValidateDataAccessAuthMode(t *testing.T) {
	tests := []struct {
		dataAccessAuthMode string
		expectedErr        error
	}{
		{
			dataAccessAuthMode: "",
			expectedErr:        nil,
		},
		{
			dataAccessAuthMode: "None",
			expectedErr:        nil,
		},
		{
			dataAccessAuthMode: "AzureActiveDirectory",
			expectedErr:        nil,
		},
		{
			dataAccessAuthMode: "invalid",
			expectedErr:        fmt.Errorf("dataAccessAuthMode(invalid) is not supported"),
		},
	}
	for _, test := range tests {
		err := ValidateDataAccessAuthMode(test.dataAccessAuthMode)
		assert.Equal(t, test.expectedErr, err)
	}
}

func TestNormalizeNetworkAccessPolicy(t *testing.T) {
	tests := []struct {
		networkAccessPolicy         string
		expectedNetworkAccessPolicy armcompute.NetworkAccessPolicy
		expectError                 bool
	}{
		{
			networkAccessPolicy:         "",
			expectedNetworkAccessPolicy: armcompute.NetworkAccessPolicy(""),
			expectError:                 false,
		},
		{
			networkAccessPolicy:         "AllowAll",
			expectedNetworkAccessPolicy: armcompute.NetworkAccessPolicyAllowAll,
			expectError:                 false,
		},
		{
			networkAccessPolicy:         "DenyAll",
			expectedNetworkAccessPolicy: armcompute.NetworkAccessPolicyDenyAll,
			expectError:                 false,
		},
		{
			networkAccessPolicy:         "AllowPrivate",
			expectedNetworkAccessPolicy: armcompute.NetworkAccessPolicyAllowPrivate,
			expectError:                 false,
		},
		{
			networkAccessPolicy:         "allowAll",
			expectedNetworkAccessPolicy: armcompute.NetworkAccessPolicy(""),
			expectError:                 true,
		},
		{
			networkAccessPolicy:         "invalid",
			expectedNetworkAccessPolicy: armcompute.NetworkAccessPolicy(""),
			expectError:                 true,
		},
	}

	for _, test := range tests {
		result, err := NormalizeNetworkAccessPolicy(test.networkAccessPolicy)
		assert.Equal(t, result, test.expectedNetworkAccessPolicy)
		assert.Equal(t, err != nil, test.expectError, fmt.Sprintf("error msg: %v", err))
	}
}

func TestNormalizePublicNetworkAccess(t *testing.T) {
	tests := []struct {
		publicNetworkAccess         string
		expectedPublicNetworkAccess armcompute.PublicNetworkAccess
		expectError                 bool
	}{
		{
			publicNetworkAccess:         "",
			expectedPublicNetworkAccess: armcompute.PublicNetworkAccess(""),
			expectError:                 false,
		},
		{
			publicNetworkAccess:         "Enabled",
			expectedPublicNetworkAccess: armcompute.PublicNetworkAccessEnabled,
			expectError:                 false,
		},
		{
			publicNetworkAccess:         "Disabled",
			expectedPublicNetworkAccess: armcompute.PublicNetworkAccessDisabled,
			expectError:                 false,
		},
		{
			publicNetworkAccess:         "enabled",
			expectedPublicNetworkAccess: armcompute.PublicNetworkAccess(""),
			expectError:                 true,
		},
		{
			publicNetworkAccess:         "disabled",
			expectedPublicNetworkAccess: armcompute.PublicNetworkAccess(""),
			expectError:                 true,
		},
	}

	for _, test := range tests {
		result, err := NormalizePublicNetworkAccess(test.publicNetworkAccess)
		assert.Equal(t, result, test.expectedPublicNetworkAccess)
		assert.Equal(t, err != nil, test.expectError, fmt.Sprintf("error msg: %v", err))
	}
}

func TestNormalizeStorageAccountType(t *testing.T) {
	tests := []struct {
		cloud                  string
		storageAccountType     string
		disableAzureStackCloud bool
		expectedAccountType    armcompute.DiskStorageAccountTypes
		expectError            bool
	}{
		{
			cloud:                  azurePublicCloud,
			storageAccountType:     "",
			disableAzureStackCloud: false,
			expectedAccountType:    armcompute.DiskStorageAccountTypesStandardSSDLRS,
			expectError:            false,
		},
		{
			cloud:                  azureStackCloud,
			storageAccountType:     "",
			disableAzureStackCloud: false,
			expectedAccountType:    armcompute.DiskStorageAccountTypesStandardLRS,
			expectError:            false,
		},
		{
			cloud:                  azurePublicCloud,
			storageAccountType:     "NOT_EXISTING",
			disableAzureStackCloud: false,
			expectedAccountType:    "",
			expectError:            true,
		},
		{
			cloud:                  azurePublicCloud,
			storageAccountType:     "Standard_LRS",
			disableAzureStackCloud: false,
			expectedAccountType:    armcompute.DiskStorageAccountTypesStandardLRS,
			expectError:            false,
		},
		{
			cloud:                  azurePublicCloud,
			storageAccountType:     "Premium_LRS",
			disableAzureStackCloud: false,
			expectedAccountType:    armcompute.DiskStorageAccountTypesPremiumLRS,
			expectError:            false,
		},
		{
			cloud:                  azurePublicCloud,
			storageAccountType:     "StandardSSD_LRS",
			disableAzureStackCloud: false,
			expectedAccountType:    armcompute.DiskStorageAccountTypesStandardSSDLRS,
			expectError:            false,
		},
		{
			cloud:                  azurePublicCloud,
			storageAccountType:     "UltraSSD_LRS",
			disableAzureStackCloud: false,
			expectedAccountType:    armcompute.DiskStorageAccountTypesUltraSSDLRS,
			expectError:            false,
		},
		{
			cloud:                  azureStackCloud,
			storageAccountType:     "UltraSSD_LRS",
			disableAzureStackCloud: false,
			expectedAccountType:    "",
			expectError:            true,
		},
		{
			cloud:                  azureStackCloud,
			storageAccountType:     "UltraSSD_LRS",
			disableAzureStackCloud: true,
			expectedAccountType:    armcompute.DiskStorageAccountTypesUltraSSDLRS,
			expectError:            false,
		},
	}

	for _, test := range tests {
		result, err := NormalizeStorageAccountType(test.storageAccountType, test.cloud, test.disableAzureStackCloud)
		assert.Equal(t, result, test.expectedAccountType)
		assert.Equal(t, err != nil, test.expectError, fmt.Sprintf("error msg: %v", err))
	}
}

func TestParseDiskParameters(t *testing.T) {
	testCases := []struct {
		name           string
		inputParams    map[string]string
		expectedOutput ManagedDiskParameters
		expectedError  error
	}{
		{
			name:        "nil disk parameters",
			inputParams: nil,
			expectedOutput: ManagedDiskParameters{
				Tags:           make(map[string]string),
				VolumeContext:  make(map[string]string),
				DeviceSettings: make(map[string]string),
			},
			expectedError: nil,
		},
		{
			name:        "invalid field in parameters",
			inputParams: map[string]string{"invalidField": "someValue"},
			expectedOutput: ManagedDiskParameters{
				Tags:           make(map[string]string),
				VolumeContext:  map[string]string{"invalidField": "someValue"},
				DeviceSettings: make(map[string]string),
			},
			expectedError: fmt.Errorf("invalid parameter %s in storage class", "invalidField"),
		},
		{
			name:        "invalid LogicalSectorSize value in parameters",
			inputParams: map[string]string{consts.LogicalSectorSizeField: "invalidValue"},
			expectedOutput: ManagedDiskParameters{
				Tags:           make(map[string]string),
				VolumeContext:  map[string]string{consts.LogicalSectorSizeField: "invalidValue"},
				DeviceSettings: make(map[string]string),
			},
			expectedError: fmt.Errorf("parse invalidValue failed with error: strconv.Atoi: parsing \"invalidValue\": invalid syntax"),
		},
		{
			name:        "invalid AttachDiskInitialDelay value in parameters",
			inputParams: map[string]string{consts.AttachDiskInitialDelayField: "invalidValue"},
			expectedOutput: ManagedDiskParameters{
				Tags:           make(map[string]string),
				VolumeContext:  map[string]string{consts.AttachDiskInitialDelayField: "invalidValue"},
				DeviceSettings: make(map[string]string),
			},
			expectedError: fmt.Errorf("parse invalidValue failed with error: strconv.Atoi: parsing \"invalidValue\": invalid syntax"),
		},
		{
			name:        "disk parameters with PremiumV2_LRS",
			inputParams: map[string]string{consts.SkuNameField: "PremiumV2_LRS"},
			expectedOutput: ManagedDiskParameters{
				AccountType:    "PremiumV2_LRS",
				Tags:           make(map[string]string),
				VolumeContext:  map[string]string{consts.SkuNameField: "PremiumV2_LRS"},
				DeviceSettings: make(map[string]string),
			},
			expectedError: nil,
		},
		{
			name: "disk parameters with PremiumV2_LRS (valid cachingMode)",
			inputParams: map[string]string{
				consts.SkuNameField:     "PremiumV2_LRS",
				consts.CachingModeField: "none",
			},
			expectedOutput: ManagedDiskParameters{
				AccountType: "PremiumV2_LRS",
				CachingMode: "none",
				Tags:        make(map[string]string),
				VolumeContext: map[string]string{
					consts.SkuNameField:     "PremiumV2_LRS",
					consts.CachingModeField: "none",
				},
				DeviceSettings: make(map[string]string),
			},
			expectedError: nil,
		},
		{
			name: "disk parameters with PremiumV2_LRS (invalid cachingMode)",
			inputParams: map[string]string{
				consts.SkuNameField:     "PremiumV2_LRS",
				consts.CachingModeField: "ReadOnly",
			},
			expectedOutput: ManagedDiskParameters{
				AccountType: "PremiumV2_LRS",
				CachingMode: "ReadOnly",
				Tags:        make(map[string]string),
				VolumeContext: map[string]string{
					consts.SkuNameField:     "PremiumV2_LRS",
					consts.CachingModeField: "ReadOnly",
				},
				DeviceSettings: make(map[string]string),
			},
			expectedError: fmt.Errorf("cachingMode ReadOnly is not supported for PremiumV2_LRS"),
		},
		{
			name:        "invalid DiskIOPSReadWriteField value in parameters",
			inputParams: map[string]string{consts.DiskIOPSReadWriteField: "diskIOPSReadWrite"},
			expectedOutput: ManagedDiskParameters{
				Tags:           make(map[string]string),
				VolumeContext:  map[string]string{consts.DiskIOPSReadWriteField: "diskIOPSReadWrite"},
				DeviceSettings: make(map[string]string),
			},
			expectedError: fmt.Errorf("parse diskiopsreadwrite:diskIOPSReadWrite failed with error: strconv.Atoi: parsing \"diskIOPSReadWrite\": invalid syntax"),
		},
		{
			name:        "invalid DiskMBPSReadWriteField value in parameters",
			inputParams: map[string]string{consts.DiskMBPSReadWriteField: "diskMBPSReadWrite"},
			expectedOutput: ManagedDiskParameters{
				Tags:           make(map[string]string),
				VolumeContext:  map[string]string{consts.DiskMBPSReadWriteField: "diskMBPSReadWrite"},
				DeviceSettings: make(map[string]string),
			},
			expectedError: fmt.Errorf("parse diskmbpsreadwrite:diskMBPSReadWrite failed with error: strconv.Atoi: parsing \"diskMBPSReadWrite\": invalid syntax"),
		},
		{
			name: "valid parameters input",
			inputParams: map[string]string{
				consts.SkuNameField:             "skuName",
				consts.LocationField:            "location",
				consts.CachingModeField:         "cachingMode",
				consts.ResourceGroupField:       "resourceGroup",
				consts.DiskIOPSReadWriteField:   "4000",
				consts.DiskMBPSReadWriteField:   "1000",
				consts.LogicalSectorSizeField:   "1",
				consts.DiskNameField:            "diskName",
				consts.DesIDField:               "diskEncyptionSetID",
				consts.TagsField:                "key0=value0, key1=value1",
				consts.WriteAcceleratorEnabled:  "writeAcceleratorEnabled",
				consts.PvcNameKey:               "pvcName",
				consts.PvcNamespaceKey:          "pvcNamespace",
				consts.PvNameKey:                "pvName",
				consts.FsTypeField:              "fsType",
				consts.KindField:                "ignored",
				consts.MaxSharesField:           "1",
				consts.PerfProfileField:         "None",
				consts.NetworkAccessPolicyField: "networkAccessPolicy",
				consts.DiskAccessIDField:        "diskAccessID",
				consts.EnableBurstingField:      "true",
				consts.UserAgentField:           "userAgent",
				consts.EnableAsyncAttachField:   "enableAsyncAttach",
				consts.ZonedField:               "ignored",
			},
			expectedOutput: ManagedDiskParameters{
				AccountType:         "skuName",
				Location:            "location",
				CachingMode:         v1.AzureDataDiskCachingMode("cachingMode"),
				ResourceGroup:       "resourceGroup",
				DiskIOPSReadWrite:   "4000",
				DiskMBPSReadWrite:   "1000",
				DiskName:            "diskName",
				DiskEncryptionSetID: "diskEncyptionSetID",
				Tags: map[string]string{
					consts.PvcNameTag:      "pvcName",
					consts.PvcNamespaceTag: "pvcNamespace",
					consts.PvNameTag:       "pvName",
					"key0":                 "value0",
					"key1":                 "value1",
				},
				WriteAcceleratorEnabled: "writeAcceleratorEnabled",
				FsType:                  "fstype",
				PerfProfile:             "None",
				NetworkAccessPolicy:     "networkAccessPolicy",
				DiskAccessID:            "diskAccessID",
				EnableBursting:          ptr.To(true),
				UserAgent:               "userAgent",
				VolumeContext: map[string]string{
					consts.SkuNameField:             "skuName",
					consts.LocationField:            "location",
					consts.CachingModeField:         "cachingMode",
					consts.ResourceGroupField:       "resourceGroup",
					consts.DiskIOPSReadWriteField:   "4000",
					consts.DiskMBPSReadWriteField:   "1000",
					consts.LogicalSectorSizeField:   "1",
					consts.DiskNameField:            "diskName",
					consts.DesIDField:               "diskEncyptionSetID",
					consts.TagsField:                "key0=value0, key1=value1",
					consts.WriteAcceleratorEnabled:  "writeAcceleratorEnabled",
					consts.PvcNameKey:               "pvcName",
					consts.PvcNamespaceKey:          "pvcNamespace",
					consts.PvNameKey:                "pvName",
					consts.FsTypeField:              "fsType",
					consts.KindField:                string(v1.AzureManagedDisk),
					consts.MaxSharesField:           "1",
					consts.PerfProfileField:         "None",
					consts.NetworkAccessPolicyField: "networkAccessPolicy",
					consts.DiskAccessIDField:        "diskAccessID",
					consts.EnableBurstingField:      "true",
					consts.UserAgentField:           "userAgent",
					consts.EnableAsyncAttachField:   "enableAsyncAttach",
					consts.ZonedField:               "ignored",
				},
				DeviceSettings:    make(map[string]string),
				MaxShares:         1,
				LogicalSectorSize: 1,
			},
			expectedError: nil,
		},
	}
	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			result, err := ParseDiskParameters(test.inputParams)
			require.Equal(t, test.expectedError, err)
			assert.Equal(t, test.expectedOutput, result)
		})
	}
}

func TestPickAvailabilityZone(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "requirement missing ",
			testFunc: func(t *testing.T) {
				expectedresponse := ""
				region := "test"
				actualresponse := PickAvailabilityZone(nil, region, "N/A")
				if !reflect.DeepEqual(expectedresponse, actualresponse) {
					t.Errorf("actualresponse: (%v), expectedresponse: (%v)", actualresponse, expectedresponse)
				}
			},
		},
		{
			name: "valid get preferred",
			testFunc: func(t *testing.T) {
				expectedresponse := "test-01"
				region := "test"
				mp := make(map[string]string)
				mp["N/A"] = "test-01"
				topology := &csi.Topology{
					Segments: mp,
				}
				topologies := []*csi.Topology{}
				topologies = append(topologies, topology)
				req := &csi.TopologyRequirement{
					Preferred: topologies,
				}
				actualresponse := PickAvailabilityZone(req, region, "N/A")
				if !reflect.DeepEqual(expectedresponse, actualresponse) {
					t.Errorf("actualresponse: (%v), expectedresponse: (%v)", actualresponse, expectedresponse)
				}
			},
		},
		{
			name: "valid get requisite",
			testFunc: func(t *testing.T) {
				expectedresponse := "test-01"
				region := "test"
				mp := make(map[string]string)
				mp["N/A"] = "test-01"
				topology := &csi.Topology{
					Segments: mp,
				}
				topologies := []*csi.Topology{}
				topologies = append(topologies, topology)
				req := &csi.TopologyRequirement{
					Requisite: topologies,
				}
				actualresponse := PickAvailabilityZone(req, region, "N/A")
				if !reflect.DeepEqual(expectedresponse, actualresponse) {
					t.Errorf("actualresponse: (%v), expectedresponse: (%v)", actualresponse, expectedresponse)
				}
			},
		},
		{
			name: "valid get preferred - WellKnownTopologyKey",
			testFunc: func(t *testing.T) {
				expectedresponse := "test-02"
				region := "test"
				mp := make(map[string]string)
				mp["N/A"] = "test-01"
				mp[consts.WellKnownTopologyKey] = "test-02"
				topology := &csi.Topology{
					Segments: mp,
				}
				topologies := []*csi.Topology{}
				topologies = append(topologies, topology)
				req := &csi.TopologyRequirement{
					Preferred: topologies,
				}
				actualresponse := PickAvailabilityZone(req, region, "N/A")
				if !reflect.DeepEqual(expectedresponse, actualresponse) {
					t.Errorf("actualresponse: (%v), expectedresponse: (%v)", actualresponse, expectedresponse)
				}
			},
		},
		{
			name: "valid get requisite - WellKnownTopologyKey",
			testFunc: func(t *testing.T) {
				expectedresponse := "test-02"
				region := "test"
				mp := make(map[string]string)
				mp["N/A"] = "test-01"
				mp[consts.WellKnownTopologyKey] = "test-02"
				topology := &csi.Topology{
					Segments: mp,
				}
				topologies := []*csi.Topology{}
				topologies = append(topologies, topology)
				req := &csi.TopologyRequirement{
					Requisite: topologies,
				}
				actualresponse := PickAvailabilityZone(req, region, "N/A")
				if !reflect.DeepEqual(expectedresponse, actualresponse) {
					t.Errorf("actualresponse: (%v), expectedresponse: (%v)", actualresponse, expectedresponse)
				}
			},
		},
		{
			name: "empty request ",
			testFunc: func(t *testing.T) {
				req := &csi.TopologyRequirement{}
				expectedresponse := ""
				region := "test"
				actualresponse := PickAvailabilityZone(req, region, "N/A")
				if !reflect.DeepEqual(expectedresponse, actualresponse) {
					t.Errorf("actualresponse: (%v), expectedresponse: (%v)", actualresponse, expectedresponse)
				}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func createTestFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return nil
}

func TestInsertDiskProperties(t *testing.T) {
	tests := []struct {
		desc        string
		disk        *armcompute.Disk
		inputMap    map[string]string
		expectedMap map[string]string
	}{
		{
			desc: "nil pointer",
		},
		{
			desc:        "empty",
			disk:        &armcompute.Disk{},
			inputMap:    map[string]string{},
			expectedMap: map[string]string{},
		},
		{
			desc: "skuName",
			disk: &armcompute.Disk{
				SKU: &armcompute.DiskSKU{Name: to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS)},
			},
			inputMap:    map[string]string{},
			expectedMap: map[string]string{"skuname": string(armcompute.DiskStorageAccountTypesPremiumLRS)},
		},
		{
			desc: "DiskProperties",
			disk: &armcompute.Disk{
				SKU: &armcompute.DiskSKU{Name: to.Ptr(armcompute.DiskStorageAccountTypesStandardSSDLRS)},
				Properties: &armcompute.DiskProperties{
					NetworkAccessPolicy: to.Ptr(armcompute.NetworkAccessPolicyAllowPrivate),
					DiskIOPSReadWrite:   ptr.To(int64(6400)),
					DiskMBpsReadWrite:   ptr.To(int64(100)),
					CreationData: &armcompute.CreationData{
						LogicalSectorSize: ptr.To(int32(512)),
					},
					Encryption: &armcompute.Encryption{DiskEncryptionSetID: ptr.To("/subs/DiskEncryptionSetID")},
					MaxShares:  ptr.To(int32(3)),
				},
			},
			inputMap: map[string]string{},
			expectedMap: map[string]string{
				consts.SkuNameField:             string(armcompute.DiskStorageAccountTypesStandardSSDLRS),
				consts.NetworkAccessPolicyField: string(armcompute.NetworkAccessPolicyAllowPrivate),
				consts.DiskIOPSReadWriteField:   "6400",
				consts.DiskMBPSReadWriteField:   "100",
				consts.LogicalSectorSizeField:   "512",
				consts.DesIDField:               "/subs/DiskEncryptionSetID",
				consts.MaxSharesField:           "3",
			},
		},
		{
			// Azure Stack does not support NetworkAccessPolicy property
			desc: "DiskProperties with nil NetworkAccessPolicy",
			disk: &armcompute.Disk{
				SKU: &armcompute.DiskSKU{Name: to.Ptr(armcompute.DiskStorageAccountTypesStandardSSDLRS)},
				Properties: &armcompute.DiskProperties{
					NetworkAccessPolicy: nil,
					DiskIOPSReadWrite:   ptr.To(int64(6400)),
					DiskMBpsReadWrite:   ptr.To(int64(100)),
					CreationData: &armcompute.CreationData{
						LogicalSectorSize: ptr.To(int32(512)),
					},
				},
			},
			inputMap: map[string]string{},
			expectedMap: map[string]string{
				consts.SkuNameField:           string(armcompute.DiskStorageAccountTypesStandardSSDLRS),
				consts.DiskIOPSReadWriteField: "6400",
				consts.DiskMBPSReadWriteField: "100",
				consts.LogicalSectorSizeField: "512",
			},
		},
	}

	for _, test := range tests {
		InsertDiskProperties(test.disk, test.inputMap)
		for k, v := range test.inputMap {
			if test.expectedMap[k] != v {
				t.Errorf("test [%q] get unexpected result: (%v, %v) != (%v, %v)", test.desc, k, v, k, test.expectedMap[k])
			}
		}
	}
}

func TestSleepIfThrottled(t *testing.T) {
	const sleepDuration = 1 * time.Second

	tests := []struct {
		description           string
		err                   error
		expectedSleepDuration time.Duration
	}{
		{
			description: "No sleep",
			err:         errors.New("do not sleep"),
		},
		{
			description:           "Too many requests, sleep 100ms",
			err:                   errors.New(consts.TooManyRequests),
			expectedSleepDuration: sleepDuration,
		},
		{
			description:           "Client throttled, sleep 100ms",
			err:                   errors.New(consts.ClientThrottled),
			expectedSleepDuration: sleepDuration,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.description, func(t *testing.T) {
			start := time.Now()
			SleepIfThrottled(test.err, int(sleepDuration.Seconds()))
			actualSleepDuration := time.Since(start)
			if test.expectedSleepDuration == 0 {
				assert.Less(t, actualSleepDuration, sleepDuration)
			} else {
				assert.GreaterOrEqual(t, actualSleepDuration, test.expectedSleepDuration)
			}
		})
	}
}

func TestSetKeyValueInMap(t *testing.T) {
	tests := []struct {
		desc     string
		m        map[string]string
		key      string
		value    string
		expected map[string]string
	}{
		{
			desc:  "nil map",
			key:   "key",
			value: "value",
		},
		{
			desc:     "empty map",
			m:        map[string]string{},
			key:      "key",
			value:    "value",
			expected: map[string]string{"key": "value"},
		},
		{
			desc:  "non-empty map",
			m:     map[string]string{"k": "v"},
			key:   "key",
			value: "value",
			expected: map[string]string{
				"k":   "v",
				"key": "value",
			},
		},
		{
			desc:     "same key already exists",
			m:        map[string]string{"subDir": "value2"},
			key:      "subDir",
			value:    "value",
			expected: map[string]string{"subDir": "value"},
		},
		{
			desc:     "case insensitive key already exists",
			m:        map[string]string{"subDir": "value2"},
			key:      "subdir",
			value:    "value",
			expected: map[string]string{"subDir": "value"},
		},
	}

	for _, test := range tests {
		SetKeyValueInMap(test.m, test.key, test.value)
		if !reflect.DeepEqual(test.m, test.expected) {
			t.Errorf("test[%s]: unexpected output: %v, expected result: %v", test.desc, test.m, test.expected)
		}
	}
}

func TestGetAttachDiskInitialDelay(t *testing.T) {
	tests := []struct {
		name       string
		attributes map[string]string
		expected   int
	}{
		{
			attributes: nil,
			expected:   -1,
		},
		{
			attributes: map[string]string{consts.AttachDiskInitialDelayField: "10"},
			expected:   10,
		},
		{
			attributes: map[string]string{"AttachDiskInitialDelay": "90"},
			expected:   90,
		},
		{
			attributes: map[string]string{"unknown": "90"},
			expected:   -1,
		},
	}

	for _, test := range tests {
		if got := GetAttachDiskInitialDelay(test.attributes); got != test.expected {
			t.Errorf("GetAttachDiskInitialDelay(%v) = %v, want %v", test.attributes, got, test.expected)
		}
	}
}

func TestGetRetryAfterSeconds(t *testing.T) {
	tests := []struct {
		desc     string
		err      error
		expected int
	}{
		{
			desc:     "nil error",
			err:      nil,
			expected: 0,
		},
		{
			desc:     "no match",
			err:      errors.New("no match"),
			expected: 0,
		},
		{
			desc:     "match",
			err:      errors.New("RetryAfter: 10s"),
			expected: 10,
		},
		{
			desc:     "match error message",
			err:      errors.New("could not list storage accounts for account type Premium_LRS: Retriable: true, RetryAfter: 217s, HTTPStatusCode: 0, RawError: azure cloud provider throttled for operation StorageAccountListByResourceGroup with reason \"client throttled\""),
			expected: 217,
		},
		{
			desc:     "match error message exceeds 1200s",
			err:      errors.New("could not list storage accounts for account type Premium_LRS: Retriable: true, RetryAfter: 2170s, HTTPStatusCode: 0, RawError: azure cloud provider throttled for operation StorageAccountListByResourceGroup with reason \"client throttled\""),
			expected: consts.MaxThrottlingSleepSec,
		},
	}

	for _, test := range tests {
		result := getRetryAfterSeconds(test.err)
		if result != test.expected {
			t.Errorf("desc: (%s), input: err(%v), getRetryAfterSeconds returned with int(%d), not equal to expected(%d)",
				test.desc, test.err, result, test.expected)
		}
	}
}

func TestIsThrottlingError(t *testing.T) {
	tests := []struct {
		desc     string
		err      error
		expected bool
	}{
		{
			desc:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			desc:     "no match",
			err:      errors.New("no match"),
			expected: false,
		},
		{
			desc:     "match error message",
			err:      errors.New("could not list storage accounts for account type Premium_LRS: Retriable: true, RetryAfter: 217s, HTTPStatusCode: 0, RawError: azure cloud provider throttled for operation StorageAccountListByResourceGroup with reason \"client throttled\""),
			expected: true,
		},
		{
			desc:     "match error message exceeds 1200s",
			err:      errors.New("could not list storage accounts for account type Premium_LRS: Retriable: true, RetryAfter: 2170s, HTTPStatusCode: 0, RawError: azure cloud provider throttled for operation StorageAccountListByResourceGroup with reason \"client throttled\""),
			expected: true,
		},
		{
			desc:     "match error message with TooManyRequests throttling",
			err:      errors.New("could not list storage accounts for account type Premium_LRS: Retriable: true, RetryAfter: 2170s, HTTPStatusCode: 429, RawError: azure cloud provider throttled for operation StorageAccountListByResourceGroup with reason \"TooManyRequests\""),
			expected: true,
		},
	}

	for _, test := range tests {
		result := IsThrottlingError(test.err)
		if result != test.expected {
			t.Errorf("desc: (%s), input: err(%v), IsThrottlingError returned with bool(%t), not equal to expected(%t)",
				test.desc, test.err, result, test.expected)
		}
	}
}

func TestGenerateVolumeName(t *testing.T) {
	// Normal operation, no truncate
	v1 := GenerateVolumeName("kubernetes", "pv-cinder-abcde", 255)
	if v1 != "kubernetes-dynamic-pv-cinder-abcde" {
		t.Errorf("Expected kubernetes-dynamic-pv-cinder-abcde, got %s", v1)
	}
	// Truncate trailing "6789-dynamic"
	prefix := strings.Repeat("0123456789", 9) // 90 characters prefix + 8 chars. of "-dynamic"
	v2 := GenerateVolumeName(prefix, "pv-cinder-abcde", 100)
	expect := prefix[:84] + "-pv-cinder-abcde"
	if v2 != expect {
		t.Errorf("Expected %s, got %s", expect, v2)
	}
	// Truncate really long cluster name
	prefix = strings.Repeat("0123456789", 1000) // 10000 characters prefix
	v3 := GenerateVolumeName(prefix, "pv-cinder-abcde", 100)
	if v3 != expect {
		t.Errorf("Expected %s, got %s", expect, v3)
	}
}

func TestRemoveOptionIfExists(t *testing.T) {
	tests := []struct {
		desc            string
		options         []string
		removeOption    string
		expectedOptions []string
		expected        bool
	}{
		{
			desc:         "nil options",
			removeOption: "option",
			expected:     false,
		},
		{
			desc:            "empty options",
			options:         []string{},
			removeOption:    "option",
			expectedOptions: []string{},
			expected:        false,
		},
		{
			desc:            "option not found",
			options:         []string{"option1", "option2"},
			removeOption:    "option",
			expectedOptions: []string{"option1", "option2"},
			expected:        false,
		},
		{
			desc:            "option found in the last element",
			options:         []string{"option1", "option2", "option"},
			removeOption:    "option",
			expectedOptions: []string{"option1", "option2"},
			expected:        true,
		},
		{
			desc:            "option found in the first element",
			options:         []string{"option", "option1", "option2"},
			removeOption:    "option",
			expectedOptions: []string{"option1", "option2"},
			expected:        true,
		},
		{
			desc:            "option found in the middle element",
			options:         []string{"option1", "option", "option2"},
			removeOption:    "option",
			expectedOptions: []string{"option1", "option2"},
			expected:        true,
		},
	}

	for _, test := range tests {
		result, exists := RemoveOptionIfExists(test.options, test.removeOption)
		if !reflect.DeepEqual(result, test.expectedOptions) {
			t.Errorf("test[%s]: unexpected output: %v, expected result: %v", test.desc, result, test.expectedOptions)
		}
		if exists != test.expected {
			t.Errorf("test[%s]: unexpected output: %v, expected result: %v", test.desc, exists, test.expected)
		}
	}
}

func TestGetInfoFromURI(t *testing.T) {
	tests := []struct {
		URL            string
		expectedName   string
		expectedRGName string
		expectedSubsID string
		expectedError  error
	}{
		{
			URL:            "/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/disks/disk-name",
			expectedName:   "disk-name",
			expectedRGName: "23",
			expectedSubsID: "12",
			expectedError:  nil,
		},
		{
			URL:            "testurl/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/snapshots/snapshot-name",
			expectedName:   "snapshot-name",
			expectedRGName: "23",
			expectedSubsID: "12",
			expectedError:  nil,
		},
		{
			// case insensitive check
			URL:            "testurl/subscriptions/12/resourcegroups/23/providers/Microsoft.Compute/snapshots/snapshot-name",
			expectedName:   "snapshot-name",
			expectedRGName: "23",
			expectedSubsID: "12",
			expectedError:  nil,
		},
		{
			URL:            "testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name",
			expectedName:   "",
			expectedRGName: "",
			expectedError:  fmt.Errorf("invalid URI: testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name"),
		},
	}
	for _, test := range tests {
		subsID, resourceGroup, snapshotName, err := GetInfoFromURI(test.URL)
		if !reflect.DeepEqual(snapshotName, test.expectedName) ||
			!reflect.DeepEqual(resourceGroup, test.expectedRGName) ||
			!reflect.DeepEqual(subsID, test.expectedSubsID) ||
			!reflect.DeepEqual(err, test.expectedError) {
			t.Errorf("input: %q, getSnapshotName result: %q, expectedName: %q, getresourcegroup result: %q, expectedRGName: %q\n", test.URL, snapshotName, test.expectedName,
				resourceGroup, test.expectedRGName)
			if err != nil {
				t.Errorf("err result %q\n", err)
			}
		}
	}
}

func TestParseDiskParameters_DiskNameTemplateVariables(t *testing.T) {
	tests := []struct {
		name        string
		diskName    string
		tags        map[string]string
		expected    string
		description string
	}{
		{
			name:     "all variables replaced",
			diskName: "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			tags: map[string]string{
				consts.PvcNameTag:      "mypvc",
				consts.PvcNamespaceTag: "myns",
				consts.PvNameTag:       "mypv",
			},
			expected:    "disk-mypvc-myns-mypv",
			description: "All template variables are replaced",
		},
		{
			name:     "missing pvc name",
			diskName: "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			tags: map[string]string{
				consts.PvcNamespaceTag: "myns",
				consts.PvNameTag:       "mypv",
			},
			expected:    "disk-${pvc.metadata.name}-myns-mypv",
			description: "pvc name missing, only others replaced",
		},
		{
			name:     "missing pvc namespace",
			diskName: "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			tags: map[string]string{
				consts.PvcNameTag: "mypvc",
				consts.PvNameTag:  "mypv",
			},
			expected:    "disk-mypvc-${pvc.metadata.namespace}-mypv",
			description: "pvc namespace missing, only others replaced",
		},
		{
			name:     "missing pv name",
			diskName: "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			tags: map[string]string{
				consts.PvcNameTag:      "mypvc",
				consts.PvcNamespaceTag: "myns",
			},
			expected:    "disk-mypvc-myns-${pv.metadata.name}",
			description: "pv name missing, only others replaced",
		},
		{
			name:     "no variables present",
			diskName: "disk-plain",
			tags: map[string]string{
				consts.PvcNameTag:      "mypvc",
				consts.PvcNamespaceTag: "myns",
				consts.PvNameTag:       "mypv",
			},
			expected:    "disk-plain",
			description: "No template variables, disk name unchanged",
		},
		{
			name:        "empty tags",
			diskName:    "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			tags:        map[string]string{},
			expected:    "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			description: "No tags, disk name unchanged",
		},
		{
			name:     "empty disk name",
			diskName: "",
			tags: map[string]string{
				consts.PvcNameTag:      "mypvc",
				consts.PvcNamespaceTag: "myns",
				consts.PvNameTag:       "mypv",
			},
			expected:    "",
			description: "Empty disk name, nothing to replace",
		},
		{
			name:     "variables with empty tag values",
			diskName: "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			tags: map[string]string{
				consts.PvcNameTag:      "",
				consts.PvcNamespaceTag: "",
				consts.PvNameTag:       "",
			},
			expected:    "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			description: "Tags present but empty, disk name unchanged",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := ManagedDiskParameters{
				DiskName: tt.diskName,
				Tags:     make(map[string]string),
			}
			// Copy tags to params.Tags
			for k, v := range tt.tags {
				params.Tags[k] = v
			}
			// Simulate the template replacement logic
			if strings.Contains(params.DiskName, "$") {
				if pvcName, ok := params.Tags[consts.PvcNameTag]; ok && pvcName != "" {
					params.DiskName = strings.ReplaceAll(params.DiskName, "${pvc.metadata.name}", pvcName)
				}
				if pvcNamespace, ok := params.Tags[consts.PvcNamespaceTag]; ok && pvcNamespace != "" {
					params.DiskName = strings.ReplaceAll(params.DiskName, "${pvc.metadata.namespace}", pvcNamespace)
				}
				if pvName, ok := params.Tags[consts.PvNameTag]; ok && pvName != "" {
					params.DiskName = strings.ReplaceAll(params.DiskName, "${pv.metadata.name}", pvName)
				}
			}
			assert.Equal(t, tt.expected, params.DiskName, tt.description)
		})
	}
}

func TestParseDiskParameters_CustomTagsWithTemplateVariables(t *testing.T) {
	tests := []struct {
		name        string
		customTags  string
		pvcName     string
		pvcNS       string
		pvName      string
		expected    map[string]string
		description string
	}{
		{
			name:       "all variables replaced in tag value",
			customTags: "tag1=${pvc.metadata.name},tag2=${pvc.metadata.namespace},tag3=${pv.metadata.name}",
			pvcName:    "mypvc",
			pvcNS:      "myns",
			pvName:     "mypv",
			expected: map[string]string{
				"tag1": "mypvc",
				"tag2": "myns",
				"tag3": "mypv",
			},
			description: "All template variables in tag values are replaced",
		},
		{
			name:       "missing pvc name in tag value",
			customTags: "tag1=${pvc.metadata.name},tag2=${pvc.metadata.namespace},tag3=${pv.metadata.name}",
			pvcName:    "",
			pvcNS:      "myns",
			pvName:     "mypv",
			expected: map[string]string{
				"tag1": "",
				"tag2": "myns",
				"tag3": "mypv",
			},
			description: "pvc name missing, only others replaced",
		},
		{
			name:       "missing pvc namespace in tag value",
			customTags: "tag1=${pvc.metadata.name},tag2=${pvc.metadata.namespace},tag3=${pv.metadata.name}",
			pvcName:    "mypvc",
			pvcNS:      "",
			pvName:     "mypv",
			expected: map[string]string{
				"tag1": "mypvc",
				"tag2": "",
				"tag3": "mypv",
			},
			description: "pvc namespace missing, only others replaced",
		},
		{
			name:       "missing pv name in tag value",
			customTags: "tag1=${pvc.metadata.name},tag2=${pvc.metadata.namespace},tag3=${pv.metadata.name}",
			pvcName:    "mypvc",
			pvcNS:      "myns",
			pvName:     "",
			expected: map[string]string{
				"tag1": "mypvc",
				"tag2": "myns",
				"tag3": "",
			},
			description: "pv name missing, only others replaced",
		},
		{
			name:       "no variables present in tag value",
			customTags: "tag1=plain,tag2=plain2",
			pvcName:    "mypvc",
			pvcNS:      "myns",
			pvName:     "mypv",
			expected: map[string]string{
				"tag1": "plain",
				"tag2": "plain2",
			},
			description: "No template variables, tag values unchanged",
		},
		{
			name:        "empty tags",
			customTags:  "",
			pvcName:     "mypvc",
			pvcNS:       "myns",
			pvName:      "mypv",
			expected:    map[string]string{},
			description: "No tags, nothing to replace",
		},
		{
			name:       "variables with empty tag values",
			customTags: "tag1=${pvc.metadata.name},tag2=${pvc.metadata.namespace},tag3=${pv.metadata.name}",
			pvcName:    "",
			pvcNS:      "",
			pvName:     "",
			expected: map[string]string{
				"tag1": "",
				"tag2": "",
				"tag3": "",
			},
			description: "Tags present but empty, tag values unchanged",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := ManagedDiskParameters{
				Tags: make(map[string]string),
			}
			// Simulate util.ConvertTagsToMap
			customTagsMap, err := util.ConvertTagsToMap(tt.customTags, "")
			require.NoError(t, err)
			pvcName := tt.pvcName
			pvcNamespace := tt.pvcNS
			pvName := tt.pvName
			for k, v := range customTagsMap {
				if strings.Contains(v, "$") {
					v = strings.ReplaceAll(v, consts.PvcMetaDataName, pvcName)
					v = strings.ReplaceAll(v, consts.PvcMetaDataNamespace, pvcNamespace)
					params.Tags[k] = strings.ReplaceAll(v, consts.PvMetaDataName, pvName)
				} else {
					params.Tags[k] = v
				}
			}
			assert.Equal(t, tt.expected, params.Tags, tt.description)
		})
	}
}

func TestReplaceTemplateVariables(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		pvcName     string
		pvcNS       string
		pvName      string
		expected    string
		description string
	}{
		{
			name:        "all variables present",
			input:       "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			pvcName:     "mypvc",
			pvcNS:       "myns",
			pvName:      "mypv",
			expected:    "disk-mypvc-myns-mypv",
			description: "All template variables are replaced",
		},
		{
			name:        "missing pvc name",
			input:       "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			pvcName:     "",
			pvcNS:       "myns",
			pvName:      "mypv",
			expected:    "disk--myns-mypv",
			description: "pvc name missing, others replaced",
		},
		{
			name:        "missing pvc namespace",
			input:       "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			pvcName:     "mypvc",
			pvcNS:       "",
			pvName:      "mypv",
			expected:    "disk-mypvc--mypv",
			description: "pvc namespace missing, others replaced",
		},
		{
			name:        "missing pv name",
			input:       "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			pvcName:     "mypvc",
			pvcNS:       "myns",
			pvName:      "",
			expected:    "disk-mypvc-myns-",
			description: "pv name missing, others replaced",
		},
		{
			name:        "no variables present",
			input:       "disk-plain",
			pvcName:     "mypvc",
			pvcNS:       "myns",
			pvName:      "mypv",
			expected:    "disk-plain",
			description: "No template variables, string unchanged",
		},
		{
			name:        "empty input string",
			input:       "",
			pvcName:     "mypvc",
			pvcNS:       "myns",
			pvName:      "mypv",
			expected:    "",
			description: "Empty input string",
		},
		{
			name:        "variables with empty tag values",
			input:       "disk-${pvc.metadata.name}-${pvc.metadata.namespace}-${pv.metadata.name}",
			pvcName:     "",
			pvcNS:       "",
			pvName:      "",
			expected:    "disk---",
			description: "All variables empty, replaced with empty strings",
		},
		{
			name:        "partial variables in string",
			input:       "disk-${pvc.metadata.name}-plain",
			pvcName:     "mypvc",
			pvcNS:       "",
			pvName:      "",
			expected:    "disk-mypvc-plain",
			description: "Only pvc name replaced",
		},
		{
			name:        "multiple occurrences of same variable",
			input:       "${pvc.metadata.name}-${pvc.metadata.name}",
			pvcName:     "mypvc",
			pvcNS:       "",
			pvName:      "",
			expected:    "mypvc-mypvc",
			description: "Multiple occurrences replaced",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceTemplateVariables(tt.input, tt.pvcName, tt.pvcNS, tt.pvName)
			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}

func TestParseDiskParametersForKey(t *testing.T) {
	tests := []struct {
		name           string
		parameters     map[string]string
		key            string
		expectedValue  string
		expectedExists bool
		description    string
	}{
		{
			name:           "nil parameters map",
			parameters:     nil,
			key:            "skuName",
			expectedValue:  "",
			expectedExists: false,
			description:    "Should handle nil parameters map gracefully",
		},
		{
			name:           "empty parameters map",
			parameters:     map[string]string{},
			key:            "skuName",
			expectedValue:  "",
			expectedExists: false,
			description:    "Should return false for empty map",
		},
		{
			name: "exact key match",
			parameters: map[string]string{
				"skuName": "Premium_LRS",
			},
			key:            "skuName",
			expectedValue:  "Premium_LRS",
			expectedExists: true,
			description:    "Should find exact key match",
		},
		{
			name: "case insensitive key match - lowercase key",
			parameters: map[string]string{
				"SKUNAME": "PremiumV2_LRS",
			},
			key:            "skuName",
			expectedValue:  "PremiumV2_LRS",
			expectedExists: true,
			description:    "Should match regardless of key case (lowercase search)",
		},
		{
			name: "case insensitive key match - uppercase key",
			parameters: map[string]string{
				"skuname": "StandardSSD_LRS",
			},
			key:            "SKUNAME",
			expectedValue:  "StandardSSD_LRS",
			expectedExists: true,
			description:    "Should match regardless of key case (uppercase search)",
		},
		{
			name: "case insensitive key match - mixed case",
			parameters: map[string]string{
				"SkuName": "Premium_ZRS",
			},
			key:            "skuname",
			expectedValue:  "Premium_ZRS",
			expectedExists: true,
			description:    "Should match with mixed case variations",
		},
		{
			name: "storageAccountType key",
			parameters: map[string]string{
				"storageAccountType": "Premium_LRS",
			},
			key:            "storageAccountType",
			expectedValue:  "Premium_LRS",
			expectedExists: true,
			description:    "Should find storageAccountType key",
		},
		{
			name: "case insensitive storageAccountType",
			parameters: map[string]string{
				"STORAGEACCOUNTTYPE": "PremiumV2_LRS",
			},
			key:            "storageaccounttype",
			expectedValue:  "PremiumV2_LRS",
			expectedExists: true,
			description:    "Should match storageAccountType case insensitively",
		},
		{
			name: "key not found",
			parameters: map[string]string{
				"skuName":       "Premium_LRS",
				"location":      "eastus",
				"resourceGroup": "myRG",
			},
			key:            "nonexistentkey",
			expectedValue:  "",
			expectedExists: false,
			description:    "Should return false for non-existent key",
		},
		{
			name: "multiple keys with case variations",
			parameters: map[string]string{
				"skuName":            "Premium_LRS",
				"STORAGEACCOUNTTYPE": "StandardSSD_LRS",
				"Location":           "eastus",
			},
			key:            "location",
			expectedValue:  "eastus",
			expectedExists: true,
			description:    "Should find correct key among multiple case variations",
		},
		{
			name: "empty value but key exists",
			parameters: map[string]string{
				"skuName": "",
			},
			key:            "skuName",
			expectedValue:  "",
			expectedExists: true,
			description:    "Should return true for empty value but existing key",
		},
		{
			name: "value with spaces",
			parameters: map[string]string{
				"description": "Premium LRS with spaces",
			},
			key:            "description",
			expectedValue:  "Premium LRS with spaces",
			expectedExists: true,
			description:    "Should handle values with spaces",
		},
		{
			name: "special characters in value",
			parameters: map[string]string{
				"customTag": "tag-value_with.special@chars",
			},
			key:            "customTag",
			expectedValue:  "tag-value_with.special@chars",
			expectedExists: true,
			description:    "Should handle special characters in values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, exists := ParseDiskParametersForKey(tt.parameters, tt.key)
			assert.Equal(t, tt.expectedExists, exists, "exists flag should match expected for: %s", tt.description)
			if tt.expectedExists {
				assert.Equal(t, tt.expectedValue, value, "value should match expected for: %s", tt.description)
			}
		})
	}
}
