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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	consts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azuredisk/mockcorev1"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azuredisk/mockkubeclient"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azuredisk/mockpersistentvolume"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azuredisk/mockpersistentvolumeclaim"
	volumehelper "sigs.k8s.io/azuredisk-csi-driver/pkg/util"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/diskclient/mock_diskclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/policy/retryrepectthrottled"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/resourcegroupclient/mock_resourcegroupclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/snapshotclient/mock_snapshotclient"
	mockvmclient "sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachineclient/mock_virtualmachineclient"
	azure "sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

var (
	testVolumeName = "unit-test-volume"
	testVolumeID   = fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", testVolumeName)
)

func checkTestError(t *testing.T, expectedErrCode codes.Code, err error) {
	s, ok := status.FromError(err)
	if !ok {
		t.Errorf("could not get error status from err: %v", s)
	}
	if s.Code() != expectedErrCode {
		t.Errorf("expected error code: %v, actual: %v, err: %v", expectedErrCode, s.Code(), err)
	}
}

func TestGetAKSClusterResourceID(t *testing.T) {
	t.Run("builds resource ID from AKS tags", func(t *testing.T) {
		cntl := gomock.NewController(t)
		d, err := NewFakeDriver(cntl)
		require.NoError(t, err)
		driver := d.(*fakeDriver)
		driver.cloud.SubscriptionID = "subscription"
		driver.cloud.ResourceGroup = "node-resource-group"

		resourceGroupClient := mock_resourcegroupclient.NewMockInterface(cntl)
		driver.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetResourceGroupClient().Return(resourceGroupClient)
		resourceGroupClient.EXPECT().Get(gomock.Any(), "node-resource-group").Return(&armresources.ResourceGroup{
			Tags: map[string]*string{
				aksManagedClusterNameTag:          to.Ptr("cluster"),
				aksManagedClusterResourceGroupTag: to.Ptr("cluster-resource-group"),
			},
		}, nil)

		resourceID, err := driver.getAKSClusterResourceID(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "/subscriptions/subscription/resourceGroups/cluster-resource-group/providers/Microsoft.ContainerService/managedClusters/cluster", resourceID)
	})

	t.Run("caches resolved resource ID", func(t *testing.T) {
		cntl := gomock.NewController(t)
		d, err := NewFakeDriver(cntl)
		require.NoError(t, err)
		driver := d.(*fakeDriver)
		driver.cloud.SubscriptionID = "subscription"
		driver.cloud.ResourceGroup = "node-resource-group"

		resourceGroupClient := mock_resourcegroupclient.NewMockInterface(cntl)
		// GetResourceGroupClient/Get must be invoked exactly once across both calls.
		driver.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetResourceGroupClient().Return(resourceGroupClient).Times(1)
		resourceGroupClient.EXPECT().Get(gomock.Any(), "node-resource-group").Return(&armresources.ResourceGroup{
			Tags: map[string]*string{
				aksManagedClusterNameTag:          to.Ptr("cluster"),
				aksManagedClusterResourceGroupTag: to.Ptr("cluster-resource-group"),
			},
		}, nil).Times(1)

		first, err := driver.getAKSClusterResourceID(context.Background())
		require.NoError(t, err)
		second, err := driver.getAKSClusterResourceID(context.Background())
		require.NoError(t, err)
		assert.Equal(t, first, second)
	})

	t.Run("falls back to subscription when AKS tags are missing", func(t *testing.T) {
		cntl := gomock.NewController(t)
		d, err := NewFakeDriver(cntl)
		require.NoError(t, err)
		driver := d.(*fakeDriver)
		driver.cloud.SubscriptionID = "subscription"
		driver.cloud.ResourceGroup = "node-resource-group"

		resourceGroupClient := mock_resourcegroupclient.NewMockInterface(cntl)
		driver.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetResourceGroupClient().Return(resourceGroupClient)
		resourceGroupClient.EXPECT().Get(gomock.Any(), "node-resource-group").Return(&armresources.ResourceGroup{}, nil)

		resourceID, err := driver.getAKSClusterResourceID(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "/subscriptions/subscription", resourceID)
	})
}

func TestClaimDiskResource(t *testing.T) {
	t.Run("returns blob URL", func(t *testing.T) {
		cntl := gomock.NewController(t)
		d, err := NewFakeDriver(cntl)
		require.NoError(t, err)
		driver := d.(*fakeDriver)

		ownerResourceID := "/subscriptions/subscription/resourceGroups/cluster-rg/providers/Microsoft.ContainerService/managedClusters/cluster"
		diskURI := "/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/disks/disk"
		blobURL := "https://md-storage.blob.core.windows.net/container/disk"
		server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
			assert.Equal(t, http.MethodPost, request.Method)
			assert.Equal(t, diskURI+"/claimResource", request.URL.Path)
			assert.Equal(t, claimResourceAPIVersion, request.URL.Query().Get("api-version"))
			assert.Equal(t, "Bearer token", request.Header.Get("Authorization"))

			var body struct {
				OwnerResourceID string `json:"ownerResourceId"`
			}
			require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
			assert.Equal(t, ownerResourceID, body.OwnerResourceID)
			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(http.StatusOK)
			_, err := fmt.Fprintf(responseWriter, `{"properties":{"blobUrl":%q,"claimIdentifier":"identifier"}}`, blobURL)
			require.NoError(t, err)
		}))
		defer server.Close()

		driver.cloud.ARMClientConfig.ResourceManagerEndpoint = server.URL
		credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
		credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil)

		actualBlobURL, claimIdentifier, err := driver.claimDiskResource(context.Background(), diskURI, ownerResourceID)
		require.NoError(t, err)
		assert.Equal(t, blobURL, actualBlobURL)
		assert.Equal(t, "identifier", claimIdentifier)
	})

	t.Run("returns empty blob URL when absent", func(t *testing.T) {
		cntl := gomock.NewController(t)
		d, err := NewFakeDriver(cntl)
		require.NoError(t, err)
		driver := d.(*fakeDriver)

		server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, _ *http.Request) {
			responseWriter.WriteHeader(http.StatusOK)
			_, err := responseWriter.Write([]byte(`{"properties":{"diskState":"Claimed"}}`))
			require.NoError(t, err)
		}))
		defer server.Close()

		driver.cloud.ARMClientConfig.ResourceManagerEndpoint = server.URL
		credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
		credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil)

		blobURL, claimIdentifier, err := driver.claimDiskResource(context.Background(), "/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/disks/disk", "owner")
		require.NoError(t, err)
		assert.Empty(t, blobURL)
		assert.Empty(t, claimIdentifier)
	})
}

func TestUnclaimDiskResource(t *testing.T) {
	t.Run("sends unclaim request", func(t *testing.T) {
		cntl := gomock.NewController(t)
		d, err := NewFakeDriver(cntl)
		require.NoError(t, err)
		driver := d.(*fakeDriver)

		ownerResourceID := "/subscriptions/subscription/resourceGroups/cluster-rg/providers/Microsoft.ContainerService/managedClusters/cluster"
		diskURI := "/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/disks/disk"
		server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
			assert.Equal(t, http.MethodPost, request.Method)
			assert.Equal(t, diskURI+"/unclaimResource", request.URL.Path)
			assert.Equal(t, claimResourceAPIVersion, request.URL.Query().Get("api-version"))
			assert.Equal(t, "Bearer token", request.Header.Get("Authorization"))

			var body struct {
				OwnerResourceID string `json:"ownerResourceId"`
			}
			require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
			assert.Equal(t, ownerResourceID, body.OwnerResourceID)
			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(http.StatusOK)
			_, err := responseWriter.Write([]byte(`{"managedBy":null,"properties":{"diskState":"Unattached"}}`))
			require.NoError(t, err)
		}))
		defer server.Close()

		driver.cloud.ARMClientConfig.ResourceManagerEndpoint = server.URL
		credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
		credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil)

		require.NoError(t, driver.unclaimDiskResource(context.Background(), diskURI, ownerResourceID))
	})

	t.Run("returns error on failure status", func(t *testing.T) {
		cntl := gomock.NewController(t)
		d, err := NewFakeDriver(cntl)
		require.NoError(t, err)
		driver := d.(*fakeDriver)

		server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, _ *http.Request) {
			responseWriter.WriteHeader(http.StatusConflict)
			_, err := responseWriter.Write([]byte(`{"error":"conflict"}`))
			require.NoError(t, err)
		}))
		defer server.Close()

		driver.cloud.ARMClientConfig.ResourceManagerEndpoint = server.URL
		credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
		credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil)

		err = driver.unclaimDiskResource(context.Background(), "/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/disks/disk", "owner")
		require.ErrorContains(t, err, "unclaimResource returned status 409")
	})

	t.Run("ignores not found for an already absent disk", func(t *testing.T) {
		cntl := gomock.NewController(t)
		d, err := NewFakeDriver(cntl)
		require.NoError(t, err)
		driver := d.(*fakeDriver)

		server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, _ *http.Request) {
			responseWriter.WriteHeader(http.StatusNotFound)
			_, err := responseWriter.Write([]byte(`{"error":{"code":"NotFound","message":"Disk is not found."}}`))
			require.NoError(t, err)
		}))
		defer server.Close()

		driver.cloud.ARMClientConfig.ResourceManagerEndpoint = server.URL
		credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
		credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil)

		err = driver.unclaimDiskResource(context.Background(), "/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/disks/missing", "owner")
		require.NoError(t, err)
	})
}

func TestPollAsyncOperation(t *testing.T) {
	t.Run("keeps polling on empty 202 then returns 200 body", func(t *testing.T) {
		cntl := gomock.NewController(t)
		d, err := NewFakeDriver(cntl)
		require.NoError(t, err)
		driver := d.(*fakeDriver)
		driver.pollInterval = time.Millisecond

		var calls int32
		server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, _ *http.Request) {
			// First poll: operation still running, 202 with an empty body.
			if atomic.AddInt32(&calls, 1) == 1 {
				responseWriter.WriteHeader(http.StatusAccepted)
				return
			}
			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(http.StatusOK)
			_, err := responseWriter.Write([]byte(`{"properties":{"blobUrl":"https://md-storage.blob.core.windows.net/container/disk"}}`))
			require.NoError(t, err)
		}))
		defer server.Close()

		credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
		credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil).AnyTimes()

		body, err := driver.pollAsyncOperation(context.Background(), server.URL)
		require.NoError(t, err)
		assert.Contains(t, string(body), "blobUrl")
		assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(2))
	})

	t.Run("fails on terminal status", func(t *testing.T) {
		cntl := gomock.NewController(t)
		d, err := NewFakeDriver(cntl)
		require.NoError(t, err)
		driver := d.(*fakeDriver)
		driver.pollInterval = time.Millisecond

		server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, _ *http.Request) {
			responseWriter.Header().Set("Content-Type", "application/json")
			responseWriter.WriteHeader(http.StatusOK)
			_, err := responseWriter.Write([]byte(`{"status":"Failed"}`))
			require.NoError(t, err)
		}))
		defer server.Close()

		credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
		credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil).AnyTimes()

		_, err = driver.pollAsyncOperation(context.Background(), server.URL)
		require.ErrorContains(t, err, "async operation Failed")
	})
}

func TestCreateVolume(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: " invalid ",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				d.setControllerCapabilities([]*csi.ControllerServiceCapability{})

				req := &csi.CreateVolumeRequest{}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "CREATE_DELETE_VOLUME")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: " volume name missing",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				req := &csi.CreateVolumeRequest{}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "CreateVolume Name must be provided")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "volume capabilities missing",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				req := &csi.CreateVolumeRequest{
					Name: "unit-test",
				}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "CreateVolume Volume capabilities must be provided")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "require volume size exceed",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				stdCapacityRange = &csi.CapacityRange{
					RequiredBytes: volumehelper.GiBToBytes(15),
					LimitBytes:    volumehelper.GiBToBytes(10),
				}
				req := &csi.CreateVolumeRequest{
					Name:               "unit-test",
					CapacityRange:      stdCapacityRange,
					VolumeCapabilities: createVolumeCapabilities(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
				}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "After round-up, volume size exceeds the limit specified")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "logical sector size parse error",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				mp := make(map[string]string)
				mp[consts.LogicalSectorSizeField] = "aaa"
				req := &csi.CreateVolumeRequest{
					Name:               "unit-test",
					VolumeCapabilities: createVolumeCapabilities(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
					Parameters:         mp,
				}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "Failed parsing disk parameters: parse aaa failed with error: strconv.Atoi: parsing \"aaa\": invalid syntax")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "maxshare parse error ",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				mp := make(map[string]string)
				mp[consts.MaxSharesField] = "aaa"
				req := &csi.CreateVolumeRequest{
					Name:               "unit-test",
					VolumeCapabilities: createVolumeCapabilities(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
					Parameters:         mp,
				}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "Failed parsing disk parameters: parse aaa failed with error: strconv.Atoi: parsing \"aaa\": invalid syntax")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "maxshare invalid value ",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				mp := make(map[string]string)
				mp[consts.MaxSharesField] = "0"
				req := &csi.CreateVolumeRequest{
					Name:               "unit-test",
					VolumeCapabilities: stdVolumeCapabilities,
					Parameters:         mp,
				}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "Failed parsing disk parameters: parse 0 returned with invalid value: 0")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "invalid perf profile",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				mp := make(map[string]string)
				mp[consts.PerfProfileField] = "blah"
				req := &csi.CreateVolumeRequest{
					Name:               "unit-test",
					VolumeCapabilities: stdVolumeCapabilities,
					Parameters:         mp,
				}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "Failed parsing disk parameters: perf profile blah is not supported, supported tuning modes are none and basic")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Volume capability not supported ",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				mp := make(map[string]string)
				mp[consts.MaxSharesField] = "1"
				mp[consts.SkuNameField] = "ut"
				mp[consts.LocationField] = "ut"
				mp[consts.StorageAccountTypeField] = "ut"
				mp[consts.ResourceGroupField] = "ut"
				mp[consts.DiskIOPSReadWriteField] = "1"
				mp[consts.DiskMBPSReadWriteField] = "1"
				mp[consts.DiskNameField] = "ut"
				mp[consts.DesIDField] = "ut"
				mp[consts.WriteAcceleratorEnabled] = "ut"
				req := &csi.CreateVolumeRequest{
					Name:               "unit-test",
					VolumeCapabilities: createVolumeCapabilities(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
					Parameters:         mp,
				}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "mountVolume is not supported for access mode: MULTI_NODE_MULTI_WRITER")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "normalize storageaccounttype error ",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				mp := make(map[string]string)
				mp[consts.StorageAccountTypeField] = "NOT_EXISTING"
				req := &csi.CreateVolumeRequest{
					Name:               "unit-test",
					VolumeCapabilities: createVolumeCapabilities(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
					Parameters:         mp,
				}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "azureDisk - NOT_EXISTING is not supported sku/storageaccounttype. Supported values are [Premium_LRS PremiumV2_LRS Premium_ZRS Standard_LRS StandardSSD_LRS StandardSSD_ZRS UltraSSD_LRS]")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "normalize cache mode error ",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				mp := make(map[string]string)
				mp[consts.CachingModeField] = "WriteOnly"
				req := &csi.CreateVolumeRequest{
					Name:               "unit-test",
					VolumeCapabilities: createVolumeCapabilities(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
					Parameters:         mp,
				}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "azureDisk - WriteOnly is not supported cachingmode. Supported values are [None ReadOnly ReadWrite]")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "custom tags error ",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				mp := make(map[string]string)
				mp["tags"] = "unit-test"
				req := &csi.CreateVolumeRequest{
					Name:               "unit-test",
					VolumeCapabilities: createVolumeCapabilities(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
					Parameters:         mp,
				}
				disk := &armcompute.Disk{
					Properties: &armcompute.DiskProperties{},
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "Failed parsing disk parameters: tags 'unit-test' are invalid, the format should like: 'key1=value1,key2=value2'")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "create managed disk error ",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				mp := make(map[string]string)
				mp["tags"] = "unit=test"
				volumeSnapshotSource := &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: fmt.Sprintf(diskSnapshotPath, "subs", "rg", "unit-test"),
				}
				volumeContentSourceSnapshotSource := &csi.VolumeContentSource_Snapshot{
					Snapshot: volumeSnapshotSource,
				}
				volumecontensource := csi.VolumeContentSource{
					Type: volumeContentSourceSnapshotSource,
				}
				req := &csi.CreateVolumeRequest{
					Name:                "unit-test",
					VolumeCapabilities:  createVolumeCapabilities(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
					Parameters:          mp,
					VolumeContentSource: &volumecontensource,
				}
				disk := &armcompute.Disk{
					Properties: &armcompute.DiskProperties{},
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				snapshotclient := mock_snapshotclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(snapshotclient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				diskClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("test")).AnyTimes()
				snapshotclient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&armcompute.Snapshot{
					SKU: &armcompute.SnapshotSKU{
						Name: ptr.To(armcompute.SnapshotStorageAccountTypesStandardZRS),
					},
					Properties: &armcompute.SnapshotProperties{
						DiskSizeBytes: ptr.To(int64(1073741824)), // 1GB in bytes
					},
				}, nil).AnyTimes()
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Errorf(codes.Internal, "test")
				if err.Error() != expectedErr.Error() {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "create managed disk not found error ",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				mp := make(map[string]string)
				volumeContentSourceSnapshotSource := &csi.VolumeContentSource_Snapshot{}
				volumecontensource := csi.VolumeContentSource{
					Type: volumeContentSourceSnapshotSource,
				}
				req := &csi.CreateVolumeRequest{
					Name:                "unit-test",
					VolumeCapabilities:  createVolumeCapabilities(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
					Parameters:          mp,
					VolumeContentSource: &volumecontensource,
				}
				disk := &armcompute.Disk{
					Properties: &armcompute.DiskProperties{
						DiskSizeBytes: ptr.To(int64(1073741824)), // 1GB in bytes
					},
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				diskClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf(consts.NotFound)).AnyTimes()
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.NotFound, "invalid URI: ")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "create managed disk throttled error returns Aborted",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				mp := make(map[string]string)
				req := &csi.CreateVolumeRequest{
					Name:               "unit-test",
					VolumeCapabilities: createVolumeCapabilities(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
					Parameters:         mp,
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("not found")).AnyTimes()
				diskClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, retryrepectthrottled.ErrTooManyRequest).AnyTimes()
				_, err := d.CreateVolume(context.Background(), req)
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				st, ok := status.FromError(err)
				if !ok {
					t.Fatalf("expected gRPC status error, got %v", err)
				}
				if st.Code() != codes.Aborted {
					t.Errorf("expected status code %v, got %v", codes.Aborted, st.Code())
				}
			},
		},
		{
			name: "valid request ZRS",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				mp := make(map[string]string)
				mp[consts.SkuNameField] = "StandardSSD_ZRS"
				stdCapacityRangetest := &csi.CapacityRange{
					RequiredBytes: volumehelper.GiBToBytes(10),
					LimitBytes:    volumehelper.GiBToBytes(15),
				}
				req := &csi.CreateVolumeRequest{
					Name:               testVolumeName,
					VolumeCapabilities: stdVolumeCapabilities,
					CapacityRange:      stdCapacityRangetest,
					Parameters:         mp,
				}
				size := int32(volumehelper.BytesToGiB(req.CapacityRange.RequiredBytes))
				id := fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", testVolumeName)
				state := "Succeeded"
				disk := &armcompute.Disk{
					ID:   &id,
					Name: &testVolumeName,
					Properties: &armcompute.DiskProperties{
						DiskSizeGB:        &size,
						ProvisioningState: &state,
					},
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				diskClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := error(nil)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "valid request",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				stdCapacityRangetest := &csi.CapacityRange{
					RequiredBytes: volumehelper.GiBToBytes(10),
					LimitBytes:    volumehelper.GiBToBytes(15),
				}
				req := &csi.CreateVolumeRequest{
					Name:               testVolumeName,
					VolumeCapabilities: stdVolumeCapabilities,
					CapacityRange:      stdCapacityRangetest,
				}
				size := int32(volumehelper.BytesToGiB(req.CapacityRange.RequiredBytes))
				id := fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", testVolumeName)
				state := "Succeeded"
				disk := &armcompute.Disk{
					ID:   &id,
					Name: &testVolumeName,
					Properties: &armcompute.DiskProperties{
						DiskSizeGB:        &size,
						ProvisioningState: &state,
					},
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				diskClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := error(nil)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "invalid parameter",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				stdCapacityRangetest := &csi.CapacityRange{
					RequiredBytes: volumehelper.GiBToBytes(10),
					LimitBytes:    volumehelper.GiBToBytes(15),
				}
				req := &csi.CreateVolumeRequest{
					Name:               testVolumeName,
					VolumeCapabilities: stdVolumeCapabilities,
					CapacityRange:      stdCapacityRangetest,
					Parameters:         map[string]string{"invalidparameter": "value"},
				}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "Failed parsing disk parameters: invalid parameter invalidparameter in storage class")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "[Failure] advanced perfProfile fails if no device settings provided",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				d.setPerfOptimizationEnabled(true)
				stdCapacityRangetest := &csi.CapacityRange{
					RequiredBytes: volumehelper.GiBToBytes(10),
					LimitBytes:    volumehelper.GiBToBytes(15),
				}
				req := &csi.CreateVolumeRequest{
					Name:               testVolumeName,
					VolumeCapabilities: stdVolumeCapabilities,
					CapacityRange:      stdCapacityRangetest,
					Parameters:         map[string]string{"perfProfile": "advanced"},
				}
				_, err := d.CreateVolume(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "AreDeviceSettingsValid: No deviceSettings passed")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "valid PerformancePlus request, disk resizes to min required size",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				stdCapacityRangetest := &csi.CapacityRange{
					RequiredBytes: volumehelper.GiBToBytes(10),
					LimitBytes:    volumehelper.GiBToBytes(514),
				}
				req := &csi.CreateVolumeRequest{
					Name:               testVolumeName,
					VolumeCapabilities: stdVolumeCapabilities,
					CapacityRange:      stdCapacityRangetest,
					Parameters:         map[string]string{consts.PerformancePlusField: "true"},
				}
				size := int32(volumehelper.BytesToGiB(req.CapacityRange.RequiredBytes))
				id := fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", testVolumeName)
				state := "Succeeded"
				disk := &armcompute.Disk{
					ID:   &id,
					Name: &testVolumeName,
					Properties: &armcompute.DiskProperties{
						DiskSizeGB:        &size,
						ProvisioningState: &state,
					},
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				diskClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				res, err := d.CreateVolume(context.Background(), req)
				assert.Equal(t, res.Volume.CapacityBytes, volumehelper.GiBToBytes(consts.PerformancePlusMinimumDiskSizeGiB))
				expectedErr := error(nil)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "valid disk created with custom parameters",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				stdCapacityRangetest := &csi.CapacityRange{
					RequiredBytes: volumehelper.GiBToBytes(10),
					LimitBytes:    volumehelper.GiBToBytes(514),
				}
				req := &csi.CreateVolumeRequest{
					Name:               testVolumeName,
					VolumeCapabilities: stdVolumeCapabilities,
					CapacityRange:      stdCapacityRangetest,
					Parameters: map[string]string{
						consts.SkuNameField:           string(armcompute.DiskStorageAccountTypesPremiumV2LRS),
						consts.DiskIOPSReadWriteField: "3000",
						consts.DiskMBPSReadWriteField: "125",
					},
					MutableParameters: map[string]string{
						consts.DiskIOPSReadWriteField: "5000",
						consts.DiskMBPSReadWriteField: "300",
					},
				}
				size := int32(volumehelper.BytesToGiB(req.CapacityRange.RequiredBytes))
				id := fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", testVolumeName)
				state := "Succeeded"
				disk := &armcompute.Disk{
					ID:   &id,
					Name: &testVolumeName,
					Properties: &armcompute.DiskProperties{
						DiskSizeGB:        &size,
						ProvisioningState: &state,
					},
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				diskClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				res, err := d.CreateVolume(context.Background(), req)
				expectedVolumeContext := map[string]string{
					consts.CachingModeField:       "None",
					consts.SkuNameField:           string(armcompute.DiskStorageAccountTypesPremiumV2LRS),
					consts.DiskIOPSReadWriteField: "5000",
					consts.DiskMBPSReadWriteField: "300",
					consts.RequestedSizeGib:       "10",
				}
				if !reflect.DeepEqual(expectedVolumeContext, res.Volume.GetVolumeContext()) {
					t.Errorf("actualVolumeContext: (%v), expectedVolumeContext: (%v)", res.Volume.GetVolumeContext(), expectedVolumeContext)
				}
				expectedErr := error(nil)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "NodeDriven rejected with FailedPrecondition when feature gate disabled",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				driver := d.(*fakeDriver)
				driver.nodeDrivenAttachDetachEnabled = false
				req := &csi.CreateVolumeRequest{
					Name:               testVolumeName,
					VolumeCapabilities: stdVolumeCapabilities,
					Parameters:         map[string]string{consts.AttachModeField: consts.AttachModeNodeDriven},
				}
				_, err := d.CreateVolume(context.Background(), req)
				require.Error(t, err)
				assert.Equal(t, codes.FailedPrecondition, status.Code(err))
				assert.Contains(t, err.Error(), consts.AttachModeNodeDriven)
				assert.Contains(t, err.Error(), NodeDrivenAttachDetach)
			},
		},
		{
			name: "NodeDriven proceeds and claims disk when feature gate enabled",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				driver := d.(*fakeDriver)
				driver.nodeDrivenAttachDetachEnabled = true
				driver.cloud.SubscriptionID = "subscription"
				driver.cloud.ResourceGroup = "node-resource-group"

				stdCapacityRangetest := &csi.CapacityRange{
					RequiredBytes: volumehelper.GiBToBytes(diskCachingLimit),
					LimitBytes:    volumehelper.GiBToBytes(diskCachingLimit),
				}
				req := &csi.CreateVolumeRequest{
					Name:               testVolumeName,
					VolumeCapabilities: stdVolumeCapabilities,
					CapacityRange:      stdCapacityRangetest,
					Parameters:         map[string]string{consts.AttachModeField: consts.AttachModeNodeDriven},
				}
				size := int32(volumehelper.BytesToGiB(req.CapacityRange.RequiredBytes))
				id := fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", testVolumeName)
				state := "Succeeded"
				disk := &armcompute.Disk{
					ID:   &id,
					Name: &testVolumeName,
					Properties: &armcompute.DiskProperties{
						DiskSizeGB:        &size,
						ProvisioningState: &state,
					},
				}
				clientFactory := driver.clientFactory.(*mock_azclient.MockClientFactory)
				diskClient := mock_diskclient.NewMockInterface(cntl)
				clientFactory.EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				diskClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()

				// getAKSClusterResourceID resolves the QAD owner from node RG tags.
				resourceGroupClient := mock_resourcegroupclient.NewMockInterface(cntl)
				clientFactory.EXPECT().GetResourceGroupClient().Return(resourceGroupClient)
				resourceGroupClient.EXPECT().Get(gomock.Any(), "node-resource-group").Return(&armresources.ResourceGroup{
					Tags: map[string]*string{
						aksManagedClusterNameTag:          to.Ptr("cluster"),
						aksManagedClusterResourceGroupTag: to.Ptr("cluster-resource-group"),
					},
				}, nil)

				// claimDiskResource POSTs to the DiskRP claimResource endpoint.
				blobURL := "https://md-storage.blob.core.windows.net/container/disk"
				server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, _ *http.Request) {
					responseWriter.Header().Set("Content-Type", "application/json")
					responseWriter.WriteHeader(http.StatusOK)
					_, _ = fmt.Fprintf(responseWriter, `{"properties":{"blobUrl":%q,"claimIdentifier":"identifier"}}`, blobURL)
				}))
				defer server.Close()
				driver.cloud.ARMClientConfig.ResourceManagerEndpoint = server.URL
				credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
				credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil)

				res, err := d.CreateVolume(context.Background(), req)
				require.NoError(t, err)
				require.NotNil(t, res)
				assert.Equal(t, blobURL, res.Volume.VolumeContext[consts.BlobURLAnnotation])
				assert.Equal(t, "identifier", res.Volume.VolumeContext[consts.ClaimIdentifierAnnotation])
				assert.Equal(t, "None", res.Volume.VolumeContext[consts.CachingModeField])
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestCreateVolumeRetriesIncompleteQADClaimWithoutUnclaim(t *testing.T) {
	cntl := gomock.NewController(t)
	d, err := NewFakeDriver(cntl)
	require.NoError(t, err)
	driver := d.(*fakeDriver)
	driver.nodeDrivenAttachDetachEnabled = true
	driver.cloud.SubscriptionID = "subscription"
	driver.cloud.ResourceGroup = "node-resource-group"

	req := &csi.CreateVolumeRequest{
		Name:               testVolumeName,
		VolumeCapabilities: stdVolumeCapabilities,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: volumehelper.GiBToBytes(10),
			LimitBytes:    volumehelper.GiBToBytes(514),
		},
		Parameters: map[string]string{consts.AttachModeField: consts.AttachModeNodeDriven},
	}
	size := int32(volumehelper.BytesToGiB(req.CapacityRange.RequiredBytes))
	diskURI := fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", testVolumeName)
	state := "Succeeded"
	disk := &armcompute.Disk{
		ID:   &diskURI,
		Name: &testVolumeName,
		Properties: &armcompute.DiskProperties{
			DiskSizeGB:        &size,
			ProvisioningState: &state,
		},
	}

	clientFactory := driver.clientFactory.(*mock_azclient.MockClientFactory)
	diskClient := mock_diskclient.NewMockInterface(cntl)
	clientFactory.EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
	diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
	diskClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()

	resourceGroupClient := mock_resourcegroupclient.NewMockInterface(cntl)
	clientFactory.EXPECT().GetResourceGroupClient().Return(resourceGroupClient)
	resourceGroupClient.EXPECT().Get(gomock.Any(), "node-resource-group").Return(&armresources.ResourceGroup{
		Tags: map[string]*string{
			aksManagedClusterNameTag:          to.Ptr("cluster"),
			aksManagedClusterResourceGroupTag: to.Ptr("cluster-resource-group"),
		},
	}, nil)

	var claimRequests atomic.Int32
	var unclaimed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/claimResource"):
			responseWriter.WriteHeader(http.StatusOK)
			if claimRequests.Add(1) == 1 {
				_, _ = responseWriter.Write([]byte(`{"properties":{"claimIdentifier":"identifier"}}`))
				return
			}
			_, _ = responseWriter.Write([]byte(`{"properties":{"blobUrl":"https://md-storage.blob.core.windows.net/container/disk","claimIdentifier":"identifier"}}`))
		case strings.HasSuffix(request.URL.Path, "/unclaimResource"):
			unclaimed.Store(true)
			responseWriter.WriteHeader(http.StatusOK)
		default:
			http.NotFound(responseWriter, request)
		}
	}))
	defer server.Close()
	driver.cloud.ARMClientConfig.ResourceManagerEndpoint = server.URL
	credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
	credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil).Times(2)

	response, err := driver.CreateVolume(context.Background(), req)

	require.Nil(t, response)
	require.Equal(t, codes.Aborted, status.Code(err))
	assert.ErrorContains(t, err, "QAD blob URL missing")
	assert.False(t, unclaimed.Load(), "an ambiguous claim must remain intact for retry")

	response, err = driver.CreateVolume(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Equal(t, "https://md-storage.blob.core.windows.net/container/disk", response.Volume.VolumeContext[consts.BlobURLAnnotation])
	assert.Equal(t, "identifier", response.Volume.VolumeContext[consts.ClaimIdentifierAnnotation])
	assert.Equal(t, int32(2), claimRequests.Load())
	assert.False(t, unclaimed.Load())
}

func TestCreateVolumeRetriesQADOwnerLookupFailure(t *testing.T) {
	cntl := gomock.NewController(t)
	d, err := NewFakeDriver(cntl)
	require.NoError(t, err)
	driver := d.(*fakeDriver)
	driver.nodeDrivenAttachDetachEnabled = true
	driver.cloud.SubscriptionID = "subscription"
	driver.cloud.ResourceGroup = "node-resource-group"

	req := &csi.CreateVolumeRequest{
		Name:               testVolumeName,
		VolumeCapabilities: stdVolumeCapabilities,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: volumehelper.GiBToBytes(10),
			LimitBytes:    volumehelper.GiBToBytes(514),
		},
		Parameters: map[string]string{consts.AttachModeField: consts.AttachModeNodeDriven},
	}
	size := int32(volumehelper.BytesToGiB(req.CapacityRange.RequiredBytes))
	diskURI := fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", testVolumeName)
	state := "Succeeded"
	disk := &armcompute.Disk{
		ID:   &diskURI,
		Name: &testVolumeName,
		Properties: &armcompute.DiskProperties{
			DiskSizeGB:        &size,
			ProvisioningState: &state,
		},
	}

	clientFactory := driver.clientFactory.(*mock_azclient.MockClientFactory)
	diskClient := mock_diskclient.NewMockInterface(cntl)
	clientFactory.EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
	diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
	diskClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil)

	resourceGroupClient := mock_resourcegroupclient.NewMockInterface(cntl)
	clientFactory.EXPECT().GetResourceGroupClient().Return(resourceGroupClient)
	resourceGroupClient.EXPECT().Get(gomock.Any(), "node-resource-group").Return(nil, errors.New("resource group unavailable"))

	response, err := driver.CreateVolume(context.Background(), req)

	require.Nil(t, response)
	assert.Equal(t, codes.Aborted, status.Code(err))
	assert.ErrorContains(t, err, "failed to determine QAD owner resource")
}

func TestCreateVolume_SnapshotPremiumLRS_ToPremiumV2_EmitsMigrationEvents(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()

	d := getFakeDriverWithKubeClient(cntl)

	// Inject mock kube client for migration monitor
	mockKube := d.getCloud().KubeClient
	coreMock := d.getCloud().KubeClient.(*mockkubeclient.MockInterface)
	pvMock := d.getCloud().KubeClient.CoreV1().PersistentVolumes().(*mockpersistentvolume.MockInterface)
	pvcMock := mockpersistentvolumeclaim.NewMockPersistentVolumeClaimInterface(cntl)

	d.getCloud().KubeClient = mockKube
	eventRecorder := record.NewFakeRecorder(100)
	d.SetMigrationMonitor(NewMigrationProgressMonitor(d.getCloud().KubeClient, eventRecorder, d.GetDiskController()))

	// Speed up polling
	origInterval := migrationCheckInterval
	migrationCheckInterval = 30 * time.Millisecond
	defer func() { migrationCheckInterval = origInterval }()

	// Mock k8s PV/PVC lookup
	coreMock.CoreV1().(*mockcorev1.MockInterface).EXPECT().PersistentVolumeClaims(gomock.Any()).Return(pvcMock).AnyTimes()

	pvName := testVolumeName
	pvcName := "test-pvc"
	storageQty := resource.MustParse("1Gi")
	pvObj := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: v1.PersistentVolumeSpec{
			ClaimRef: &v1.ObjectReference{Name: pvcName, Namespace: "default"},
			Capacity: v1.ResourceList{v1.ResourceStorage: storageQty},
		},
	}
	pvcObj := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: "default"},
		Spec:       v1.PersistentVolumeClaimSpec{VolumeName: pvName},
	}

	pvMock.EXPECT().Get(gomock.Any(), pvName, gomock.Any()).Return(pvObj.DeepCopy(), nil).AnyTimes()
	// Expect label update once (may be more; allow AnyTimes)
	pvMock.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, in *v1.PersistentVolume, _ metav1.UpdateOptions) (*v1.PersistentVolume, error) {
			if in.Labels == nil {
				in.Labels = map[string]string{}
			}
			in.Labels[LabelMigrationInProgress] = "true"
			return in, nil
		}).AnyTimes()
	pvcMock.EXPECT().Get(gomock.Any(), pvcName, gomock.Any()).Return(pvcObj.DeepCopy(), nil).AnyTimes()

	// Mock snapshot (source Premium_LRS)
	snapshotClient := mock_snapshotclient.NewMockInterface(cntl)
	d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(snapshotClient, nil).AnyTimes()
	snapID := "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/snapshots/src-snap"
	srcSKU := armcompute.SnapshotStorageAccountTypesPremiumLRS
	sizeGB := int32(1)
	provState := "Succeeded"
	snapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&armcompute.Snapshot{
		ID: &snapID,
		SKU: &armcompute.SnapshotSKU{
			Name: &srcSKU,
		},
		Properties: &armcompute.SnapshotProperties{
			DiskSizeGB:        &sizeGB,
			DiskSizeBytes:     ptr.To(int64(1073741824)), // 1GB in bytes
			ProvisioningState: &provState,
		},
	}, nil).AnyTimes()

	// Mock disk client for new PremiumV2 disk + progress polling
	diskClient := mock_diskclient.NewMockInterface(cntl)
	d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()

	newDiskID := fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", testVolumeName)
	newDiskName := testVolumeName
	percent := float32(0)
	state := "Succeeded"

	// CreateOrUpdate returns initial disk
	diskClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _ string, _ armcompute.Disk) (*armcompute.Disk, error) {
			return &armcompute.Disk{
				ID:   &newDiskID,
				Name: &newDiskName,
				SKU:  &armcompute.DiskSKU{Name: ptr.To(armcompute.DiskStorageAccountTypesPremiumV2LRS)},
				Properties: &armcompute.DiskProperties{
					DiskSizeGB:        &sizeGB,
					ProvisioningState: &state,
					CompletionPercent: &percent,
				},
			}, nil
		}).Times(1)

	// Get simulates migration progress milestones 0->25->45->65->85->100
	diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _ string) (*armcompute.Disk, error) {
			if percent < 100 {
				switch {
				case percent < 25:
					percent = 25
				case percent < 45:
					percent = 45
				case percent < 65:
					percent = 65
				case percent < 85:
					percent = 85
				default:
					percent = 100
				}
			}
			return &armcompute.Disk{
				ID:   &newDiskID,
				Name: &newDiskName,
				SKU:  &armcompute.DiskSKU{Name: ptr.To(armcompute.DiskStorageAccountTypesPremiumV2LRS)},
				Properties: &armcompute.DiskProperties{
					DiskSizeGB:        &sizeGB,
					ProvisioningState: &state,
					CompletionPercent: &percent,
				},
			}, nil
		}).AnyTimes()

	req := &csi.CreateVolumeRequest{
		Name:               testVolumeName,
		VolumeCapabilities: stdVolumeCapabilities,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: volumehelper.GiBToBytes(1),
			LimitBytes:    volumehelper.GiBToBytes(1),
		},
		Parameters: map[string]string{
			consts.SkuNameField: string(armcompute.DiskStorageAccountTypesPremiumV2LRS),
		},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: snapID,
				},
			},
		},
	}

	_, err := d.CreateVolume(context.Background(), req)
	assert.NoError(t, err)

	// Wait for monitor to emit events
	time.Sleep(500 * time.Millisecond)

	// Drain events
	var events []string
collect:
	for {
		select {
		case e := <-eventRecorder.Events:
			events = append(events, e)
		default:
			break collect
		}
	}

	hasStart := false
	hasProgress := false
	hasCompleted := false
	for _, e := range events {
		if strings.Contains(e, ReasonSKUMigrationStarted) {
			hasStart = true
		}
		if strings.Contains(e, ReasonSKUMigrationProgress) {
			hasProgress = true
		}
		if strings.Contains(e, ReasonSKUMigrationCompleted) {
			hasCompleted = true
		}
	}

	if !hasStart || !hasProgress || !hasCompleted {
		t.Fatalf("expected start+progress+completed events, got: %v", events)
	}

	d.GetMigrationMonitor().Stop()
}

func TestCreateVolume_RecoveryLoopStartsMigrationAfterInitialMonitorUnavailable(t *testing.T) {
	origInterval := migrationCheckInterval
	defer func() { migrationCheckInterval = origInterval }()
	migrationCheckInterval = 25 * time.Millisecond

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	d := getFakeDriverWithKubeClient(ctrl)
	// Simulate transient unavailability – monitor not ready at provisioning time
	d.SetMigrationMonitor(nil)

	snapshotID := "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/snapshots/snap-recov1"
	volName := "pv-recover-transient-start"
	sizeGi := int64(10)
	capBytes := sizeGi * 1024 * 1024 * 1024
	diskURI := fmt.Sprintf("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/%s", volName)

	// Mock disk & snapshot clients
	diskClient := mock_diskclient.NewMockInterface(ctrl)
	d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().
		GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()

	snapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
	d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().
		GetSnapshotClientForSub(gomock.Any()).
		Return(snapshotClient, nil).AnyTimes()

	// Snapshot SKU (source = Premium_LRS)
	snapshotClient.EXPECT().Get(gomock.Any(), "rg", "snap-recov1").Return(
		&armcompute.Snapshot{
			Name: to.Ptr("snap-recov1"),
			SKU:  &armcompute.SnapshotSKU{Name: to.Ptr(armcompute.SnapshotStorageAccountTypesPremiumLRS)},
			Properties: &armcompute.SnapshotProperties{
				CreationData: &armcompute.CreationData{},
			},
		}, nil).AnyTimes()

	// Track creation & progress
	var createDone atomic.Bool
	var progressPolls atomic.Int32

	// GET behavior:
	//  - Before CreateOrUpdate: return NotFound
	//  - After CreateOrUpdate: return disk with ProvisioningState=Succeeded and evolving CompletionPercent
	diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ string) (*armcompute.Disk, error) {
			if !createDone.Load() {
				// Simulate disk not existing yet
				return nil, apierrors.NewInternalError(fmt.Errorf("transient error"))
			}
			p := progressPolls.Add(1)
			var pct float32
			if p >= 5 {
				pct = 100
			} else {
				pct = 0
			}
			return &armcompute.Disk{
				ID:  to.Ptr(diskURI),
				SKU: &armcompute.DiskSKU{Name: to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS)},
				Properties: &armcompute.DiskProperties{
					DiskSizeGB:        to.Ptr[int32](int32(sizeGi)),
					ProvisioningState: to.Ptr("Succeeded"),
					CompletionPercent: to.Ptr(pct),
				},
			}, nil
		}).AnyTimes()

	// CreateOrUpdate sets createDone
	diskClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ string, _ armcompute.Disk) (*armcompute.Disk, error) {
			return &armcompute.Disk{
				ID:  to.Ptr(diskURI),
				SKU: &armcompute.DiskSKU{Name: to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS)},
				Properties: &armcompute.DiskProperties{
					DiskSizeGB:        to.Ptr[int32](int32(sizeGi)),
					ProvisioningState: to.Ptr("Succeeded"),
				},
			}, nil
		}).Times(1)

	// Patch used during migration monitor after recovery
	diskClient.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&armcompute.Disk{
			ID:  to.Ptr(diskURI),
			SKU: &armcompute.DiskSKU{Name: to.Ptr(armcompute.DiskStorageAccountTypesPremiumV2LRS)},
		}, nil).AnyTimes()

	req := &csi.CreateVolumeRequest{
		Name: volName,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: capBytes,
		},
		Parameters: map[string]string{
			consts.SkuNameField: string(armcompute.DiskStorageAccountTypesPremiumV2LRS),
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: snapshotID,
				},
			},
		},
	}

	_, err := d.CreateVolume(context.Background(), req)
	assert.NoError(t, err, "CreateVolume should succeed")
	assert.True(t, d.GetMigrationMonitor().IsMigrationActive(diskURI), "Migration for volume %s should be active", diskURI)

	// Simulate restart
	d.GetMigrationMonitor().Stop()
	// Ensure cleanup (task removed)
	cleanupDeadline := time.Now().Add(1 * time.Second)
	for {
		if !d.GetMigrationMonitor().IsMigrationActive(diskURI) {
			break
		}
		if time.Now().After(cleanupDeadline) {
			t.Fatalf("Migration task still active after completion")
		}
		time.Sleep(25 * time.Millisecond)
	}
	createDone.Store(true)

	eventRecorder := record.NewFakeRecorder(100)
	d.SetMigrationMonitor(NewMigrationProgressMonitor(d.getCloud().KubeClient, eventRecorder, d.GetDiskController()))

	// Prepare labeled PV/PVC for recovery
	coreMock := d.getCloud().KubeClient.CoreV1().(*mockcorev1.MockInterface)
	pvIf := d.getCloud().KubeClient.CoreV1().PersistentVolumes().(*mockpersistentvolume.MockInterface)
	pvcIf := mockpersistentvolumeclaim.NewMockPersistentVolumeClaimInterface(ctrl)
	coreMock.EXPECT().PersistentVolumeClaims(gomock.Any()).Return(pvcIf).AnyTimes()

	pvcName := "pvc-" + volName
	ns := "default"

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volName,
			Labels: map[string]string{
				LabelMigrationInProgress: "true",
			},
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName("storage"): *resource.NewQuantity(capBytes, resource.BinarySI),
			},
			ClaimRef: &v1.ObjectReference{Name: pvcName, Namespace: ns},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       "disk.csi.azure.com",
					VolumeHandle: diskURI,
					VolumeAttributes: map[string]string{
						"storageAccountType": string(armcompute.DiskStorageAccountTypesPremiumV2LRS),
						"skuName":            string(armcompute.DiskStorageAccountTypesPremiumV2LRS),
					},
				},
			},
		},
	}
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: ns},
		Spec:       v1.PersistentVolumeClaimSpec{VolumeName: pv.Name},
	}

	pvIf.EXPECT().List(gomock.Any(), gomock.Any()).
		Return(&v1.PersistentVolumeList{Items: []v1.PersistentVolume{*pv}}, nil).AnyTimes()
	pvIf.EXPECT().Get(gomock.Any(), pv.Name, gomock.Any()).
		Return(pv.DeepCopy(), nil).AnyTimes()
	pvIf.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, updated *v1.PersistentVolume, _ metav1.UpdateOptions) (*v1.PersistentVolume, error) {
			return updated, nil
		}).AnyTimes()
	pvcIf.EXPECT().Get(gomock.Any(), pvc.Name, gomock.Any()).
		Return(pvc.DeepCopy(), nil).AnyTimes()

	// Accelerated recovery loop (instead of 30s + 10m)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_ = d.RecoverMigrationMonitor(context.Background())
				time.Sleep(40 * time.Millisecond)
			}
		}
	}()

	// Wait for migration activation
	activateDeadline := time.Now().Add(2 * time.Second)
	for {
		if d.GetMigrationMonitor().IsMigrationActive(diskURI) {
			break
		}
		if time.Now().After(activateDeadline) {
			t.Fatalf("Migration did not activate via recovery")
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Collect events until Started & Completed
	foundStart, foundCompletion := false, false
	timeout := time.After(6 * time.Second)
	for !(foundStart && foundCompletion) {
		select {
		case e := <-eventRecorder.Events:
			if strings.Contains(e, ReasonSKUMigrationStarted) {
				foundStart = true
			}
			if strings.Contains(e, ReasonSKUMigrationCompleted) {
				foundCompletion = true
			}
		case <-timeout:
			t.Fatalf("Timed out waiting for migration start/completion (start=%v completion=%v)", foundStart, foundCompletion)
		case <-time.After(30 * time.Millisecond):
		}
	}

	assert.True(t, foundStart, "Expected migration start event")
	assert.True(t, foundCompletion, "Expected migration completion event")

	// Ensure cleanup (task removed)
	cleanupDeadline = time.Now().Add(1 * time.Second)
	for {
		if !d.GetMigrationMonitor().IsMigrationActive(diskURI) {
			break
		}
		if time.Now().After(cleanupDeadline) {
			t.Fatalf("Migration task still active after completion")
		}
		time.Sleep(25 * time.Millisecond)
	}

	d.GetMigrationMonitor().Stop()
}

func TestDeleteVolume(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, err := NewFakeDriver(cntl)
	if err != nil {
		t.Fatalf("Error getting driver: %v", err)
	}
	driver := d.(*fakeDriver)
	_, err = driver.kubeClient.CoreV1().PersistentVolumes().Create(context.Background(), &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "standard-pv"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       driver.Name,
					VolumeHandle: testVolumeID,
				},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	tests := []struct {
		desc            string
		req             *csi.DeleteVolumeRequest
		expectedResp    *csi.DeleteVolumeResponse
		expectedErrCode codes.Code
	}{
		{
			desc: "success standard",
			req: &csi.DeleteVolumeRequest{
				VolumeId: testVolumeID,
			},
			expectedResp: &csi.DeleteVolumeResponse{},
		},
		{
			desc: "fail with no volume id",
			req: &csi.DeleteVolumeRequest{
				VolumeId: "",
			},
			expectedResp:    nil,
			expectedErrCode: codes.InvalidArgument,
		},
		{
			desc: "fail with the invalid diskURI",
			req: &csi.DeleteVolumeRequest{
				VolumeId: "123",
			},
			expectedResp: &csi.DeleteVolumeResponse{},
		},
	}

	for _, test := range tests {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		id := test.req.VolumeId
		disk := &armcompute.Disk{
			ID: &id,
		}
		diskClient := mock_diskclient.NewMockInterface(cntl)
		d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
		diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
		diskClient.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		result, err := d.DeleteVolume(ctx, test.req)
		if err != nil {
			checkTestError(t, test.expectedErrCode, err)
		}
		if !reflect.DeepEqual(result, test.expectedResp) {
			t.Errorf("input request: %v, DeleteVolume result: %v, expected: %v", test.req, result, test.expectedResp)
		}
	}
}

func TestDeleteVolumePVLookup(t *testing.T) {
	tests := []struct {
		desc              string
		pvListErr         error
		expectDelete      bool
		expectedErrorCode codes.Code
	}{
		{
			desc:         "deletes disk when PV is missing",
			expectDelete: true,
		},
		{
			desc:              "fails closed when PV list fails",
			pvListErr:         errors.New("Kubernetes API unavailable"),
			expectedErrorCode: codes.Internal,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			cntl := gomock.NewController(t)
			d, err := NewFakeDriver(cntl)
			require.NoError(t, err)
			driver := d.(*fakeDriver)
			driver.nodeDrivenAttachDetachEnabled = true

			if test.pvListErr != nil {
				driver.kubeClient.(*fake.Clientset).PrependReactor("list", "persistentvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, test.pvListErr
				})
			}

			diskClient := mock_diskclient.NewMockInterface(cntl)
			d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
			if test.expectDelete {
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&armcompute.Disk{}, nil)
				diskClient.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			}

			result, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: testVolumeID})
			if test.expectedErrorCode != codes.OK {
				require.Equal(t, test.expectedErrorCode, status.Code(err))
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.Equal(t, &csi.DeleteVolumeResponse{}, result)
		})
	}
}

func TestDeleteVolumeUnclaimsAKSManagedDiskAfterDeleteFailure(t *testing.T) {
	cntl := gomock.NewController(t)
	d, err := NewFakeDriver(cntl)
	require.NoError(t, err)
	driver := d.(*fakeDriver)

	ownerResourceID := "/subscriptions/subscription/resourceGroups/cluster-rg/providers/Microsoft.ContainerService/managedClusters/cluster"
	driver.clusterResourceID = ownerResourceID
	server := httptest.NewServer(http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		assert.Equal(t, http.MethodPost, request.Method)
		assert.Equal(t, testVolumeID+"/unclaimResource", request.URL.Path)

		var body struct {
			OwnerResourceID string `json:"ownerResourceId"`
		}
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		assert.Equal(t, ownerResourceID, body.OwnerResourceID)
		responseWriter.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	driver.cloud.ARMClientConfig.ResourceManagerEndpoint = server.URL

	credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
	credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil)

	diskClient := mock_diskclient.NewMockInterface(cntl)
	driver.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
	diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&armcompute.Disk{
		ManagedBy: to.Ptr(strings.ToUpper(ownerResourceID)),
	}, nil).Times(1)

	result, err := d.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: testVolumeID})
	require.ErrorContains(t, err, "is managed by Azure resource")
	require.Equal(t, &csi.DeleteVolumeResponse{}, result)
}

func TestControllerGetVolume(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, err := NewFakeDriver(cntl)
	if err != nil {
		t.Fatalf("Error getting driver: %v", err)
	}
	req := csi.ControllerGetVolumeRequest{}
	resp, err := d.ControllerGetVolume(context.Background(), &req)
	assert.Nil(t, resp)
	if !reflect.DeepEqual(err, status.Error(codes.Unimplemented, "")) {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestControllerModifyVolume(t *testing.T) {
	tests := []struct {
		desc                                    string
		req                                     *csi.ControllerModifyVolumeRequest
		oldSKU                                  *armcompute.DiskStorageAccountTypes
		expectedResp                            *csi.ControllerModifyVolumeResponse
		expectedErrCode                         codes.Code
		expectedErrmsg                          string
		expectMigrationStarted                  bool
		setupPVCMocks                           bool
		simulateRestart                         bool
		pvcExists                               bool
		pvHasMigrationLabels                    bool
		multipleMigrationsToRecover             bool
		simulateMigrationCompletionAfterRestart bool
	}{
		{
			desc: "success standard",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: testVolumeID,
				MutableParameters: map[string]string{
					consts.DiskIOPSReadWriteField: "100",
					consts.DiskMBPSReadWriteField: "100",
				},
			},
			oldSKU:                 to.Ptr(armcompute.DiskStorageAccountTypesUltraSSDLRS),
			expectedResp:           &csi.ControllerModifyVolumeResponse{},
			expectMigrationStarted: false,
		},
		{
			desc: "fail with no volume id",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: "",
			},
			expectedResp:           nil,
			expectedErrCode:        codes.InvalidArgument,
			expectMigrationStarted: false,
		},
		{
			desc: "fail with the invalid diskURI",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: "123",
			},
			expectedResp:           nil,
			expectedErrCode:        codes.NotFound,
			expectMigrationStarted: false,
		},
		{
			desc: "fail with wrong disk name",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: "/subscriptions/123",
			},
			expectedResp:           nil,
			expectedErrCode:        codes.NotFound,
			expectMigrationStarted: false,
		},
		{
			desc: "fail with wrong sku name",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: testVolumeID,
				MutableParameters: map[string]string{
					consts.SkuNameField: "ut",
				},
			},
			expectedResp:           nil,
			expectedErrCode:        codes.InvalidArgument,
			expectMigrationStarted: false,
		},
		{
			desc: "fail with error parse parameter",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: testVolumeID,
				MutableParameters: map[string]string{
					consts.DiskIOPSReadWriteField: "ut",
				},
			},
			expectedResp:           nil,
			expectedErrCode:        codes.InvalidArgument,
			expectMigrationStarted: false,
		},
		{
			desc: "fail with unsupported sku",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: testVolumeID,
				MutableParameters: map[string]string{
					consts.SkuNameField:           "Premium_LRS",
					consts.DiskIOPSReadWriteField: "100",
				},
			},
			expectedResp:           nil,
			expectedErrCode:        codes.Internal,
			expectMigrationStarted: false,
		},
		{
			desc: "success SKU migration from Premium_LRS to PremiumV2_LRS",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: testVolumeID,
				MutableParameters: map[string]string{
					consts.SkuNameField: string(armcompute.DiskStorageAccountTypesPremiumV2LRS),
				},
			},
			oldSKU:                 to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS),
			expectedResp:           &csi.ControllerModifyVolumeResponse{},
			expectMigrationStarted: true,
			pvcExists:              true,
			setupPVCMocks:          true,
		},
		{
			desc: "Migration monitor not triggered for Standard_LRS to Premium_LRS",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: testVolumeID,
				MutableParameters: map[string]string{
					consts.SkuNameField: string(armcompute.DiskStorageAccountTypesPremiumLRS),
				},
			},
			oldSKU:                 to.Ptr(armcompute.DiskStorageAccountTypesStandardLRS),
			expectedResp:           &csi.ControllerModifyVolumeResponse{},
			expectMigrationStarted: false,
			setupPVCMocks:          true,
		},
		{
			desc: "no migration for same SKU",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: testVolumeID,
				MutableParameters: map[string]string{
					consts.SkuNameField: string(armcompute.DiskStorageAccountTypesPremiumLRS),
				},
			},
			oldSKU:                 to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS),
			expectedResp:           &csi.ControllerModifyVolumeResponse{},
			expectMigrationStarted: false,
		},
		{
			desc: "controller restart - recover ongoing migration from annotations",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: testVolumeID,
				MutableParameters: map[string]string{
					consts.SkuNameField: string(armcompute.DiskStorageAccountTypesPremiumV2LRS),
				},
			},
			oldSKU:                 to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS),
			expectedResp:           &csi.ControllerModifyVolumeResponse{},
			expectMigrationStarted: true,
			setupPVCMocks:          true,
			pvcExists:              true,
			simulateRestart:        true,
			pvHasMigrationLabels:   true,
		},
		{
			desc: "controller restart - no migration to recover",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: testVolumeID,
				MutableParameters: map[string]string{
					consts.DiskIOPSReadWriteField: "3000",
				},
			},
			oldSKU:                 to.Ptr(armcompute.DiskStorageAccountTypesUltraSSDLRS),
			expectedResp:           &csi.ControllerModifyVolumeResponse{},
			expectMigrationStarted: false,
			setupPVCMocks:          true,
			pvcExists:              true,
			simulateRestart:        true,
			pvHasMigrationLabels:   false,
		},
		{
			desc: "controller restart - recover multiple ongoing migrations and cleanup on completion",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: testVolumeID,
				MutableParameters: map[string]string{
					consts.SkuNameField: string(armcompute.DiskStorageAccountTypesPremiumV2LRS),
				},
			},
			oldSKU:                                  to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS),
			expectedResp:                            &csi.ControllerModifyVolumeResponse{},
			expectMigrationStarted:                  true,
			setupPVCMocks:                           true,
			pvcExists:                               true,
			simulateRestart:                         true,
			pvHasMigrationLabels:                    true,
			multipleMigrationsToRecover:             true,
			simulateMigrationCompletionAfterRestart: true,
		},
	}

	for _, test := range tests {
		klog.Infof("Running test: %s", test.desc)

		//Cancel all existing mocking call expectations
		cntl := gomock.NewController(t)
		defer cntl.Finish()
		d := getFakeDriverWithKubeClient(cntl)
		d.(*fakeDriver).kubeClient = fake.NewClientset(&v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: testVolumeName},
			Spec: v1.PersistentVolumeSpec{
				PersistentVolumeSource: v1.PersistentVolumeSource{
					CSI: &v1.CSIPersistentVolumeSource{
						Driver:       fakeDriverName,
						VolumeHandle: test.req.VolumeId,
					},
				},
			},
		})

		// Initialize migration monitor with the fake driver's kube client
		mockEventRecorder := record.NewFakeRecorder(100)
		d.SetMigrationMonitor(NewMigrationProgressMonitor(d.getCloud().KubeClient, mockEventRecorder, d.GetDiskController()))

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		id := test.req.VolumeId
		var diskSizeInGb int64 = 10
		disk := &armcompute.Disk{
			ID: &id,
			SKU: &armcompute.DiskSKU{
				Name: test.oldSKU,
			},
			Properties: &armcompute.DiskProperties{
				DiskSizeGB: to.Ptr(int32(diskSizeInGb)),
				// A real in-progress migration reports a real (non-nil) CompletionPercent,
				// starting at 0%; a nil CompletionPercent means there's no pending copy
				// (i.e. complete), so setting this to 0 here keeps a just-started
				// migration genuinely active for the assertions below.
				CompletionPercent: to.Ptr(float32(0)),
			},
		}

		// Setup disk client mocks
		diskClient := mock_diskclient.NewMockInterface(cntl)
		d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
		diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
		diskClient.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()

		// Setup PVC mocks for migration monitoring if needed
		var testPVCs []*v1.PersistentVolumeClaim
		var testPVs []*v1.PersistentVolume
		var testPVCopies []*v1.PersistentVolume
		if test.setupPVCMocks {
			// Create primary test PVC
			testPVC := &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc-0",
					Namespace: "default",
				},
				Spec: v1.PersistentVolumeClaimSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{
						v1.PersistentVolumeAccessMode("ReadWriteOnce"),
					},
					Resources: v1.VolumeResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceName("storage"): *resource.NewQuantity(diskSizeInGb*1024*1024*1024, resource.BinarySI), // 10GiB
						},
					},
					VolumeName: testVolumeName,
				},
			}

			testPVCs = append(testPVCs, testPVC)

			// Create additional PVCs for multiple migration recovery test
			if test.multipleMigrationsToRecover {
				for i := 1; i <= 2; i++ {
					additionalPVC := &v1.PersistentVolumeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name:      fmt.Sprintf("test-pvc-%d", i),
							Namespace: "default",
						},
						Spec: v1.PersistentVolumeClaimSpec{
							AccessModes: []v1.PersistentVolumeAccessMode{
								v1.PersistentVolumeAccessMode("ReadWriteOnce"),
							},
							Resources: v1.VolumeResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceName("storage"): *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI), // 10GiB
								},
							},
							VolumeName: fmt.Sprintf("test-pv-%d", i),
						},
					}
					testPVCs = append(testPVCs, additionalPVC)
				}
			}

			// Setup mock expectations for PVC/PV operations
			pvcInterface := mockpersistentvolumeclaim.NewMockPersistentVolumeClaimInterface(cntl)
			mockCoreV1 := d.getCloud().KubeClient.CoreV1()

			// Mock List call for recovery scenarios
			pvcList := &v1.PersistentVolumeClaimList{Items: []v1.PersistentVolumeClaim{}}
			for _, pvc := range testPVCs {
				pvcList.Items = append(pvcList.Items, *pvc)

				volumeHandle := fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", pvc.Spec.VolumeName)
				pv := &v1.PersistentVolume{
					ObjectMeta: metav1.ObjectMeta{
						Name: pvc.Spec.VolumeName,
					},
					Spec: v1.PersistentVolumeSpec{
						PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
						AccessModes: []v1.PersistentVolumeAccessMode{
							v1.ReadWriteOnce,
						},
						Capacity: v1.ResourceList{
							v1.ResourceName("storage"): *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI), // 10GiB
						},
						ClaimRef: &v1.ObjectReference{
							Namespace: pvc.Namespace,
							Name:      pvc.Name,
						},
						PersistentVolumeSource: v1.PersistentVolumeSource{
							CSI: &v1.CSIPersistentVolumeSource{
								Driver:       "disk.csi.azure.com",
								VolumeHandle: volumeHandle,
							},
						},
					},
				}
				pvCopy := pv.DeepCopy()
				if test.pvHasMigrationLabels {
					if pvCopy.Labels == nil {
						pvCopy.Labels = make(map[string]string)
					}
					pvCopy.Labels[LabelMigrationInProgress] = "true"
				}
				testPVCopies = append(testPVCopies, pvCopy)
				testPVs = append(testPVs, pv)
			}
			mockCoreV1.(*mockcorev1.MockInterface).EXPECT().PersistentVolumeClaims(gomock.Any()).Return(pvcInterface).AnyTimes()

			// Mock Get and Update calls
			for _, pvc := range testPVCs {
				if test.pvcExists {
					pvcInterface.EXPECT().Get(gomock.Any(), pvc.Name, gomock.Any()).
						Return(pvc, nil).AnyTimes()

					d.getCloud().KubeClient.CoreV1().PersistentVolumes().(*mockpersistentvolume.MockInterface).EXPECT().Get(gomock.Any(), pvc.Spec.VolumeName, gomock.Any()).DoAndReturn(func(_ context.Context, name string, _ metav1.GetOptions) (*v1.PersistentVolume, error) {
						for _, pv := range testPVs {
							if pv.Name == name {
								return pv, nil
							}
						}
						return nil, errors.New("not found")
					}).AnyTimes()

					d.getCloud().KubeClient.CoreV1().PersistentVolumes().(*mockpersistentvolume.MockInterface).EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).
						DoAndReturn(func(_ context.Context, pv *v1.PersistentVolume, _ metav1.UpdateOptions) (*v1.PersistentVolume, error) {
							return pv, nil
						}).AnyTimes()
				} else {
					pvcInterface.EXPECT().Get(gomock.Any(), pvc.Name, gomock.Any()).
						Return(nil, errors.New("not found")).AnyTimes()
				}
			}
		}

		result, err := d.ControllerModifyVolume(ctx, test.req)
		if err != nil {
			checkTestError(t, test.expectedErrCode, err)
		}
		if !reflect.DeepEqual(result, test.expectedResp) {
			t.Errorf("input request: %v, ControllerModifyVolume result: %v, expected: %v", test.req, result, test.expectedResp)
		}

		// Verify migration monitoring state
		if test.expectMigrationStarted {
			time.Sleep(100 * time.Millisecond) // Allow some time for migration go routine to begin
			assert.True(t, d.GetMigrationMonitor().IsMigrationActive(test.req.VolumeId),
				"Migration for volume %s should be active for test: %s", test.req.VolumeId, test.desc)

			activeMigrations := d.GetMigrationMonitor().GetActiveMigrations()
			assert.Equal(t, 1, len(activeMigrations),
				"Should have one active migration for test: %s", test.desc)

			migration, exists := activeMigrations[test.req.VolumeId]
			assert.True(t, exists, "Migration should exist for volume ID: %s", test.req.VolumeId)
			assert.Equal(t, test.req.VolumeId, migration.DiskURI)
			assert.Equal(t, testVolumeName, migration.PVName)

			// Verify SKU change details if this is a migration test
			if newSKU, exists := test.req.MutableParameters[consts.SkuNameField]; exists && test.oldSKU != nil {
				assert.Equal(t, armcompute.DiskStorageAccountTypes(newSKU), migration.ToSKU)
				assert.Equal(t, string(*test.oldSKU), migration.FromSKU)
			}

			// Verify migration started event was emitted for successful cases
			if test.setupPVCMocks {
				// wait for events in mockEventRecorder
				time.Sleep(100 * time.Millisecond)

				select {
				case event := <-mockEventRecorder.Events:
					assert.Contains(t, event, "Normal", "Event should be Normal type")
					assert.Contains(t, event, ReasonSKUMigrationStarted, "Event should contain migration started reason")
					assert.Contains(t, event, testVolumeName, "Event should contain PV name")
					if test.oldSKU != nil {
						assert.Contains(t, event, string(*test.oldSKU), "Event should contain source SKU")
					}
					if newSKU, exists := test.req.MutableParameters[consts.SkuNameField]; exists {
						assert.Contains(t, event, newSKU, "Event should contain target SKU")
					}
				default:
					t.Errorf("Expected migration started event was not recorded for test: %s", test.desc)
				}
			}
		} else {
			assert.False(t, d.GetMigrationMonitor().IsMigrationActive(test.req.VolumeId),
				"Migration should NOT be active for test: %s", test.desc)

			activeMigrations := d.GetMigrationMonitor().GetActiveMigrations()
			assert.Equal(t, 0, len(activeMigrations),
				"Should have no active migrations for test: %s", test.desc)

			// Verify no events were emitted for non-migration cases
			select {
			case event := <-mockEventRecorder.Events:
				// Only error if this is not a migration case or if it's an error case
				if !test.expectMigrationStarted && test.expectedErrCode == codes.OK {
					t.Errorf("Unexpected event recorded for test %s: %s", test.desc, event)
				}
			default:
				// Expected - no events should be recorded for non-migration cases
			}
		}

		// Simulate controller restart scenario
		if test.simulateRestart {

			disk = &armcompute.Disk{
				ID: &id,
				SKU: &armcompute.DiskSKU{
					Name: test.oldSKU,
				},
				Properties: &armcompute.DiskProperties{},
			}
			disk.Properties.CompletionPercent = to.Ptr(float32(0))

			cntlForRestart := gomock.NewController(t)
			defer cntlForRestart.Finish()
			drestart := getFakeDriverWithKubeClient(cntlForRestart)

			// Mock List call for recovery scenarios
			pvList := &v1.PersistentVolumeList{Items: []v1.PersistentVolume{}}
			for _, pv := range testPVCopies {
				testPvCopy := pv.DeepCopy()
				testPvCopy.Labels = map[string]string{
					LabelMigrationInProgress: "true",
				}
				pvList.Items = append(pvList.Items, *testPvCopy)
			}

			// Setup mock expectations for PV operations
			mockCoreV1 := drestart.getCloud().KubeClient.CoreV1()
			mockPVInterface := mockCoreV1.PersistentVolumes().(*mockpersistentvolume.MockInterface)
			mockCoreV1.(*mockcorev1.MockInterface).EXPECT().PersistentVolumes().Return(mockPVInterface).AnyTimes()
			mockPVInterface.EXPECT().List(gomock.Any(), gomock.Any()).Return(pvList, nil).AnyTimes()
			pvcInterface := mockpersistentvolumeclaim.NewMockPersistentVolumeClaimInterface(cntlForRestart)
			mockCoreV1.(*mockcorev1.MockInterface).EXPECT().PersistentVolumeClaims(gomock.Any()).Return(pvcInterface).AnyTimes()
			mockPVCInterface := mockCoreV1.PersistentVolumeClaims("default").(*mockpersistentvolumeclaim.MockPersistentVolumeClaimInterface)

			// Mock Get and Update calls
			for _, pvc := range testPVCs {
				if test.pvcExists {
					mockPVCInterface.EXPECT().Get(gomock.Any(), pvc.Name, gomock.Any()).
						Return(pvc, nil).AnyTimes()
				}
			}
			for _, pv := range testPVCopies {
				if test.pvcExists {
					mockPVInterface.EXPECT().Get(gomock.Any(), pv.Name, gomock.Any()).
						Return(pv.DeepCopy(), nil).AnyTimes()

					mockPVInterface.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).
						Return(pv.DeepCopy(), nil).AnyTimes()
				} else {
					mockPVInterface.EXPECT().Get(gomock.Any(), pv.Name, gomock.Any()).
						Return(nil, errors.New("not found")).AnyTimes()
				}
			}

			diskCompleted := atomic.Bool{}
			diskCompleted.Store(false)
			// Setup disk client mocks
			diskclientForRestart := mock_diskclient.NewMockInterface(cntlForRestart)
			drestart.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskclientForRestart, nil).AnyTimes()
			diskclientForRestart.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, _, _ string) (*armcompute.Disk, error) {
					diskCopy := &armcompute.Disk{
						ID: disk.ID,
						SKU: &armcompute.DiskSKU{
							Name: disk.SKU.Name,
						},
						// Report a real, non-nil, in-progress CompletionPercent (0%) by
						// default so the recovered migration stays genuinely active for
						// this test's assertions below. A nil CompletionPercent is now
						// always treated as "no pending copy" (complete).
						Properties: &armcompute.DiskProperties{
							CompletionPercent: to.Ptr(float32(0)),
						},
					}
					if diskCompleted.Load() {
						diskCopy.Properties.CompletionPercent = to.Ptr(float32(100))
					}
					return diskCopy, nil
				},
			).AnyTimes()
			diskclientForRestart.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()

			// Simulate controller restart by resetting migration monitor
			drestart.SetMigrationMonitor(nil)

			migrationCheckIntervalOriginal := migrationCheckInterval
			defer func() {
				migrationCheckInterval = migrationCheckIntervalOriginal
			}()
			migrationCheckInterval = time.Millisecond * 100 // Speed up migration checks for tests

			// Create new migration monitor (simulating controller restart)
			drestart.SetMigrationMonitor(NewMigrationProgressMonitor(drestart.getCloud().KubeClient, mockEventRecorder, drestart.GetDiskController()))

			// Simulate recovery process that would happen on controller startup
			if test.pvHasMigrationLabels {
				// Call recovery function that would be called during controller initialization
				err := drestart.RecoverMigrationMonitor(ctx)
				assert.NoError(t, err, "Recovery should succeed for test: %s", test.desc)
				time.Sleep(100 * time.Millisecond) // Allow some time for migration go routine to begin

				activeMigrations := drestart.GetMigrationMonitor().GetActiveMigrations()
				assert.GreaterOrEqual(t, len(activeMigrations), 1, "At least one migration should be recovered")
				assert.True(t, drestart.GetMigrationMonitor().IsMigrationActive(testVolumeID), "Migration should be recovered for test: %s", test.desc)

				if test.multipleMigrationsToRecover {
					activeMigrations := drestart.GetMigrationMonitor().GetActiveMigrations()
					assert.Equal(t, 3, len(activeMigrations), "Should recover 3 migrations for test: %s", test.desc)
				}
			}

			if test.simulateMigrationCompletionAfterRestart {
				diskCompleted.Store(true)
			} else {
				drestart.GetMigrationMonitor().Stop()
			}

			// Wait for all active migrations to complete and maximum 2 seconds
			startedTime := time.Now()
			for {
				activeMigrations := drestart.GetMigrationMonitor().GetActiveMigrations()
				if len(activeMigrations) == 0 {
					break
				}
				if time.Since(startedTime) > time.Second*2 {
					klog.Errorf("Timeout waiting for migrations to complete for test: %s", test.desc)
					break
				}
				time.Sleep(100 * time.Millisecond)
			}

			activeMigrations := drestart.GetMigrationMonitor().GetActiveMigrations()
			assert.Equal(t, 0, len(activeMigrations), "All migrations should be completed after restart for test: %s", test.desc)

			drestart.GetMigrationMonitor().Stop()
		}

		// Clean up migration monitor
		if d.GetMigrationMonitor() != nil {
			d.GetMigrationMonitor().Stop()
		}
	}
}

func TestControllerVolumeMutationPVLookup(t *testing.T) {
	tests := []struct {
		name            string
		pvListErr       error
		invoke          func(FakeDriver) error
		expectedCode    codes.Code
		expectedMessage string
	}{
		{
			name:      "modify fails closed on PV API error",
			pvListErr: errors.New("Kubernetes API unavailable"),
			invoke: func(driver FakeDriver) error {
				_, err := driver.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{VolumeId: testVolumeID})
				return err
			},
			expectedCode:    codes.Internal,
			expectedMessage: "failed to get PV from disk URI",
		},
		{
			name: "modify continues when PV is absent",
			invoke: func(driver FakeDriver) error {
				_, err := driver.ControllerModifyVolume(context.Background(), &csi.ControllerModifyVolumeRequest{VolumeId: "invalid"})
				return err
			},
			expectedCode:    codes.NotFound,
			expectedMessage: "Volume not found",
		},
		{
			name:      "expand fails closed on PV API error",
			pvListErr: errors.New("Kubernetes API unavailable"),
			invoke: func(driver FakeDriver) error {
				_, err := driver.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
					VolumeId:      testVolumeID,
					CapacityRange: &csi.CapacityRange{RequiredBytes: volumehelper.GiBToBytes(10)},
				})
				return err
			},
			expectedCode:    codes.Internal,
			expectedMessage: "failed to get PV from disk URI",
		},
		{
			name: "expand continues when PV is absent",
			invoke: func(driver FakeDriver) error {
				_, err := driver.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
					VolumeId:      "invalid",
					CapacityRange: &csi.CapacityRange{RequiredBytes: volumehelper.GiBToBytes(10)},
				})
				return err
			},
			expectedCode:    codes.Internal,
			expectedMessage: "GetDiskByURI(invalid)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cntl := gomock.NewController(t)
			driver, err := NewFakeDriver(cntl)
			require.NoError(t, err)
			driver.(*fakeDriver).nodeDrivenAttachDetachEnabled = true
			if test.pvListErr != nil {
				driver.(*fakeDriver).kubeClient.(*fake.Clientset).PrependReactor("list", "persistentvolumes", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, test.pvListErr
				})
			}

			err = test.invoke(driver)

			assert.Equal(t, test.expectedCode, status.Code(err))
			assert.ErrorContains(t, err, test.expectedMessage)
		})
	}
}

func TestControllerModifyVolume_MigrationLifecycleAndTimeout(t *testing.T) {
	// Save originals
	origInterval := migrationCheckInterval
	origMax := maxMigrationTimeout
	origTimeouts := make(map[int64]time.Duration)
	for k, v := range migrationTimeouts {
		origTimeouts[k] = v
	}
	defer func() {
		migrationCheckInterval = origInterval
		maxMigrationTimeout = origMax
		for k := range migrationTimeouts {
			delete(migrationTimeouts, k)
		}
		for k, v := range origTimeouts {
			migrationTimeouts[k] = v
		}
	}()

	// Base shared timing (used by idempotent subtest)
	baseInterval := 20 * time.Millisecond
	baseSlabTimeout := 100 * time.Millisecond
	baseMaxTimeout := 200 * time.Millisecond

	setTiming := func(interval time.Duration, slabTimeout time.Duration, maxTimeout time.Duration) {
		migrationCheckInterval = interval
		migrationTimeouts[volumeSize2TB] = slabTimeout
		maxMigrationTimeout = maxTimeout
	}

	// Apply base once
	setTiming(baseInterval, baseSlabTimeout, baseMaxTimeout)

	newEnv := func(ctrl *gomock.Controller, testVolumeStr string) (FakeDriver, *record.FakeRecorder,
		*mockpersistentvolume.MockInterface, *mockpersistentvolumeclaim.MockPersistentVolumeClaimInterface,
		*mock_diskclient.MockInterface, *v1.PersistentVolume, *v1.PersistentVolumeClaim) {

		d := getFakeDriverWithKubeClient(ctrl)
		rec := record.NewFakeRecorder(200)
		d.SetMigrationMonitor(NewMigrationProgressMonitor(d.getCloud().KubeClient, rec, d.GetDiskController()))

		// get the last token in testVolumeStr
		volumeID := strings.Split(testVolumeStr, "/")[len(strings.Split(testVolumeStr, "/"))-1]
		pvcID := fmt.Sprintf("pvc-%s", volumeID)

		sizeGi := int64(10)
		pv := &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: volumeID},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{
					v1.ResourceName("storage"): *resource.NewQuantity(sizeGi*1024*1024*1024, resource.BinarySI),
				},
				ClaimRef: &v1.ObjectReference{Name: pvcID, Namespace: "default"},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					CSI: &v1.CSIPersistentVolumeSource{
						Driver:       "disk.csi.azure.com",
						VolumeHandle: testVolumeStr,
					},
				},
			},
		}
		d.(*fakeDriver).kubeClient = fake.NewClientset(pv)
		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcID, Namespace: "default"},
			Spec: v1.PersistentVolumeClaimSpec{
				VolumeName: pv.Name,
				Resources: v1.VolumeResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceName("storage"): *resource.NewQuantity(sizeGi*1024*1024*1024, resource.BinarySI),
					},
				},
				AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			},
		}

		coreMock := d.getCloud().KubeClient.CoreV1().(*mockcorev1.MockInterface)
		pvIf := d.getCloud().KubeClient.CoreV1().PersistentVolumes().(*mockpersistentvolume.MockInterface)
		pvcIf := mockpersistentvolumeclaim.NewMockPersistentVolumeClaimInterface(ctrl)

		// Use gomock.Any() for namespace to avoid strict mismatch; set expectations BEFORE any call
		coreMock.EXPECT().PersistentVolumes().Return(pvIf).AnyTimes()
		coreMock.EXPECT().PersistentVolumeClaims(gomock.Any()).Return(pvcIf).AnyTimes()

		pvIf.EXPECT().Get(gomock.Any(), pv.Name, gomock.Any()).Return(pv.DeepCopy(), nil).AnyTimes()
		pvcIf.EXPECT().Get(gomock.Any(), pvc.Name, gomock.Any()).Return(pvc.DeepCopy(), nil).AnyTimes()

		diskClient := mock_diskclient.NewMockInterface(ctrl)
		d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().
			GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()

		return d, rec, pvIf, pvcIf, diskClient, pv, pvc
	}

	t.Run("lifecycle: start -> milestones -> completion -> label removed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testVolumeStr := fmt.Sprintf("%s%d", testVolumeID, 1)
		d, rec, pvIf, _, diskClient, _, _ := newEnv(ctrl, testVolumeStr)

		updateCount := atomic.Int32{}
		pvIf.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, got *v1.PersistentVolume, _ metav1.UpdateOptions) (*v1.PersistentVolume, error) {
				updateCount.Add(1)
				return got, nil
			},
		).MinTimes(2)

		progressSeq := []float32{0, 10, 20, 35, 40, 55, 60, 75, 80, 95, 100}
		var idx int32
		baseDisk := &armcompute.Disk{
			ID:  to.Ptr(testVolumeStr),
			SKU: &armcompute.DiskSKU{Name: to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS)},
			Properties: &armcompute.DiskProperties{
				DiskSizeGB: to.Ptr[int32](10),
			},
		}
		diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _, _ string) (*armcompute.Disk, error) {
				i := int(atomic.AddInt32(&idx, 1)) - 1
				if i >= len(progressSeq) {
					i = len(progressSeq) - 1
				}
				cp := progressSeq[i]
				dcopy := &armcompute.Disk{
					ID:  baseDisk.ID,
					SKU: baseDisk.SKU,
					Properties: &armcompute.DiskProperties{
						DiskSizeGB:        baseDisk.Properties.DiskSizeGB,
						CompletionPercent: to.Ptr(cp),
					},
				}
				return dcopy, nil
			},
		).AnyTimes()
		diskClient.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(baseDisk, nil).AnyTimes()

		req := &csi.ControllerModifyVolumeRequest{
			VolumeId: testVolumeStr,
			MutableParameters: map[string]string{
				consts.SkuNameField: string(armcompute.DiskStorageAccountTypesPremiumV2LRS),
			},
		}
		_, err := d.ControllerModifyVolume(context.Background(), req)
		assert.NoError(t, err)

		var events []string
		timeout := time.After(maxMigrationTimeout)
		for {
			time.Sleep(migrationCheckInterval)
			select {
			case e := <-rec.Events:
				events = append(events, e)
				if strings.Contains(e, ReasonSKUMigrationCompleted) {
					goto DONE
				}
			case <-timeout:
				goto DONE
			}
		}
	DONE:
		startCnt := 0
		compCnt := 0
		milestones := map[int]bool{}
		for _, e := range events {
			if strings.Contains(e, ReasonSKUMigrationStarted) {
				startCnt++
			}
			if strings.Contains(e, ReasonSKUMigrationProgress) {
				for _, m := range []int{20, 40, 60, 80} {
					if strings.Contains(e, fmt.Sprintf("%.1f%%", float32(m))) {
						milestones[m] = true
					}
				}
			}
			if strings.Contains(e, ReasonSKUMigrationCompleted) {
				compCnt++
			}
		}
		assert.Equal(t, 1, startCnt, "expected one start event")
		assert.True(t, milestones[20] && milestones[40] && milestones[60] && milestones[80], "missing milestone events: %v", events)
		assert.Equal(t, 1, compCnt, "expected one completion event")
		time.Sleep(100 * time.Millisecond)
		assert.False(t, d.GetMigrationMonitor().IsMigrationActive(testVolumeStr))
		assert.GreaterOrEqual(t, updateCount.Load(), int32(2))
	})

	t.Run("idempotent: second modify call while active does not duplicate task or emit second start", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testVolumeStr := fmt.Sprintf("%s%d", testVolumeID, 2)
		d, rec, pvIf, _, diskClient, _, _ := newEnv(ctrl, testVolumeStr)

		pvIf.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(&v1.PersistentVolume{}, nil).MinTimes(1)

		disk := &armcompute.Disk{
			ID:  to.Ptr(testVolumeStr),
			SKU: &armcompute.DiskSKU{Name: to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS)},
			Properties: &armcompute.DiskProperties{
				DiskSizeGB:        to.Ptr[int32](10),
				CompletionPercent: to.Ptr(float32(10)),
			},
		}
		diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
		diskClient.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()

		req := &csi.ControllerModifyVolumeRequest{
			VolumeId: testVolumeStr,
			MutableParameters: map[string]string{
				consts.SkuNameField: string(armcompute.DiskStorageAccountTypesPremiumV2LRS),
			},
		}
		_, err := d.ControllerModifyVolume(context.Background(), req)
		assert.NoError(t, err)
		time.Sleep(60 * time.Millisecond)
		_, err = d.ControllerModifyVolume(context.Background(), req)
		assert.NoError(t, err)

		starts := 0
		time.Sleep(migrationCheckInterval)
		for {
			timeout := time.After(maxMigrationTimeout)
			select {
			case e := <-rec.Events:
				if strings.Contains(e, ReasonSKUMigrationStarted) {
					starts++
				}
			case <-timeout:
				goto DONE
			}
		}
	DONE:
		assert.Equal(t, 1, starts)
		d.GetMigrationMonitor().Stop()
	})

	t.Run("timeout: emits timeout event without completion", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testVolumeStr := fmt.Sprintf("%s%d", testVolumeID, 3)
		d, rec, pvIf, _, diskClient, _, _ := newEnv(ctrl, testVolumeStr)

		pvIf.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(&v1.PersistentVolume{}, nil).MinTimes(1)

		disk := &armcompute.Disk{
			ID:  to.Ptr(testVolumeStr),
			SKU: &armcompute.DiskSKU{Name: to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS)},
			Properties: &armcompute.DiskProperties{
				DiskSizeGB:        to.Ptr[int32](10),
				CompletionPercent: to.Ptr(float32(10)),
			},
		}
		diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
		diskClient.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()

		req := &csi.ControllerModifyVolumeRequest{
			VolumeId: testVolumeStr,
			MutableParameters: map[string]string{
				consts.SkuNameField: string(armcompute.DiskStorageAccountTypesPremiumV2LRS),
			},
		}
		_, err := d.ControllerModifyVolume(context.Background(), req)
		assert.NoError(t, err)

		timeout := time.After(2 * time.Second)
		var timeoutFound, completionFound bool
		for !timeoutFound && !completionFound {
			time.Sleep(migrationCheckInterval)
			select {
			case e := <-rec.Events:
				if strings.Contains(e, ReasonSKUMigrationTimeout) {
					timeoutFound = true
				}
				if strings.Contains(e, ReasonSKUMigrationCompleted) {
					completionFound = true
				}
			case <-timeout:
				goto DONE
			}
		}
	DONE:
		assert.True(t, timeoutFound, "expected timeout event")
		assert.False(t, completionFound, "unexpected completion event")
		d.GetMigrationMonitor().Stop()
	})
}

func TestControllerPublishVolume(t *testing.T) {
	volumeCap := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: 2}}
	volumeCapWrong := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: 10}}
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, err := NewFakeDriver(cntl)
	nodeName := "unit-test-node"
	//d.setCloud(&azure.Cloud{})
	if err != nil {
		t.Fatalf("Error getting driver: %v", err)
	}
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Volume ID missing",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				req := &csi.ControllerPublishVolumeRequest{}
				expectedErr := status.Error(codes.InvalidArgument, "Volume ID not provided")
				_, err := d.ControllerPublishVolume(context.Background(), req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Volume capability missing",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				req := &csi.ControllerPublishVolumeRequest{
					VolumeId: "vol_1",
				}
				expectedErr := status.Error(codes.InvalidArgument, "Volume capability not provided")
				_, err := d.ControllerPublishVolume(context.Background(), req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Volume capability not supported",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				req := &csi.ControllerPublishVolumeRequest{
					VolumeId:         "vol_1",
					VolumeCapability: volumeCapWrong,
				}
				expectedErr := status.Error(codes.InvalidArgument, "invalid access mode: [mount:{} access_mode:{mode:10}]")
				_, err := d.ControllerPublishVolume(context.Background(), req)
				if !reflect.DeepEqual(err, expectedErr) && !strings.Contains(err.Error(), "invalid access mode") {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "diskName error",
			testFunc: func(t *testing.T) {
				req := &csi.ControllerPublishVolumeRequest{
					VolumeId:         "vol_1",
					VolumeCapability: volumeCap,
				}
				expectedErr := status.Error(codes.NotFound, "Volume not found, failed with error: invalid URI: vol_1")
				_, err := d.ControllerPublishVolume(context.Background(), req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "NodeID missing",
			testFunc: func(t *testing.T) {
				req := &csi.ControllerPublishVolumeRequest{
					VolumeId:         testVolumeID,
					VolumeCapability: volumeCap,
				}
				id := req.VolumeId
				disk := &armcompute.Disk{
					ID: &id,
				}
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()

				expectedErr := status.Error(codes.InvalidArgument, "Node ID not provided")
				_, err := d.ControllerPublishVolume(context.Background(), req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "failed provisioning state",
			testFunc: func(t *testing.T) {
				req := &csi.ControllerPublishVolumeRequest{
					VolumeId:         testVolumeID,
					VolumeCapability: volumeCap,
					NodeId:           nodeName,
				}
				id := req.VolumeId
				disk := &armcompute.Disk{
					ID: &id,
				}
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				instanceID := fmt.Sprintf("/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/%s", nodeName)
				vm := armcompute.VirtualMachine{
					Name:     &nodeName,
					ID:       &instanceID,
					Location: &d.getCloud().Location,
				}
				vmstatus := []*armcompute.InstanceViewStatus{
					{
						Code: ptr.To("PowerState/Running"),
					},
					{
						Code: ptr.To("ProvisioningState/succeeded"),
					},
				}
				vm.Properties = &armcompute.VirtualMachineProperties{
					ProvisioningState: ptr.To("Failed"),
					HardwareProfile: &armcompute.HardwareProfile{
						VMSize: ptr.To(armcompute.VirtualMachineSizeTypesStandardA0),
					},
					InstanceView: &armcompute.VirtualMachineInstanceView{
						Statuses: vmstatus,
					},
					StorageProfile: &armcompute.StorageProfile{
						DataDisks: []*armcompute.DataDisk{},
					},
				}
				dataDisks := make([]*armcompute.DataDisk, 1)
				dataDisks[0] = &armcompute.DataDisk{Lun: ptr.To(int32(0)), Name: &testVolumeName}
				vm.Properties.StorageProfile.DataDisks = dataDisks
				mockVMClient := d.getCloud().ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
				mockVMClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&vm, nil).AnyTimes()
				mockVMClient.EXPECT().BeginUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("Retriable: false, RetryAfter: 0s, HTTPStatusCode: 0, RawError: error")).AnyTimes()
				expectedErr := status.Errorf(codes.Internal, "update instance \"unit-test-node\" failed with Retriable: false, RetryAfter: 0s, HTTPStatusCode: 0, RawError: error")
				_, err := d.ControllerPublishVolume(context.Background(), req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Volume already attached success",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, err := NewFakeDriver(cntl)
				if err != nil {
					t.Fatalf("Error getting driver: %v", err)
				}
				req := &csi.ControllerPublishVolumeRequest{
					VolumeId:         testVolumeID,
					VolumeCapability: volumeCap,
					NodeId:           nodeName,
				}
				id := req.VolumeId
				disk := &armcompute.Disk{
					ID: &id,
				}
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				instanceID := fmt.Sprintf("/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/%s", nodeName)
				vm := armcompute.VirtualMachine{
					Name:     &nodeName,
					ID:       &instanceID,
					Location: &d.getCloud().Location,
				}
				vmstatus := []*armcompute.InstanceViewStatus{
					{
						Code: ptr.To("PowerState/Running"),
					},
					{
						Code: ptr.To("ProvisioningState/succeeded"),
					},
				}
				vm.Properties = &armcompute.VirtualMachineProperties{
					ProvisioningState: ptr.To("Succeeded"),
					HardwareProfile: &armcompute.HardwareProfile{
						VMSize: ptr.To(armcompute.VirtualMachineSizeTypesStandardA0),
					},
					InstanceView: &armcompute.VirtualMachineInstanceView{
						Statuses: vmstatus,
					},
					StorageProfile: &armcompute.StorageProfile{
						DataDisks: []*armcompute.DataDisk{},
					},
				}
				dataDisks := make([]*armcompute.DataDisk, 1)
				dataDisks[0] = &armcompute.DataDisk{Lun: ptr.To(int32(0)), Name: &testVolumeName}
				vm.Properties.StorageProfile.DataDisks = dataDisks
				mockVMClient := d.getCloud().ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
				mockVMClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&vm, nil).AnyTimes()
				_, err = d.ControllerPublishVolume(context.Background(), req)
				if !reflect.DeepEqual(err, nil) {
					t.Errorf("actualErr: (%v), expectedErr: (<nil>)", err)
				}
			},
		},
		{
			name: "CachingMode Error",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, err := NewFakeDriver(cntl)
				if err != nil {
					t.Fatalf("Error getting driver: %v", err)
				}
				volumeContext := make(map[string]string)
				volumeContext[consts.CachingModeField] = "badmode"
				req := &csi.ControllerPublishVolumeRequest{
					VolumeId:         testVolumeID,
					VolumeCapability: volumeCap,
					NodeId:           nodeName,
					VolumeContext:    volumeContext,
				}
				id := req.VolumeId
				disk := &armcompute.Disk{
					ID: &id,
				}
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				instanceID := fmt.Sprintf("/subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/%s", nodeName)
				vm := armcompute.VirtualMachine{
					Name:     &nodeName,
					ID:       &instanceID,
					Location: &d.getCloud().Location,
				}
				vmstatus := []*armcompute.InstanceViewStatus{
					{
						Code: ptr.To("PowerState/Running"),
					},
					{
						Code: ptr.To("ProvisioningState/succeeded"),
					},
				}
				vm.Properties = &armcompute.VirtualMachineProperties{
					ProvisioningState: ptr.To("Succeeded"),
					HardwareProfile: &armcompute.HardwareProfile{
						VMSize: ptr.To(armcompute.VirtualMachineSizeTypesStandardA0),
					},
					InstanceView: &armcompute.VirtualMachineInstanceView{
						Statuses: vmstatus,
					},
					StorageProfile: &armcompute.StorageProfile{
						DataDisks: []*armcompute.DataDisk{},
					},
				}
				mockVMClient := d.getCloud().ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
				mockVMClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&vm, nil).AnyTimes()
				expectedErr := status.Errorf(codes.Internal, "azureDisk - badmode is not supported cachingmode. Supported values are [None ReadOnly ReadWrite]")
				_, err = d.ControllerPublishVolume(context.Background(), req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (<nil>)", err)
				}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestEnsureQADPVAnnotationsRetriesConflict(t *testing.T) {
	const (
		pvName          = "qad-pv"
		blobURL         = "https://example.blob.storage.azure.net/container/disk"
		claimIdentifier = "claim-id"
	)
	kubeClient := fake.NewClientset(&v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
	})

	updateAttempts := 0
	kubeClient.Fake.PrependReactor("update", "persistentvolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updateAttempts++
		if updateAttempts == 1 {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Resource: "persistentvolumes"}, pvName, errors.New("concurrent update"))
		}
		return false, nil, nil
	})

	require.NoError(t, ensureQADPVAnnotations(context.Background(), kubeClient, pvName, blobURL, claimIdentifier, "ReadOnly"))
	assert.Equal(t, 2, updateAttempts)

	pv, err := kubeClient.CoreV1().PersistentVolumes().Get(context.Background(), pvName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "0", pv.Annotations[consts.AttachSequenceAnnotation])
	assert.Equal(t, blobURL, pv.Annotations[consts.BlobURLAnnotation])
	assert.Equal(t, claimIdentifier, pv.Annotations[consts.ClaimIdentifierAnnotation])
}

func TestEnsureQADPVAnnotationsRejectsEmptyPVName(t *testing.T) {
	err := ensureQADPVAnnotations(context.Background(), nil, "", "blob-url", "claim-id", "ReadOnly")
	require.EqualError(t, err, "PV name must not be empty")
}

func TestEnsureQADPVAnnotationsRejectsIncompleteMetadata(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		blobURL     string
		claimID     string
		expectedErr string
	}{
		{
			name:        "new QAD PV without blob URL",
			claimID:     "claim-id",
			expectedErr: "without blob URL and claim identifier",
		},
		{
			name:        "new QAD PV without claim identifier",
			blobURL:     "https://example.blob.storage.azure.net/container/disk",
			expectedErr: "without blob URL and claim identifier",
		},
		{
			name: "existing QAD PV without blob URL",
			annotations: map[string]string{
				consts.AttachSequenceAnnotation:  "1",
				consts.ClaimIdentifierAnnotation: "claim-id",
			},
			expectedErr: "attach-sequence annotation but incomplete QAD metadata",
		},
		{
			name: "existing QAD PV without claim identifier",
			annotations: map[string]string{
				consts.AttachSequenceAnnotation: "1",
				consts.BlobURLAnnotation:        "https://example.blob.storage.azure.net/container/disk",
			},
			expectedErr: "attach-sequence annotation but incomplete QAD metadata",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const pvName = "qad-pv"
			kubeClient := fake.NewClientset(&v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: pvName, Annotations: test.annotations},
			})

			err := ensureQADPVAnnotations(context.Background(), kubeClient, pvName, test.blobURL, test.claimID, "ReadOnly")
			require.ErrorContains(t, err, test.expectedErr)

			pv, getErr := kubeClient.CoreV1().PersistentVolumes().Get(context.Background(), pvName, metav1.GetOptions{})
			require.NoError(t, getErr)
			assert.Equal(t, test.annotations, pv.Annotations)
		})
	}
}

func TestEnsureQADPVAnnotationsFillsMissingCompanionAnnotations(t *testing.T) {
	const (
		pvName          = "qad-pv"
		blobURL         = "https://example.blob.storage.azure.net/container/disk"
		claimIdentifier = "claim-id"
	)
	// PV already adopted (attach-sequence set) but the companion annotations are
	// missing; the metadata arrives via the volume context (passed args).
	kubeClient := fake.NewClientset(&v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName, Annotations: map[string]string{
			consts.AttachSequenceAnnotation: "3",
		}},
	})

	require.NoError(t, ensureQADPVAnnotations(context.Background(), kubeClient, pvName, blobURL, claimIdentifier, "ReadOnly"))

	pv, err := kubeClient.CoreV1().PersistentVolumes().Get(context.Background(), pvName, metav1.GetOptions{})
	require.NoError(t, err)
	// attach-sequence must be preserved, not reset.
	assert.Equal(t, "3", pv.Annotations[consts.AttachSequenceAnnotation])
	assert.Equal(t, blobURL, pv.Annotations[consts.BlobURLAnnotation])
	assert.Equal(t, claimIdentifier, pv.Annotations[consts.ClaimIdentifierAnnotation])
}

func TestEnsureQADPVAnnotationsSeedsFromExistingAnnotations(t *testing.T) {
	const (
		pvName          = "static-qad-pv"
		blobURL         = "https://example.blob.storage.azure.net/container/disk"
		claimIdentifier = "claim-id"
	)
	// Static PV carries the claim metadata in annotations but no attach-sequence
	// yet, and the volume context (passed args) is empty.
	kubeClient := fake.NewClientset(&v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName, Annotations: map[string]string{
			consts.BlobURLAnnotation:         blobURL,
			consts.ClaimIdentifierAnnotation: claimIdentifier,
		}},
	})

	require.NoError(t, ensureQADPVAnnotations(context.Background(), kubeClient, pvName, "", "", "ReadOnly"))

	pv, err := kubeClient.CoreV1().PersistentVolumes().Get(context.Background(), pvName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "0", pv.Annotations[consts.AttachSequenceAnnotation])
	assert.Equal(t, blobURL, pv.Annotations[consts.BlobURLAnnotation])
	assert.Equal(t, claimIdentifier, pv.Annotations[consts.ClaimIdentifierAnnotation])
}

func TestControllerPublishVolumePersistsEffectiveQADCachePolicyForLargeStaticDisk(t *testing.T) {
	cntl := gomock.NewController(t)
	d, err := NewFakeDriver(cntl)
	require.NoError(t, err)
	driver := d.(*fakeDriver)
	driver.nodeDrivenAttachDetachEnabled = true

	const pvName = "static-qad-pv"
	volumeContext := map[string]string{
		consts.AttachModeField:           consts.AttachModeNodeDriven,
		consts.CachingModeField:          "ReadOnly",
		consts.BlobURLAnnotation:         "https://example.blob.storage.azure.net/container/disk",
		consts.ClaimIdentifierAnnotation: "claim-id",
	}
	_, err = driver.kubeClient.CoreV1().PersistentVolumes().Create(context.Background(), &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       driver.Name,
				VolumeHandle: testVolumeID,
				VolumeAttributes: map[string]string{
					consts.AttachModeField:           consts.AttachModeNodeDriven,
					consts.CachingModeField:          "ReadOnly",
					consts.BlobURLAnnotation:         "https://example.blob.storage.azure.net/container/disk",
					consts.ClaimIdentifierAnnotation: "claim-id",
				},
			},
		}},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	diskClient := mock_diskclient.NewMockInterface(cntl)
	driver.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil)
	diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&armcompute.Disk{
		ID: to.Ptr(testVolumeID),
		Properties: &armcompute.DiskProperties{
			DiskSizeGB: to.Ptr(int32(diskCachingLimit)),
		},
	}, nil)

	result, err := d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId:         testVolumeID,
		NodeId:           "unit-test-node",
		VolumeCapability: stdVolumeCapabilities[0],
		VolumeContext:    volumeContext,
	})
	require.NoError(t, err)
	require.Equal(t, &csi.ControllerPublishVolumeResponse{}, result)

	pv, err := driver.kubeClient.CoreV1().PersistentVolumes().Get(context.Background(), pvName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "ReadOnly", pv.Spec.CSI.VolumeAttributes[consts.CachingModeField])
	assert.Equal(t, "None", pv.Annotations[consts.QADCachePolicyAnnotation])
	assert.Equal(t, "0", pv.Annotations[consts.AttachSequenceAnnotation])
}

func TestControllerPublishVolumeRejectsIncompleteQADMetadata(t *testing.T) {
	tests := []struct {
		name          string
		annotations   map[string]string
		volumeContext map[string]string
		expectedErr   string
	}{
		{
			name: "new QAD PV without claim metadata",
			volumeContext: map[string]string{
				consts.AttachModeField: consts.AttachModeNodeDriven,
			},
			expectedErr: "without blob URL and claim identifier",
		},
		{
			// Neither the volume context nor the PV annotations supply the claim
			// identifier, so the QAD metadata stays incomplete and publish is rejected.
			name: "existing QAD PV with incomplete companion annotations",
			annotations: map[string]string{
				consts.AttachSequenceAnnotation: "1",
				consts.BlobURLAnnotation:        "https://example.blob.storage.azure.net/container/disk",
			},
			volumeContext: map[string]string{
				consts.AttachModeField:   consts.AttachModeNodeDriven,
				consts.BlobURLAnnotation: "https://example.blob.storage.azure.net/container/disk",
			},
			expectedErr: "attach-sequence annotation but incomplete QAD metadata",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cntl := gomock.NewController(t)
			d, err := NewFakeDriver(cntl)
			require.NoError(t, err)
			driver := d.(*fakeDriver)
			driver.nodeDrivenAttachDetachEnabled = true
			_, err = driver.kubeClient.CoreV1().PersistentVolumes().Create(context.Background(), &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: "qad-pv", Annotations: test.annotations},
				Spec: v1.PersistentVolumeSpec{PersistentVolumeSource: v1.PersistentVolumeSource{
					CSI: &v1.CSIPersistentVolumeSource{Driver: driver.Name, VolumeHandle: testVolumeID},
				}},
			}, metav1.CreateOptions{})
			require.NoError(t, err)

			diskClient := mock_diskclient.NewMockInterface(cntl)
			driver.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil)
			diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&armcompute.Disk{
				ID: to.Ptr(testVolumeID),
				Properties: &armcompute.DiskProperties{
					DiskSizeGB: to.Ptr(int32(10)),
				},
			}, nil)

			result, err := d.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
				VolumeId: testVolumeID,
				NodeId:   "unit-test-node",
				VolumeCapability: &csi.VolumeCapability{
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
				},
				VolumeContext: test.volumeContext,
			})
			require.Nil(t, result)
			require.Equal(t, codes.Internal, status.Code(err))
			require.ErrorContains(t, err, test.expectedErr)
		})
	}
}

func TestControllerUnpublishVolume(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, err := NewFakeDriver(cntl)
	if err != nil {
		t.Fatalf("Error getting driver: %v", err)
	}
	d.setCloud(&azure.Cloud{})
	if err != nil {
		t.Fatalf("Error getting driver: %v", err)
	}
	tests := []struct {
		desc        string
		req         *csi.ControllerUnpublishVolumeRequest
		expectedErr error
	}{
		{
			desc:        "Volume ID missing",
			req:         &csi.ControllerUnpublishVolumeRequest{},
			expectedErr: status.Error(codes.InvalidArgument, "Volume ID not provided"),
		},
		{
			desc: "Node ID missing",
			req: &csi.ControllerUnpublishVolumeRequest{
				VolumeId: "vol_1",
			},
			expectedErr: status.Error(codes.InvalidArgument, "Node ID not provided"),
		},
		{
			desc: "DiskName error",
			req: &csi.ControllerUnpublishVolumeRequest{
				VolumeId: "vol_1",
				NodeId:   "unit-test-node",
			},
			expectedErr: status.Errorf(codes.Internal, "invalid URI: vol_1"),
		},
	}
	for _, test := range tests {
		_, err := d.ControllerUnpublishVolume(context.Background(), test.req)
		if !reflect.DeepEqual(err, test.expectedErr) {
			t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
		}
	}
}

func TestControllerUnpublishVolumeQADRequiresNodeID(t *testing.T) {
	cntl := gomock.NewController(t)
	d, err := NewFakeDriver(cntl)
	require.NoError(t, err)
	driver := d.(*fakeDriver)
	driver.nodeDrivenAttachDetachEnabled = true
	driver.kubeClient = fake.NewClientset(&v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "qad-pv",
			Annotations: map[string]string{
				consts.AttachSequenceAnnotation: "1",
			},
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       driver.Name,
					VolumeHandle: testVolumeID,
				},
			},
		},
	})

	resp, err := d.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: testVolumeID,
	})

	require.Nil(t, resp)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.ErrorContains(t, err, "Node ID not provided")
}

func TestControllerGetCapabilities(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
	capType := &csi.ControllerServiceCapability_Rpc{
		Rpc: &csi.ControllerServiceCapability_RPC{
			Type: csi.ControllerServiceCapability_RPC_UNKNOWN,
		},
	}
	capList := []*csi.ControllerServiceCapability{{
		Type: capType,
	}}
	d.setControllerCapabilities(capList)
	// Test valid request
	req := csi.ControllerGetCapabilitiesRequest{}
	resp, err := d.ControllerGetCapabilities(context.Background(), &req)
	assert.NotNil(t, resp)
	assert.Equal(t, resp.Capabilities[0].GetType(), capType)
	assert.NoError(t, err)
}

func TestControllerExpandVolume(t *testing.T) {
	stdVolSize := int64(5 * 1024 * 1024 * 1024)
	stdCapRange := &csi.CapacityRange{RequiredBytes: stdVolSize}

	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Volume ID missing",
			testFunc: func(t *testing.T) {
				req := &csi.ControllerExpandVolumeRequest{}

				ctx := context.Background()
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)

				expectedErr := status.Error(codes.InvalidArgument, "Volume ID missing in the request")
				_, err := d.ControllerExpandVolume(ctx, req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("Unexpected error: %v", err)
				}
			},
		},
		{
			name: "Volume capabilities missing",
			testFunc: func(t *testing.T) {
				req := &csi.ControllerExpandVolumeRequest{
					VolumeId: "vol_1",
				}

				ctx := context.Background()
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				var csc []*csi.ControllerServiceCapability
				d.setControllerCapabilities(csc)
				expectedErr := status.Error(codes.InvalidArgument, "invalid expand volume request: volume_id:\"vol_1\"")
				_, err := d.ControllerExpandVolume(ctx, req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("Unexpected error: %v", err)
				}
			},
		},
		{
			name: "Volume Capacity range missing",
			testFunc: func(t *testing.T) {
				req := &csi.ControllerExpandVolumeRequest{
					VolumeId: "vol_1",
				}

				ctx := context.Background()
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)

				expectedErr := status.Error(codes.InvalidArgument, "volume capacity range missing in request")
				_, err := d.ControllerExpandVolume(ctx, req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("Unexpected error: %v", err)
				}
			},
		},
		{
			name: "disk type is not managedDisk",
			testFunc: func(t *testing.T) {
				req := &csi.ControllerExpandVolumeRequest{
					VolumeId:      "httptest",
					CapacityRange: stdCapRange,
				}
				ctx := context.Background()
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				d.(*fakeDriver).kubeClient = fake.NewClientset(newTestPV(req.VolumeId))

				expectedErr := status.Error(codes.Internal, "GetDiskByURI(httptest) failed with error(invalid URI: httptest)")
				_, err := d.ControllerExpandVolume(ctx, req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("Unexpected error: %v", err)
				}
			},
		},
		{
			name: "Disk URI not valid",
			testFunc: func(t *testing.T) {
				req := &csi.ControllerExpandVolumeRequest{
					VolumeId:      "vol_1",
					CapacityRange: stdCapRange,
				}

				ctx := context.Background()
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				d.(*fakeDriver).kubeClient = fake.NewClientset(newTestPV(req.VolumeId))

				expectedErr := status.Errorf(codes.Internal, "GetDiskByURI(vol_1) failed with error(invalid URI: vol_1)")
				_, err := d.ControllerExpandVolume(ctx, req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "DiskSize missing",
			testFunc: func(t *testing.T) {
				req := &csi.ControllerExpandVolumeRequest{
					VolumeId:      testVolumeID,
					CapacityRange: stdCapRange,
				}
				id := req.VolumeId
				diskProperties := armcompute.DiskProperties{}
				disk := &armcompute.Disk{
					ID:         &id,
					Properties: &diskProperties,
				}
				ctx := context.Background()
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				d.(*fakeDriver).kubeClient = fake.NewClientset(newTestPV(req.VolumeId))
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				expectedErr := status.Errorf(codes.Internal, "could not get size of the disk(/subscriptions/subs/resourceGroups/rg/providers/Microsoft.Compute/disks/unit-test-volume)")
				_, err := d.ControllerExpandVolume(ctx, req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			// Regression guard for the nil-dereference fix in this PR: the
			// disk client can legitimately return (nil, nil) (e.g. NotFound
			// mapped to a nil result with a nil error). Before the fix, the
			// controller dereferenced result.SKU before the nil check and
			// panicked; now it must return an Internal error naming the disk.
			name: "disk client returns (nil, nil) - no panic, Internal error",
			testFunc: func(t *testing.T) {
				req := &csi.ControllerExpandVolumeRequest{
					VolumeId:      testVolumeID,
					CapacityRange: stdCapRange,
				}
				ctx := context.Background()
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
				expectedErr := status.Errorf(codes.Internal, "could not get size of the disk(/subscriptions/subs/resourceGroups/rg/providers/Microsoft.Compute/disks/unit-test-volume)")
				_, err := d.ControllerExpandVolume(ctx, req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestCreateSnapshot(t *testing.T) {

	t.Logf("Wait for snapshot ready is set")
	RunTestCreateSnapshot(t, func(t *gomock.Controller) (FakeDriver, error) {
		return NewFakeDriver(t)
	})
	t.Logf("Wait for snapshot ready is cleared")
	RunTestCreateSnapshot(t, func(t *gomock.Controller) (FakeDriver, error) {
		driver, err := NewFakeDriver(t)
		if err != nil {
			return nil, err
		}
		driver.SetWaitForSnapshotReady(false)
		return driver, nil
	})
}

func RunTestCreateSnapshot(t *testing.T, fakeDriverFn func(t *gomock.Controller) (FakeDriver, error)) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Source volume ID missing",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				req := &csi.CreateSnapshotRequest{}
				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "CreateSnapshot Source Volume ID must be provided")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Snapshot name missing",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: "vol_1"}
				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Error(codes.InvalidArgument, "snapshot name must be provided")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Invalid parameter option",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				parameter := make(map[string]string)
				parameter["unit-test"] = "test"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: "vol_1",
					Name:           "snapname",
					Parameters:     parameter,
				}

				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.Internal, "AzureDisk - invalid option unit-test in VolumeSnapshotClass")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Invalid volume ID",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: "vol_1",
					Name:           "snapname",
				}
				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.InvalidArgument, "could not get resource group from diskURI(vol_1) with error(invalid URI: vol_1)")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Invalid tag ",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				parameter := make(map[string]string)
				parameter["tags"] = "unit-test"
				parameter[consts.IncrementalField] = "false"
				parameter[consts.ResourceGroupField] = "test"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}

				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.InvalidArgument, "tags 'unit-test' are invalid, the format should like: 'key1=value1,key2=value2'")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "invalid data access auth mode ",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["dataaccessauthmode"] = "Invalid"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})

				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.InvalidArgument, "dataAccessAuthMode(Invalid) is not supported")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "invalid network access policy ",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["networkaccesspolicy"] = "Invalid"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})

				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.InvalidArgument, "azureDisk - Invalid is not supported NetworkAccessPolicy. Supported values are [AllowAll AllowPrivate DenyAll]")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "invalid publicNetworkAccess ",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["publicnetworkaccess"] = "Invalid"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})

				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.InvalidArgument, "azureDisk - Invalid is not supported PublicNetworkAccess. Supported values are [Disabled Enabled]")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "cross region non-incremental error ",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["location"] = "eastus"
				parameter["incremental"] = "false"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)

				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.InvalidArgument, "could not create snapshot cross region with incremental is false")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "get snapshot client error ",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["SubscriptionID"] = "1"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(nil, fmt.Errorf("test")).AnyTimes()

				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.Internal, "could not get snapshot client for subscription(1) with error(test)")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "create snapshot error ",
			testFunc: func(t *testing.T) {
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("test")).AnyTimes()
				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("test")).AnyTimes()

				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.Internal, "create snapshot error: test")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "create snapshot already exist ",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["tags"] = "unit=test"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("existing disk")).AnyTimes()
				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.AlreadyExists, "request snapshot(snapname) under rg(rg) already exists, but the SourceVolumeId is different, error details: existing disk")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "create snapshot already exist - waits for snapshot ready",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["tags"] = "unit=test"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				timeCreated := ptr.To(time.Now())

				snapshotNotProvisioned := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						CreationData:      &armcompute.CreationData{SourceResourceID: &req.SourceVolumeId},
						TimeCreated:       timeCreated,
						DiskSizeGB:        ptr.To[int32](5),
						ProvisioningState: ptr.To("Updating"),
						CompletionPercent: ptr.To[float32](0),
					},
					ID:   ptr.To("subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/snapshots/snapname"),
					Name: ptr.To("snapname"),
				}
				snapshotProvisioned := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						CreationData:      &armcompute.CreationData{SourceResourceID: &req.SourceVolumeId},
						TimeCreated:       timeCreated,
						DiskSizeGB:        ptr.To[int32](5),
						CompletionPercent: ptr.To[float32](0),
						ProvisioningState: ptr.To("succeeded"),
					},
					ID:   ptr.To("subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/snapshots/snapname"),
					Name: ptr.To("snapname"),
				}
				snapshotComplete := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						CreationData:      &armcompute.CreationData{SourceResourceID: &req.SourceVolumeId},
						CompletionPercent: ptr.To[float32](100),
						ProvisioningState: ptr.To("succeeded"),
						TimeCreated:       timeCreated,
						DiskSizeGB:        ptr.To[int32](5),
					},
					ID:   ptr.To("subscriptions/subscription/resourceGroups/rg/providers/Microsoft.Compute/snapshots/snapname"),
					Name: ptr.To("snapname"),
				}

				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				if d.GetWaitForSnapshotReady() {
					mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshotNotProvisioned, nil).Times(3)
					mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshotProvisioned, nil).Times(2)
					mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshotComplete, nil).Times(2)
				} else {
					mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshotNotProvisioned, nil).Times(4)
					mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshotProvisioned, nil).Times(2)
					mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshotComplete, nil).Times(2)
				}
				resp, err := d.CreateSnapshot(context.Background(), req)
				if err == nil && !d.GetWaitForSnapshotReady() {
					for range 3 {
						resp, err = d.CreateSnapshot(context.Background(), req) // retry without waiting for snapshot ready
						if err != nil {
							break
						}
					}
				}
				if !reflect.DeepEqual(err, nil) {
					t.Errorf("actualErr: (%v), expectedErr: nil", err)
				} else if !resp.Snapshot.ReadyToUse {
					t.Errorf("Snapshot not ready to use, expected: true, got: %v", resp.Snapshot.ReadyToUse)
				}
			},
		},
		{
			name: "Wait snapshot ready error ",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["tags"] = "unit=test"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "unit-test",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()

				snapshot := &armcompute.Snapshot{}
				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, fmt.Errorf("get snapshot error")).AnyTimes()
				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.Internal, "waitForSnapshotReady(, rg, unit-test) failed with get snapshot error")
				if !d.GetWaitForSnapshotReady() {
					expectedErr = status.Errorf(codes.Internal, "get snapshot unit-test from rg(rg) error: get snapshot error")
				}
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Get snapshot ID error ",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["tags"] = "unit=test"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "unit-test",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()

				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
					},
					ID: &snapshotID,
				}
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
				if d.GetWaitForSnapshotReady() {
					gomock.InOrder(
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("get snapshot error")).AnyTimes(),
					)
				} else {
					gomock.InOrder(
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("get snapshot error")).AnyTimes(),
					)
				}
				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.Internal, "get snapshot unit-test from rg(rg) error: get snapshot error")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "create snapshot error - cross region",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["tags"] = "unit=test"
				parameter["location"] = "eastus"
				parameter["incremental"] = "true"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
					},
					ID: &snapshotID,
				}
				if d.GetWaitForSnapshotReady() {
					gomock.InOrder(
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).Times(2),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("test")).Times(1),
					)
				} else {
					gomock.InOrder(
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("test")).Times(1),
					)
				}
				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.Internal, "create snapshot error: test")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "create snapshot already exist - cross region",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["tags"] = "unit=test"
				parameter["location"] = "eastus"
				parameter["incremental"] = "true"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
					},
					ID: &snapshotID,
				}
				if d.GetWaitForSnapshotReady() {
					gomock.InOrder(
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).Times(2),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("existing disk")).Times(1),
					)
				} else {
					gomock.InOrder(
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("existing disk")).Times(1),
					)
				}
				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.AlreadyExists, "request snapshot(snapname) under rg(rg) already exists, but the SourceVolumeId is different, error details: existing disk")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Wait snapshot ready error - cross region",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["tags"] = "unit=test"
				parameter["location"] = "eastus"
				parameter["incremental"] = "true"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "unit-test",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()

				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
					},
					ID: &snapshotID,
				}
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
				if d.GetWaitForSnapshotReady() {
					gomock.InOrder(
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).Times(2),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("get snapshot error")).AnyTimes(),
					)
				} else {
					gomock.InOrder(
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("get snapshot error")).AnyTimes(),
					)
				}
				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.Internal, "waitForSnapshotReady(, rg, unit-test) failed with get snapshot error")
				if !d.GetWaitForSnapshotReady() {
					expectedErr = status.Errorf(codes.Internal, "rpc error: code = Internal desc = get snapshot unit-test from rg(rg) error: get snapshot error")
				}
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Get snapshot ID error - cross region",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["tags"] = "unit=test"
				parameter["location"] = "eastus"
				parameter["incremental"] = "true"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "unit-test",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()

				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
					},
					ID: &snapshotID,
				}
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
				if d.GetWaitForSnapshotReady() {
					gomock.InOrder(
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).Times(2),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("get snapshot error")).AnyTimes(),
					)
				} else {
					gomock.InOrder(
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("get snapshot error")).AnyTimes(),
					)
				}
				mockSnapshotClient.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.Internal, "rpc error: code = Internal desc = get snapshot unit-test from rg(rg) error: get snapshot error")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "valid request ",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["tags"] = "unit=test"
				parameter["publicNetworkAccess"] = "Enabled"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
					},
					ID: &snapshotID,
				}

				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).AnyTimes()
				actualresponse, err := d.CreateSnapshot(context.Background(), req)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				ready := true
				expectedresponse := &csi.CreateSnapshotResponse{
					Snapshot: &csi.Snapshot{
						SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
						SnapshotId:     *snapshot.ID,
						SourceVolumeId: req.SourceVolumeId,
						CreationTime:   tp,
						ReadyToUse:     ready,
					},
				}
				if !reflect.DeepEqual(expectedresponse, actualresponse) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", actualresponse, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
		{
			name: "valid request - set optional parameter",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["tags"] = "unit=test"
				parameter["dataaccessauthmode"] = "None"
				parameter["networkAccessPolicy"] = "AllowAll"
				parameter["publicNetworkAccess"] = "Enabled"
				parameter["tagvaluedelimiter"] = ","
				parameter["useragent"] = "ut"
				parameter["csi.storage.k8s.io/volumesnapshot/name"] = "VolumeSnapshotNameKeyPlaceholder"
				parameter["csi.storage.k8s.io/volumesnapshot/namespace"] = "VolumeSnapshotNamespaceKeyPlaceholder"
				parameter["csi.storage.k8s.io/volumesnapshotcontent/name"] = "VolumeSnapshotContentNameKeyPlaceholder"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(azure.GetTestCloudWithExtendedLocation(cntl))
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
					},
					ID: &snapshotID,
				}

				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).AnyTimes()
				actualresponse, err := d.CreateSnapshot(context.Background(), req)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				ready := true
				expectedresponse := &csi.CreateSnapshotResponse{
					Snapshot: &csi.Snapshot{
						SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
						SnapshotId:     *snapshot.ID,
						SourceVolumeId: req.SourceVolumeId,
						CreationTime:   tp,
						ReadyToUse:     ready,
					},
				}
				if !reflect.DeepEqual(expectedresponse, actualresponse) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", actualresponse, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
		{
			name: "valid request - azure stack",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)

				az := azure.GetTestCloud(cntl)
				az.Config.Cloud = "AZURESTACKCLOUD"
				d.setCloud(az)
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
					},
					ID: &snapshotID,
				}

				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).AnyTimes()
				actualresponse, err := d.CreateSnapshot(context.Background(), req)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				ready := true
				expectedresponse := &csi.CreateSnapshotResponse{
					Snapshot: &csi.Snapshot{
						SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
						SnapshotId:     *snapshot.ID,
						SourceVolumeId: req.SourceVolumeId,
						CreationTime:   tp,
						ReadyToUse:     ready,
					},
				}
				if !reflect.DeepEqual(expectedresponse, actualresponse) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", actualresponse, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
		{
			name: "valid request - cross region",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["location"] = "eastus"
				parameter["incremental"] = "true"
				parameter["networkAccessPolicy"] = "AllowAll"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
					},
					ID: &snapshotID,
				}

				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				actualresponse, err := d.CreateSnapshot(context.Background(), req)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				ready := true
				expectedresponse := &csi.CreateSnapshotResponse{
					Snapshot: &csi.Snapshot{
						SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
						SnapshotId:     *snapshot.ID,
						SourceVolumeId: req.SourceVolumeId,
						CreationTime:   tp,
						ReadyToUse:     ready,
					},
				}
				if !reflect.DeepEqual(expectedresponse, actualresponse) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", actualresponse, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
		{
			name: "valid request snapshots taking time - cross region",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["location"] = "eastus"
				parameter["incremental"] = "true"
				snapshotName := "snapshotname"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           snapshotName,
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				DiskSize := int32(10)
				localSnapshotName := fmt.Sprintf("local_%s", snapshotName)
				snapshotURI := "/subscriptions/23/providers/Microsoft.Compute/snapshots/"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated: &time.Time{},
						DiskSizeGB:  &DiskSize,
					},
					ID: ptr.To(fmt.Sprintf("%s%s", snapshotURI, snapshotName)),
				}

				if d.GetWaitForSnapshotReady() {
					mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
					gomock.InOrder(
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ string) (*armcompute.Snapshot, error) {
							snapshot.ID = ptr.To(fmt.Sprintf("%s%s", snapshotURI, localSnapshotName))
							snapshot.Name = ptr.To(localSnapshotName)
							snapshot.Properties.ProvisioningState = ptr.To("updating")
							snapshot.Properties.CompletionPercent = ptr.To(float32(0.0))
							return snapshot, nil
						}).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ string) (*armcompute.Snapshot, error) {
							snapshot.ID = ptr.To(fmt.Sprintf("%s%s", snapshotURI, localSnapshotName))
							snapshot.Name = ptr.To(localSnapshotName)
							snapshot.Properties.ProvisioningState = ptr.To("succeeded")
							snapshot.Properties.CompletionPercent = ptr.To(float32(100.0))
							return snapshot, nil
						}).Times(2),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ string) (*armcompute.Snapshot, error) {
							snapshot.ID = ptr.To(fmt.Sprintf("%s%s", snapshotURI, snapshotName))
							snapshot.Name = ptr.To(snapshotName)
							snapshot.Properties.ProvisioningState = ptr.To("updating")
							snapshot.Properties.CompletionPercent = ptr.To(float32(0.0))
							return snapshot, nil
						}).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ string) (*armcompute.Snapshot, error) {
							snapshot.ID = ptr.To(fmt.Sprintf("%s%s", snapshotURI, snapshotName))
							snapshot.Name = ptr.To(snapshotName)
							snapshot.Properties.ProvisioningState = ptr.To("succeeded")
							snapshot.Properties.CompletionPercent = ptr.To(float32(100.0))
							return snapshot, nil
						}).Times(2),
					)
					mockSnapshotClient.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
				} else {
					mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
					gomock.InOrder(
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ string) (*armcompute.Snapshot, error) {
							snapshot.ID = ptr.To(fmt.Sprintf("%s%s", snapshotURI, localSnapshotName))
							snapshot.Name = ptr.To(localSnapshotName)
							snapshot.Properties.ProvisioningState = ptr.To("updating")
							return snapshot, nil
						}).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ string) (*armcompute.Snapshot, error) {
							snapshot.ID = ptr.To(fmt.Sprintf("%s%s", snapshotURI, localSnapshotName))
							snapshot.Name = ptr.To(localSnapshotName)
							snapshot.Properties.ProvisioningState = ptr.To("succeeded")
							return snapshot, nil
						}).Times(2),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ string) (*armcompute.Snapshot, error) {
							snapshot.ID = ptr.To(fmt.Sprintf("%s%s", snapshotURI, snapshotName))
							snapshot.Name = ptr.To(snapshotName)
							snapshot.Properties.ProvisioningState = ptr.To("updating")
							return snapshot, nil
						}).Times(1),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ string) (*armcompute.Snapshot, error) {
							snapshot.ID = ptr.To(fmt.Sprintf("%s%s", snapshotURI, localSnapshotName))
							snapshot.Name = ptr.To(localSnapshotName)
							snapshot.Properties.ProvisioningState = ptr.To("succeeded")
							return snapshot, nil
						}).Times(2),
						mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ string) (*armcompute.Snapshot, error) {
							snapshot.ID = ptr.To(fmt.Sprintf("%s%s", snapshotURI, snapshotName))
							snapshot.Name = ptr.To(snapshotName)
							snapshot.Properties.ProvisioningState = ptr.To("succeeded")
							return snapshot, nil
						}).Times(2),
					)
					mockSnapshotClient.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
				}

				actualresponse, err := d.CreateSnapshot(context.Background(), req)
				if err == nil && !actualresponse.Snapshot.ReadyToUse {
					for range 2 {
						if actualresponse.Snapshot.SnapshotId != fmt.Sprintf("%s%s", snapshotURI, snapshotName) {
							err = fmt.Errorf("snapshot ID mismatch")
						} else {
							actualresponse, err = d.CreateSnapshot(context.Background(), req)
							if err != nil {
								break
							}
						}
					}
				}
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				ready := true
				expectedresponse := &csi.CreateSnapshotResponse{
					Snapshot: &csi.Snapshot{
						SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
						SnapshotId:     fmt.Sprintf("%s%s", snapshotURI, snapshotName),
						SourceVolumeId: req.SourceVolumeId,
						CreationTime:   tp,
						ReadyToUse:     ready,
					},
				}
				if !reflect.DeepEqual(expectedresponse, actualresponse) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", actualresponse, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
		{
			name: "valid request - cross region with delete error still success",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["tags"] = "unit=test"
				parameter["location"] = "eastus"
				parameter["incremental"] = "true"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "unit-test",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()

				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
					},
					ID: &snapshotID,
				}
				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("test")).AnyTimes()
				actualresponse, err := d.CreateSnapshot(context.Background(), req)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				ready := true
				expectedresponse := &csi.CreateSnapshotResponse{
					Snapshot: &csi.Snapshot{
						SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
						SnapshotId:     *snapshot.ID,
						SourceVolumeId: req.SourceVolumeId,
						CreationTime:   tp,
						ReadyToUse:     ready,
					},
				}
				if !reflect.DeepEqual(expectedresponse, actualresponse) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", actualresponse, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
		{
			name: "invalid instantAccessDurationMinutes - non-numeric",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["instantaccessdurationminutes"] = "abc"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})

				_, err := d.CreateSnapshot(context.Background(), req)
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
					t.Errorf("expected InvalidArgument error, got: %v", err)
				}
			},
		},
		{
			name: "invalid instantAccessDurationMinutes - below range",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["instantaccessdurationminutes"] = "59"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})

				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.InvalidArgument, "invalid value(%d) for %s: must be between 60 and 300 minutes", 59, "instantaccessdurationminutes")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "invalid instantAccessDurationMinutes - above range",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["instantaccessdurationminutes"] = "301"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "snapname",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(&azure.Cloud{})

				_, err := d.CreateSnapshot(context.Background(), req)
				expectedErr := status.Errorf(codes.InvalidArgument, "invalid value(%d) for %s: must be between 60 and 300 minutes", 301, "instantaccessdurationminutes")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "valid instantAccessDurationMinutes - lower bound",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["instantaccessdurationminutes"] = "60"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(azure.GetTestCloudWithExtendedLocation(cntl))
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
						CreationData: &armcompute.CreationData{
							InstantAccessDurationMinutes: ptr.To(int64(60)),
						},
					},
					ID: &snapshotID,
				}

				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).AnyTimes()
				actualresponse, err := d.CreateSnapshot(context.Background(), req)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				ready := true
				expectedresponse := &csi.CreateSnapshotResponse{
					Snapshot: &csi.Snapshot{
						SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
						SnapshotId:     *snapshot.ID,
						SourceVolumeId: req.SourceVolumeId,
						CreationTime:   tp,
						ReadyToUse:     ready,
					},
				}
				if !reflect.DeepEqual(expectedresponse, actualresponse) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", actualresponse, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
		{
			name: "valid instantAccessDurationMinutes - upper bound",
			testFunc: func(t *testing.T) {
				parameter := make(map[string]string)
				parameter["instantaccessdurationminutes"] = "300"
				req := &csi.CreateSnapshotRequest{
					SourceVolumeId: testVolumeID,
					Name:           "testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name",
					Parameters:     parameter,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := fakeDriverFn(cntl)
				d.setCloud(azure.GetTestCloudWithExtendedLocation(cntl))
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
						CreationData: &armcompute.CreationData{
							InstantAccessDurationMinutes: ptr.To(int64(300)),
						},
					},
					ID: &snapshotID,
				}

				mockSnapshotClient.EXPECT().CreateOrUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).AnyTimes()
				actualresponse, err := d.CreateSnapshot(context.Background(), req)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				ready := true
				expectedresponse := &csi.CreateSnapshotResponse{
					Snapshot: &csi.Snapshot{
						SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
						SnapshotId:     *snapshot.ID,
						SourceVolumeId: req.SourceVolumeId,
						CreationTime:   tp,
						ReadyToUse:     ready,
					},
				}
				if !reflect.DeepEqual(expectedresponse, actualresponse) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", actualresponse, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestDeleteSnapshot(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Snapshot ID missing",
			testFunc: func(t *testing.T) {
				req := &csi.DeleteSnapshotRequest{}
				expectedErr := status.Error(codes.InvalidArgument, "Snapshot ID must be provided")
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				_, err := d.DeleteSnapshot(context.Background(), req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Snapshot ID invalid",
			testFunc: func(t *testing.T) {
				req := &csi.DeleteSnapshotRequest{
					SnapshotId: "/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name",
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				expectedErr := status.Errorf(codes.InvalidArgument, "invalid URI: /subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name")
				_, err := d.DeleteSnapshot(context.Background(), req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "delete Snapshot error",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				req := &csi.DeleteSnapshotRequest{
					SnapshotId: "testurl/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/snapshots/snapshot-name",
				}
				mockSnapshotClient.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("get snapshot error")).AnyTimes()
				expectedErr := status.Errorf(codes.Internal, "delete snapshot error: get snapshot error")
				_, err := d.DeleteSnapshot(context.Background(), req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Valid delete Snapshot ",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				req := &csi.DeleteSnapshotRequest{
					SnapshotId: "testurl/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/snapshots/snapshot-name",
				}
				mockSnapshotClient.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				_, err := d.DeleteSnapshot(context.Background(), req)
				if !reflect.DeepEqual(err, nil) {
					t.Errorf("actualErr: (%v), expectedErr: nil)", err)
				}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestGetSnapshotByID(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "snapshotID not valid",
			testFunc: func(t *testing.T) {
				sourceVolumeID := "unit-test"
				ctx := context.Background()
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				d.setCloud(&azure.Cloud{})
				snapshotID := "testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name"
				expectedErr := status.Errorf(codes.InvalidArgument, "invalid URI: testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name")
				_, err := d.getSnapshotByID(ctx, d.getCloud().SubscriptionID, d.getCloud().ResourceGroup, snapshotID, sourceVolumeID)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "snapshot get error",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				d.setCloud(&azure.Cloud{})
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				snapshotID := "testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{},
					ID:         &snapshotID,
				}
				snapshotVolumeID := "unit-test"
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, fmt.Errorf("test")).AnyTimes()
				expectedErr := status.Errorf(codes.InvalidArgument, "invalid URI: testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name")
				_, err := d.getSnapshotByID(context.Background(), d.getCloud().SubscriptionID, d.getCloud().ResourceGroup, snapshotID, snapshotVolumeID)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestListSnapshots(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "snapshotID not valid",
			testFunc: func(t *testing.T) {
				req := csi.ListSnapshotsRequest{
					SnapshotId: "testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-nametestVolumeName",
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				expectedErr := status.Errorf(codes.InvalidArgument, "invalid URI: testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-nametestVolumeName")
				_, err := d.ListSnapshots(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "valid List",
			testFunc: func(t *testing.T) {
				req := csi.ListSnapshotsRequest{
					SnapshotId: "testurl/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/snapshots/snapshot-name",
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
					},
					ID: &snapshotID,
				}
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClientForSub(gomock.Any()).Return(mockSnapshotClient, nil).AnyTimes()
				mockSnapshotClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(snapshot, nil).AnyTimes()
				expectedErr := error(nil)
				_, err := d.ListSnapshots(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "List resource error",
			testFunc: func(t *testing.T) {
				req := csi.ListSnapshotsRequest{}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				snapshot := &armcompute.Snapshot{}
				snapshots := []*armcompute.Snapshot{}
				snapshots = append(snapshots, snapshot)
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClient().Return(mockSnapshotClient).AnyTimes()
				mockSnapshotClient.EXPECT().List(gomock.Any(), gomock.Any()).Return(snapshots, fmt.Errorf("test")).AnyTimes()
				expectedErr := status.Error(codes.Internal, "Unknown list snapshot error: test")
				_, err := d.ListSnapshots(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "snapshot property nil",
			testFunc: func(t *testing.T) {
				req := csi.ListSnapshotsRequest{}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				snapshot := &armcompute.Snapshot{}
				snapshots := []*armcompute.Snapshot{}
				snapshots = append(snapshots, snapshot)
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClient().Return(mockSnapshotClient).AnyTimes()
				mockSnapshotClient.EXPECT().List(gomock.Any(), gomock.Any()).Return(snapshots, nil).AnyTimes()
				expectedErr := fmt.Errorf("failed to generate snapshot entry: snapshot property is nil")
				_, err := d.ListSnapshots(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "List snapshots when source volumeId is given",
			testFunc: func(t *testing.T) {
				req := csi.ListSnapshotsRequest{SourceVolumeId: "test"}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				volumeID := "test"
				DiskSize := int32(10)
				snapshotID := "test"
				provisioningState := "succeeded"
				snapshot1 := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
						CreationData:      &armcompute.CreationData{SourceResourceID: &volumeID},
					},
					ID: &snapshotID}
				snapshot2 := &armcompute.Snapshot{}
				snapshots := []*armcompute.Snapshot{}
				snapshots = append(snapshots, snapshot1, snapshot2)
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				mockSnapshotClient := mock_snapshotclient.NewMockInterface(ctrl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetSnapshotClient().Return(mockSnapshotClient).AnyTimes()
				mockSnapshotClient.EXPECT().List(gomock.Any(), gomock.Any()).Return(snapshots, nil).AnyTimes()
				snapshotsResponse, _ := d.ListSnapshots(context.TODO(), &req)
				if len(snapshotsResponse.Entries) != 1 {
					t.Errorf("actualNumberOfEntries: (%v), expectedNumberOfEntries: (%v)", len(snapshotsResponse.Entries), 1)
				}
				if snapshotsResponse.Entries[0].Snapshot.SourceVolumeId != volumeID {
					t.Errorf("actualVolumeId: (%v), expectedVolumeId: (%v)", snapshotsResponse.Entries[0].Snapshot.SourceVolumeId, volumeID)
				}
				if snapshotsResponse.NextToken != "" {
					t.Errorf("actualNextToken: (%v), expectedNextToken: (%v)", snapshotsResponse.NextToken, "")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}

}
func TestGetCapacity(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
	req := csi.GetCapacityRequest{}
	resp, err := d.GetCapacity(context.Background(), &req)
	assert.Nil(t, resp)
	if !reflect.DeepEqual(err, status.Error(codes.Unimplemented, "")) {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestListVolumes(t *testing.T) {
	volume1 := v1.PersistentVolume{
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       "disk.csi.azure.com",
					VolumeHandle: "/subscriptions/test-subscription/resourceGroups/test_resourcegroup-1/providers/Microsoft.Compute/disks/test-pv-1",
				},
			},
		},
	}
	volume2 := v1.PersistentVolume{
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       "disk.csi.azure.com",
					VolumeHandle: "/subscriptions/test-subscription/resourceGroups/test_resourcegroup-2/providers/Microsoft.Compute/disks/test-pv-2",
				},
			},
		},
	}

	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "When no KubeClient exists, Valid list without max_entries or starting_token",
			testFunc: func(t *testing.T) {
				t.SkipNow() //todo: fix this test
				req := csi.ListVolumesRequest{}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				fakeVolumeID := "test"
				disk := &armcompute.Disk{ID: &fakeVolumeID}
				disks := []*armcompute.Disk{}
				disks = append(disks, disk)
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClient().Return(diskClient).AnyTimes()
				diskClient.EXPECT().List(gomock.Any(), gomock.Any()).Return(disks, nil).AnyTimes()
				expectedErr := error(nil)
				listVolumesResponse, err := d.ListVolumes(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
				if listVolumesResponse.NextToken != "" {
					t.Errorf("actualNextToken: (%v), expectedNextToken: (%v)", listVolumesResponse.NextToken, "")
				}
			},
		},
		{
			name: "When no KubeClient exists, Valid list with max_entries",
			testFunc: func(t *testing.T) {
				t.SkipNow() //todo: fix this test
				req := csi.ListVolumesRequest{
					MaxEntries: 1,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				fakeVolumeID := "test"
				disk1, disk2 := &armcompute.Disk{ID: &fakeVolumeID}, &armcompute.Disk{ID: &fakeVolumeID}
				disks := []*armcompute.Disk{}
				disks = append(disks, disk1, disk2)
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClient().Return(diskClient).AnyTimes()
				diskClient.EXPECT().List(gomock.Any(), gomock.Any()).Return(disks, nil).AnyTimes()
				expectedErr := error(nil)
				listVolumesResponse, err := d.ListVolumes(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
				if len(listVolumesResponse.Entries) != int(req.MaxEntries) {
					t.Errorf("Actual number of entries: (%v), Expected number of entries: (%v)", len(listVolumesResponse.Entries), req.MaxEntries)
				}
				if listVolumesResponse.NextToken != "1" {
					t.Errorf("actualNextToken: (%v), expectedNextToken: (%v)", listVolumesResponse.NextToken, "1")
				}
			},
		},
		{
			name: "When no KubeClient exists, Valid list with max_entries and starting_token",
			testFunc: func(t *testing.T) {
				t.SkipNow() //todo: fix this test
				req := csi.ListVolumesRequest{
					StartingToken: "1",
					MaxEntries:    1,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				fakeVolumeID1, fakeVolumeID12 := "test1", "test2"
				disk1, disk2 := &armcompute.Disk{ID: &fakeVolumeID1}, &armcompute.Disk{ID: &fakeVolumeID12}
				disks := []*armcompute.Disk{}
				disks = append(disks, disk1, disk2)
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClient().Return(diskClient).AnyTimes()
				diskClient.EXPECT().List(gomock.Any(), gomock.Any()).Return(disks, nil).AnyTimes()
				expectedErr := error(nil)
				listVolumesResponse, err := d.ListVolumes(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
				if len(listVolumesResponse.Entries) != int(req.MaxEntries) {
					t.Errorf("Actual number of entries: (%v), Expected number of entries: (%v)", len(listVolumesResponse.Entries), req.MaxEntries)
				}
				if listVolumesResponse.NextToken != "" {
					t.Errorf("actualNextToken: (%v), expectedNextToken: (%v)", listVolumesResponse.NextToken, "")
				}
				if listVolumesResponse.Entries[0].Volume.VolumeId != fakeVolumeID12 {
					t.Errorf("actualVolumeId: (%v), expectedVolumeId: (%v)", listVolumesResponse.Entries[0].Volume.VolumeId, fakeVolumeID12)
				}
			},
		},
		{
			name: "When no KubeClient exists, ListVolumes request with starting token but no entries in response",
			testFunc: func(t *testing.T) {
				t.SkipNow() //todo: fix this test
				req := csi.ListVolumesRequest{
					StartingToken: "1",
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				disks := []*armcompute.Disk{}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClient().Return(diskClient).AnyTimes()
				diskClient.EXPECT().List(gomock.Any(), gomock.Any()).Return(disks, nil).AnyTimes()
				expectedErr := status.Error(codes.FailedPrecondition, "ListVolumes starting token(1) on rg(rg) is greater than total number of volumes")
				_, err := d.ListVolumes(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "When no KubeClient exists, ListVolumes list resource error",
			testFunc: func(t *testing.T) {
				t.SkipNow() //todo: fix this test
				req := csi.ListVolumesRequest{
					StartingToken: "1",
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				disks := []*armcompute.Disk{}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClient().Return(diskClient).AnyTimes()
				diskClient.EXPECT().List(gomock.Any(), gomock.Any()).Return(disks, fmt.Errorf("test")).AnyTimes()
				expectedErr := status.Error(codes.Internal, "ListVolumes on rg(rg) failed with error: test")
				_, err := d.ListVolumes(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "When KubeClient exists, Empty list without start token should not return error",
			testFunc: func(t *testing.T) {
				req := csi.ListVolumesRequest{}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d := getFakeDriverWithKubeClient(cntl)
				pvList := v1.PersistentVolumeList{
					Items: []v1.PersistentVolume{},
				}
				d.getCloud().KubeClient.CoreV1().PersistentVolumes().(*mockpersistentvolume.MockInterface).EXPECT().List(gomock.Any(), gomock.Any()).Return(&pvList, nil)
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClient().Return(diskClient).AnyTimes()
				diskClient.EXPECT().List(gomock.Any(), gomock.Any()).Return([]*armcompute.Disk{}, nil).AnyTimes()
				expectedErr := error(nil)
				_, err := d.ListVolumes(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "When KubeClient exists, Valid list without max_entries or starting_token",
			testFunc: func(t *testing.T) {
				t.SkipNow() //todo: fix this test
				req := csi.ListVolumesRequest{}
				fakeVolumeID := "/subscriptions/test-subscription/resourceGroups/test_resourcegroup-1/providers/Microsoft.Compute/disks/test-pv-1"
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d := getFakeDriverWithKubeClient(cntl)
				pvList := v1.PersistentVolumeList{
					Items: []v1.PersistentVolume{volume1},
				}
				d.getCloud().KubeClient.CoreV1().PersistentVolumes().(*mockpersistentvolume.MockInterface).EXPECT().List(gomock.Any(), gomock.Any()).Return(&pvList, nil)
				disk1 := &armcompute.Disk{ID: &fakeVolumeID}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClient().Return(diskClient).AnyTimes()
				diskClient.EXPECT().List(gomock.Any(), gomock.Any()).Return([]*armcompute.Disk{disk1}, nil).AnyTimes()
				expectedErr := error(nil)
				listVolumesResponse, err := d.ListVolumes(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
				if listVolumesResponse.NextToken != "" {
					t.Errorf("actualNextToken: (%v), expectedNextToken: (%v)", listVolumesResponse.NextToken, "")
				}
			},
		},
		{
			name: "When KubeClient exists, Valid list with max_entries",
			testFunc: func(t *testing.T) {
				t.SkipNow() //todo: fix this test
				req := csi.ListVolumesRequest{
					MaxEntries: 1,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d := getFakeDriverWithKubeClient(cntl)
				d.getCloud().SubscriptionID = "test-subscription"
				fakeVolumeID := "/subscriptions/test-subscription/resourceGroups/test_resourcegroup-1/providers/Microsoft.Compute/disks/test-pv-1"
				disk1, disk2 := &armcompute.Disk{ID: &fakeVolumeID}, &armcompute.Disk{ID: &fakeVolumeID}
				pvList := v1.PersistentVolumeList{
					Items: []v1.PersistentVolume{volume1, volume2},
				}
				d.getCloud().KubeClient.CoreV1().PersistentVolumes().(*mockpersistentvolume.MockInterface).EXPECT().List(gomock.Any(), gomock.Any()).Return(&pvList, nil)
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClient().Return(diskClient).AnyTimes()
				diskClient.EXPECT().List(gomock.Any(), gomock.Any()).Return([]*armcompute.Disk{disk1}, nil).AnyTimes()
				diskClient.EXPECT().List(gomock.Any(), gomock.Any()).Return([]*armcompute.Disk{disk2}, nil).AnyTimes()
				expectedErr := error(nil)
				listVolumesResponse, err := d.ListVolumes(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
				if len(listVolumesResponse.Entries) != int(req.MaxEntries) {
					t.Errorf("Actual number of entries: (%v), Expected number of entries: (%v)", len(listVolumesResponse.Entries), req.MaxEntries)
				}
				if listVolumesResponse.NextToken != "1" {
					t.Errorf("actualNextToken: (%v), expectedNextToken: (%v)", listVolumesResponse.NextToken, "1")
				}
			},
		},
		{
			name: "When KubeClient exists, Valid list with max_entries and starting_token",
			testFunc: func(t *testing.T) {
				t.SkipNow() //todo: fix this test
				req := csi.ListVolumesRequest{
					StartingToken: "1",
					MaxEntries:    1,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d := getFakeDriverWithKubeClient(cntl)
				d.getCloud().SubscriptionID = "test-subscription"
				pvList := v1.PersistentVolumeList{
					Items: []v1.PersistentVolume{volume1, volume2},
				}
				fakeVolumeID11, fakeVolumeID12 := "/subscriptions/test-subscription/resourceGroups/test_resourcegroup-1/providers/Microsoft.Compute/disks/test-pv-1", "/subscriptions/test-subscription/resourceGroups/test_resourcegroup-2/providers/Microsoft.Compute/disks/test-pv-2"
				disk1, disk2 := &armcompute.Disk{ID: &fakeVolumeID11}, &armcompute.Disk{ID: &fakeVolumeID12}
				d.getCloud().KubeClient.CoreV1().PersistentVolumes().(*mockpersistentvolume.MockInterface).EXPECT().List(gomock.Any(), gomock.Any()).Return(&pvList, nil)
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClient().Return(diskClient).AnyTimes()
				diskClient.EXPECT().List(gomock.Any(), gomock.Any()).Return([]*armcompute.Disk{disk1}, nil).AnyTimes()
				diskClient.EXPECT().List(gomock.Any(), gomock.Any()).Return([]*armcompute.Disk{disk2}, nil).AnyTimes()
				expectedErr := error(nil)
				listVolumesResponse, err := d.ListVolumes(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
				if len(listVolumesResponse.Entries) != int(req.MaxEntries) {
					t.Errorf("Actual number of entries: (%v), Expected number of entries: (%v)", len(listVolumesResponse.Entries), req.MaxEntries)
				}
				if listVolumesResponse.NextToken != "" {
					t.Errorf("actualNextToken: (%v), expectedNextToken: (%v)", listVolumesResponse.NextToken, "")
				}
				if listVolumesResponse.Entries[0].Volume.VolumeId != fakeVolumeID11 {
					t.Errorf("actualVolumeId: (%v), expectedVolumeId: (%v)", listVolumesResponse.Entries[0].Volume.VolumeId, fakeVolumeID11)
				}
			},
		},
		{
			name: "When KubeClient exists, ListVolumes request with starting token but no entries in response",
			testFunc: func(t *testing.T) {
				req := csi.ListVolumesRequest{
					StartingToken: "1",
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d := getFakeDriverWithKubeClient(cntl)
				pvList := v1.PersistentVolumeList{
					Items: []v1.PersistentVolume{},
				}
				d.getCloud().KubeClient.CoreV1().PersistentVolumes().(*mockpersistentvolume.MockInterface).EXPECT().List(gomock.Any(), gomock.Any()).Return(&pvList, nil)
				expectedErr := status.Error(codes.FailedPrecondition, "ListVolumes starting token(1) is greater than total number of disks")
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClient().Return(diskClient).AnyTimes()
				_, err := d.ListVolumes(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "When KubeClient exists, ListVolumes list pv error",
			testFunc: func(t *testing.T) {
				req := csi.ListVolumesRequest{
					StartingToken: "1",
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d := getFakeDriverWithKubeClient(cntl)
				rerr := fmt.Errorf("test")
				d.getCloud().KubeClient.CoreV1().PersistentVolumes().(*mockpersistentvolume.MockInterface).EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, rerr)
				expectedErr := status.Error(codes.Internal, "ListVolumes failed while fetching PersistentVolumes List with error: test")
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClient().Return(diskClient).AnyTimes()
				_, err := d.ListVolumes(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestValidateVolumeCapabilities(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Volume ID missing ",
			testFunc: func(t *testing.T) {
				req := csi.ValidateVolumeCapabilitiesRequest{}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				expectedErr := status.Errorf(codes.InvalidArgument, "Volume ID missing in the request")
				_, err := d.ValidateVolumeCapabilities(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "Volume capabilities missing ",
			testFunc: func(t *testing.T) {
				req := csi.ValidateVolumeCapabilitiesRequest{
					VolumeId: "unit-test",
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				expectedErr := status.Errorf(codes.InvalidArgument, "VolumeCapabilities missing in the request")
				_, err := d.ValidateVolumeCapabilities(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "check disk err ",
			testFunc: func(t *testing.T) {
				req := csi.ValidateVolumeCapabilitiesRequest{
					VolumeId:           "-",
					VolumeCapabilities: stdVolumeCapabilities,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				expectedErr := status.Errorf(codes.NotFound, "Volume not found, failed with error: invalid URI: -")
				_, err := d.ValidateVolumeCapabilities(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "invalid req ",
			testFunc: func(t *testing.T) {
				req := csi.ValidateVolumeCapabilitiesRequest{
					VolumeId:           testVolumeID,
					VolumeCapabilities: stdVolumeCapabilities,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				disk := &armcompute.Disk{
					Properties: &armcompute.DiskProperties{},
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				expectedErr := error(nil)
				_, err := d.ValidateVolumeCapabilities(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "valid req ",
			testFunc: func(t *testing.T) {
				stdVolumeCapabilitytest := &csi.VolumeCapability{
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
					},
				}
				stdVolumeCapabilitiestest := []*csi.VolumeCapability{
					stdVolumeCapabilitytest,
				}
				req := csi.ValidateVolumeCapabilitiesRequest{
					VolumeId:           testVolumeID,
					VolumeCapabilities: stdVolumeCapabilitiestest,
				}
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				disk := &armcompute.Disk{
					Properties: &armcompute.DiskProperties{},
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				expectedErr := error(nil)
				_, err := d.ValidateVolumeCapabilities(context.TODO(), &req)
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestGetSourceDiskSize(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "max depth reached",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				_, _, err := d.GetSourceDiskSize(context.Background(), "", "test-rg", "test-disk", 2, 1)
				expectedErr := status.Errorf(codes.Internal, "current depth (2) surpassed the max depth (1) while searching for the source disk size")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "diskproperty not found",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				disk := &armcompute.Disk{}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()

				_, _, err := d.GetSourceDiskSize(context.Background(), "", "test-rg", "test-disk", 0, 1)
				expectedErr := status.Error(codes.Internal, "DiskProperty not found for disk (test-disk) in resource group (test-rg)")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "nil DiskSizeGB",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				diskProperties := armcompute.DiskProperties{}
				disk := &armcompute.Disk{
					Properties: &diskProperties,
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				_, _, err := d.GetSourceDiskSize(context.Background(), "", "test-rg", "test-disk", 0, 1)
				expectedErr := status.Error(codes.Internal, "DiskSizeGB for disk (test-disk) in resourcegroup (test-rg) is nil")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "successful search: depth 1",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				diskSizeGB := int32(8)
				diskProperties := armcompute.DiskProperties{
					DiskSizeGB: &diskSizeGB,
				}
				disk := &armcompute.Disk{
					Properties: &diskProperties,
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk, nil).AnyTimes()
				size, _, _ := d.GetSourceDiskSize(context.Background(), "", "test-rg", "test-disk", 0, 1)
				expectedOutput := diskSizeGB
				if *size != expectedOutput {
					t.Errorf("actualOutput: (%v), expectedOutput: (%v)", *size, expectedOutput)
				}
			},
		},
		{
			name: "successful search: depth 2",
			testFunc: func(t *testing.T) {
				cntl := gomock.NewController(t)
				defer cntl.Finish()
				d, _ := NewFakeDriver(cntl)
				diskSizeGB1 := int32(16)
				diskSizeGB2 := int32(8)
				sourceURI := "/subscriptions/xxxxxxxx/resourcegroups/test-rg/providers/microsoft.compute/disks/test-disk-1"
				creationData := armcompute.CreationData{
					CreateOption: to.Ptr(armcompute.DiskCreateOptionCopy),
					SourceURI:    &sourceURI,
				}
				diskProperties1 := armcompute.DiskProperties{
					CreationData: &creationData,
					DiskSizeGB:   &diskSizeGB1,
				}
				diskProperties2 := armcompute.DiskProperties{
					DiskSizeGB: &diskSizeGB2,
				}
				disk1 := &armcompute.Disk{
					Properties: &diskProperties1,
				}
				disk2 := &armcompute.Disk{
					Properties: &diskProperties2,
				}
				diskClient := mock_diskclient.NewMockInterface(cntl)
				d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(gomock.Any()).Return(diskClient, nil).AnyTimes()
				diskClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(disk1, nil).Return(disk2, nil).AnyTimes()
				size, _, _ := d.GetSourceDiskSize(context.Background(), "", "test-rg", "test-disk-1", 0, 2)
				expectedOutput := diskSizeGB2
				if *size != expectedOutput {
					t.Errorf("actualOutput: (%v), expectedOutput: (%v)", *size, expectedOutput)
				}
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}
}

func TestGetSnapshotSKU(t *testing.T) {
	type testCase struct {
		name            string
		snapshotURI     string
		setupMocks      func(factory *mock_azclient.MockClientFactory, snap *mock_snapshotclient.MockInterface)
		expectedSKU     string
		expectFactory   bool
		expectErrSubstr string
		expectGRPCCode  codes.Code
	}
	tests := []testCase{
		{
			name:          "success premium",
			snapshotURI:   "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/snapshots/snap",
			expectFactory: true,
			expectedSKU:   string(armcompute.SnapshotStorageAccountTypesPremiumLRS),
			setupMocks: func(f *mock_azclient.MockClientFactory, s *mock_snapshotclient.MockInterface) {
				f.EXPECT().GetSnapshotClientForSub("sub").Return(s, nil)
				s.EXPECT().
					Get(gomock.Any(), "rg", "snap").
					Return(&armcompute.Snapshot{
						SKU: &armcompute.SnapshotSKU{
							Name: to.Ptr(armcompute.SnapshotStorageAccountTypesPremiumLRS),
						},
					}, nil)
			},
		},
		{
			name:            "bad URI",
			snapshotURI:     "bad-uri",
			expectErrSubstr: "invalid URI",
			expectGRPCCode:  codes.NotFound,
			setupMocks:      func(_ *mock_azclient.MockClientFactory, _ *mock_snapshotclient.MockInterface) {},
		},
		{
			name:            "factory error -> empty string",
			snapshotURI:     "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/snapshots/snap",
			expectFactory:   true,
			expectedSKU:     "",
			expectErrSubstr: "factory error",
			setupMocks: func(f *mock_azclient.MockClientFactory, _ *mock_snapshotclient.MockInterface) {
				f.EXPECT().GetSnapshotClientForSub("sub").Return(nil, fmt.Errorf("factory error"))
			},
		},
		{
			name:            "get error -> empty string",
			snapshotURI:     "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/snapshots/snap",
			expectFactory:   true,
			expectedSKU:     "",
			expectErrSubstr: "get error",
			setupMocks: func(f *mock_azclient.MockClientFactory, s *mock_snapshotclient.MockInterface) {
				f.EXPECT().GetSnapshotClientForSub("sub").Return(s, nil)
				s.EXPECT().Get(gomock.Any(), "rg", "snap").Return(nil, fmt.Errorf("get error"))
			},
		},
		{
			name:            "nil snapshot result -> empty string",
			snapshotURI:     "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/snapshots/snap",
			expectFactory:   true,
			expectedSKU:     "",
			expectErrSubstr: "Snapshot is nil",
			setupMocks: func(f *mock_azclient.MockClientFactory, s *mock_snapshotclient.MockInterface) {
				f.EXPECT().GetSnapshotClientForSub("sub").Return(s, nil)
				s.EXPECT().Get(gomock.Any(), "rg", "snap").Return(nil, nil)
			},
		},
		{
			name:            "nil SKU struct -> empty string",
			snapshotURI:     "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/snapshots/snap",
			expectFactory:   true,
			expectedSKU:     "",
			expectErrSubstr: "Snapshot SKU property not found",
			setupMocks: func(f *mock_azclient.MockClientFactory, s *mock_snapshotclient.MockInterface) {
				f.EXPECT().GetSnapshotClientForSub("sub").Return(s, nil)
				s.EXPECT().Get(gomock.Any(), "rg", "snap").Return(&armcompute.Snapshot{}, nil)
			},
		},
		{
			name:            "nil SKU name -> empty string",
			snapshotURI:     "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/snapshots/snap",
			expectFactory:   true,
			expectedSKU:     "",
			expectErrSubstr: "Snapshot SKU property not found",
			setupMocks: func(f *mock_azclient.MockClientFactory, s *mock_snapshotclient.MockInterface) {
				f.EXPECT().GetSnapshotClientForSub("sub").Return(s, nil)
				s.EXPECT().Get(gomock.Any(), "rg", "snap").Return(&armcompute.Snapshot{
					SKU: &armcompute.SnapshotSKU{Name: nil},
				}, nil)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			d, _ := NewFakeDriver(ctrl)

			factory, ok := d.getClientFactory().(*mock_azclient.MockClientFactory)
			if !ok {
				t.Fatalf("clientFactory is not a *mock_azclient.MockClientFactory")
			}
			snapClient := mock_snapshotclient.NewMockInterface(ctrl)

			if tc.setupMocks != nil {
				tc.setupMocks(factory, snapClient)
			}

			invoke := func() (string, error) {
				snapshot, err := d.getSnapshot(context.Background(), tc.snapshotURI)
				if err != nil {
					return "", err
				}
				return getSnapshotSKUFromSnapshot(snapshot)
			}

			sku, err := invoke()

			if tc.expectErrSubstr != "" {
				require.Error(t, err)
				require.Empty(t, sku)
				require.Contains(t, err.Error(), tc.expectErrSubstr)
				if tc.expectGRPCCode != 0 {
					if st, ok := status.FromError(err); ok {
						require.Equal(t, tc.expectGRPCCode, st.Code())
					}
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, sku)
				require.Equal(t, tc.expectedSKU, sku)
			}
		})
	}
}

func TestGetSnapshotSKUFromSnapshot(t *testing.T) {
	type testCase struct {
		name            string
		snapshot        *armcompute.Snapshot
		expectedSKU     string
		expectErrSubstr string
		expectGRPCCode  codes.Code
	}
	tests := []testCase{
		{
			name:        "success - premium LRS",
			expectedSKU: string(armcompute.SnapshotStorageAccountTypesPremiumLRS),
			snapshot: &armcompute.Snapshot{
				SKU: &armcompute.SnapshotSKU{
					Name: to.Ptr(armcompute.SnapshotStorageAccountTypesPremiumLRS),
				},
			},
		},
		{
			name:        "success - standard LRS",
			expectedSKU: string(armcompute.SnapshotStorageAccountTypesStandardLRS),
			snapshot: &armcompute.Snapshot{
				SKU: &armcompute.SnapshotSKU{
					Name: to.Ptr(armcompute.SnapshotStorageAccountTypesStandardLRS),
				},
			},
		},
		{
			name:            "nil snapshot",
			snapshot:        nil,
			expectedSKU:     "",
			expectErrSubstr: "Snapshot is nil",
			expectGRPCCode:  codes.NotFound,
		},
		{
			name:            "nil SKU",
			snapshot:        &armcompute.Snapshot{},
			expectedSKU:     "",
			expectErrSubstr: "Snapshot SKU property not found",
			expectGRPCCode:  codes.NotFound,
		},
		{
			name: "nil SKU Name",
			snapshot: &armcompute.Snapshot{
				SKU: &armcompute.SnapshotSKU{Name: nil},
			},
			expectedSKU:     "",
			expectErrSubstr: "Snapshot SKU property not found",
			expectGRPCCode:  codes.NotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			sku, err := getSnapshotSKUFromSnapshot(tc.snapshot)

			if tc.expectErrSubstr != "" {
				require.Error(t, err)
				require.Empty(t, sku)
				require.Contains(t, err.Error(), tc.expectErrSubstr)
				if tc.expectGRPCCode != 0 {
					if st, ok := status.FromError(err); ok {
						require.Equal(t, tc.expectGRPCCode, st.Code())
					}
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedSKU, sku)
			}
		})
	}
}

func TestGetDiskSizeInBytesFromSnapshot(t *testing.T) {
	type testCase struct {
		name            string
		snapshot        *armcompute.Snapshot
		expectedSize    int64
		expectErrSubstr string
		expectGRPCCode  codes.Code
	}
	tests := []testCase{
		{
			name:         "success - 100GB disk",
			expectedSize: 107374182400, // 100GB in bytes
			snapshot: &armcompute.Snapshot{
				Properties: &armcompute.SnapshotProperties{
					DiskSizeBytes: to.Ptr(int64(107374182400)),
				},
			},
		},
		{
			name:         "success - 1TB disk",
			expectedSize: 1099511627776, // 1TB in bytes
			snapshot: &armcompute.Snapshot{
				Properties: &armcompute.SnapshotProperties{
					DiskSizeBytes: to.Ptr(int64(1099511627776)),
				},
			},
		},
		{
			name:         "success - small disk",
			expectedSize: 4294967296, // 4GB in bytes
			snapshot: &armcompute.Snapshot{
				Properties: &armcompute.SnapshotProperties{
					DiskSizeBytes: to.Ptr(int64(4294967296)),
				},
			},
		},
		{
			name:            "nil snapshot",
			snapshot:        nil,
			expectedSize:    0,
			expectErrSubstr: "Snapshot is nil",
			expectGRPCCode:  codes.NotFound,
		},
		{
			name:            "nil Properties",
			snapshot:        &armcompute.Snapshot{},
			expectedSize:    0,
			expectErrSubstr: "Snapshot size not found",
			expectGRPCCode:  codes.NotFound,
		},
		{
			name: "nil DiskSizeBytes",
			snapshot: &armcompute.Snapshot{
				Properties: &armcompute.SnapshotProperties{
					DiskSizeBytes: nil,
				},
			},
			expectedSize:    0,
			expectErrSubstr: "Snapshot size not found",
			expectGRPCCode:  codes.NotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			size, err := getDiskSizeInBytesFromSnapshot(tc.snapshot)

			if tc.expectErrSubstr != "" {
				require.Error(t, err)
				require.Equal(t, int64(0), size)
				require.Contains(t, err.Error(), tc.expectErrSubstr)
				if tc.expectGRPCCode != 0 {
					if st, ok := status.FromError(err); ok {
						require.Equal(t, tc.expectGRPCCode, st.Code())
					}
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedSize, size)
			}
		})
	}
}

func getFakeDriverWithKubeClient(ctrl *gomock.Controller) FakeDriver {
	d, _ := NewFakeDriver(ctrl)

	corev1 := mockcorev1.NewMockInterface(ctrl)
	persistentvolume := mockpersistentvolume.NewMockInterface(ctrl)
	d.getCloud().KubeClient = mockkubeclient.NewMockInterface(ctrl)
	d.getCloud().KubeClient.(*mockkubeclient.MockInterface).EXPECT().CoreV1().Return(corev1).AnyTimes()
	d.getCloud().KubeClient.CoreV1().(*mockcorev1.MockInterface).EXPECT().PersistentVolumes().Return(persistentvolume).AnyTimes()
	return d
}

func newTestPV(volumeHandle string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pv"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       fakeDriverName,
					VolumeHandle: volumeHandle,
				},
			},
		},
	}
}

func TestHasVolumeAttachmentForDiskOnNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	d, _ := NewFakeDriver(ctrl)
	driver := d.(*fakeDriver)
	client := fake.NewClientset()
	driver.getCloud().KubeClient = client

	nodeName := "node-1"
	otherNode := "node-2"
	diskURI := testVolumeID
	otherDiskURI := fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", "other-disk")
	inlineDiskURI := fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", "inline-disk")
	pvName := "pv-1"

	_, err := client.CoreV1().PersistentVolumes().Create(context.Background(), &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       driver.Name,
					VolumeHandle: diskURI,
				},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	_, err = client.StorageV1().VolumeAttachments().Create(context.Background(), &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-1"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: driver.Name,
			NodeName: nodeName,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: ptr.To(pvName),
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	_, err = client.CoreV1().PersistentVolumes().Create(context.Background(), &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-2"},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       driver.Name,
					VolumeHandle: otherDiskURI,
				},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	_, err = client.StorageV1().VolumeAttachments().Create(context.Background(), &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-2"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: driver.Name,
			NodeName: nodeName,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: ptr.To("pv-2"),
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	_, err = client.StorageV1().VolumeAttachments().Create(context.Background(), &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-inline"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: driver.Name,
			NodeName: otherNode,
			Source: storagev1.VolumeAttachmentSource{
				InlineVolumeSpec: &v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:       driver.Name,
							VolumeHandle: diskURI,
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	_, err = client.StorageV1().VolumeAttachments().Create(context.Background(), &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-line"},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: driver.Name,
			NodeName: nodeName,
			Source: storagev1.VolumeAttachmentSource{
				InlineVolumeSpec: &v1.PersistentVolumeSpec{
					PersistentVolumeSource: v1.PersistentVolumeSource{
						CSI: &v1.CSIPersistentVolumeSource{
							Driver:       driver.Name,
							VolumeHandle: inlineDiskURI,
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	missingDiskURI := fmt.Sprintf(consts.ManagedDiskPath, "subs", "rg", "missing-disk")
	tests := []struct {
		name    string
		node    string
		diskURI string
		want    bool
	}{
		{
			name:    "returns true when VA on node matches disk",
			node:    nodeName,
			diskURI: diskURI,
			want:    true,
		},
		{
			name:    "returns false when no VA on node matches disk",
			node:    nodeName,
			diskURI: missingDiskURI,
			want:    false,
		},
		{
			name:    "ignores VA on other node",
			node:    otherNode,
			diskURI: otherDiskURI,
			want:    false,
		},
		{
			name:    "returns true when inline VA matches node",
			node:    nodeName,
			diskURI: inlineDiskURI,
			want:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hasVA, err := driver.hasVolumeAttachmentForDiskOnNode(context.Background(), tc.node, tc.diskURI)
			require.NoError(t, err)
			require.Equal(t, tc.want, hasVA)
		})
	}
}

func TestTruncateErrMsg(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantLen  int
		wantHead string
		wantTail string
	}{
		{
			name:    "short message not truncated",
			input:   "short error",
			wantLen: len("short error"),
		},
		{
			name:    "exactly at limit not truncated",
			input:   strings.Repeat("a", maxErrMsgLength),
			wantLen: maxErrMsgLength,
		},
		{
			name:     "long message preserves head and tail",
			input:    strings.Repeat("H", 500) + strings.Repeat("T", 600),
			wantLen:  maxErrMsgLength,
			wantHead: "HHHH",
			wantTail: "TTTT",
		},
		{
			name:     "permission error preserves critical tail",
			input:    "Attach volume pvc-xxx to instance aks-node failed with PUT https://long-url/..." + strings.Repeat("x", 900) + "does not have permission to perform action 'Microsoft.Compute/diskEncryptionSets/read' on the linked scope",
			wantLen:  maxErrMsgLength,
			wantTail: "diskEncryptionSets/read' on the linked scope",
		},
		{
			// Regression: verbose DES permission errors end with the
			// ClientId / ObjectId of the identity that lacks the role
			// assignment. Those IDs are the actionable part for the
			// customer (they tell them which managed identity needs the
			// role) and must survive truncation.
			name:     "DES permission error preserves ClientId and ObjectId at tail",
			input:    "Attach volume pvc-abcd-1234 to instance aks-nodepool1-12345678-vmss000000 failed with rpc error: code = Internal desc = " + strings.Repeat("filler junk from long Azure error body ", 30) + "does not have authorization to perform action 'Microsoft.Compute/diskEncryptionSets/read' over scope '/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-des/providers/Microsoft.Compute/diskEncryptionSets/my-des' or the scope is invalid. ClientId=11111111-2222-3333-4444-555555555555 ObjectId=66666666-7777-8888-9999-aaaaaaaaaaaa",
			wantLen:  maxErrMsgLength,
			wantTail: "ClientId=11111111-2222-3333-4444-555555555555 ObjectId=66666666-7777-8888-9999-aaaaaaaaaaaa",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := truncateErrMsg(tc.input)
			if len(result) > maxErrMsgLength {
				t.Errorf("truncateErrMsg() length = %d, want <= %d", len(result), maxErrMsgLength)
			}
			if tc.wantLen > 0 && len(result) != tc.wantLen {
				t.Errorf("truncateErrMsg() length = %d, want %d", len(result), tc.wantLen)
			}
			if tc.wantHead != "" && !strings.HasPrefix(result, tc.wantHead) {
				t.Errorf("truncateErrMsg() should start with %q, got prefix %q", tc.wantHead, result[:20])
			}
			if tc.wantTail != "" && !strings.HasSuffix(result, tc.wantTail) {
				t.Errorf("truncateErrMsg() should end with %q, got suffix %q", tc.wantTail, result[len(result)-50:])
			}
			if len(tc.input) > maxErrMsgLength && !strings.Contains(result, " ... ") {
				t.Error("truncateErrMsg() should contain ellipsis for truncated messages")
			}
		})
	}
}
