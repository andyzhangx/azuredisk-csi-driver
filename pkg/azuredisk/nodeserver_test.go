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
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	directvolume "github.com/kata-containers/kata-containers/src/runtime/pkg/direct-volume"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/mount-utils"
	testingexec "k8s.io/utils/exec/testing"
	"k8s.io/utils/ptr"
	consts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/mounter"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/optimization/mockoptimization"
	volumehelper "sigs.k8s.io/azuredisk-csi-driver/pkg/util"
	"sigs.k8s.io/azuredisk-csi-driver/test/utils/testutil"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	mockvmclient "sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachineclient/mock_virtualmachineclient"
	azureconsts "sigs.k8s.io/cloud-provider-azure/pkg/consts"
	azure "sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

const (
	virtualMachineURIFormat = "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/virtualMachines/%s"
)

type testRoundTripper func(*http.Request) (*http.Response, error)

func (f testRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

var (
	sourceTest string
	targetTest string

	testSubscription  = "01234567-89ab-cdef-0123-456789abcdef"
	testResourceGroup = "rg"

	provisioningStateSucceeded = "Succeeded"

	testVMName     = fakeNodeID
	testVMURI      = fmt.Sprintf(virtualMachineURIFormat, testSubscription, testResourceGroup, testVMName)
	testVMSize     = armcompute.VirtualMachineSizeTypesStandardD3V2
	testVMLocation = "westus"
	testVMZones    = []*string{ptr.To("1")}
	testVM         = armcompute.VirtualMachine{
		Name:     &testVMName,
		ID:       &testVMURI,
		Location: &testVMLocation,
		Zones:    testVMZones,
		Properties: &armcompute.VirtualMachineProperties{
			ProvisioningState: &provisioningStateSucceeded,
			HardwareProfile: &armcompute.HardwareProfile{
				VMSize: &testVMSize,
			},
			StorageProfile: &armcompute.StorageProfile{
				DataDisks: []*armcompute.DataDisk{},
			},
		},
	}
)

type kataStubDirectVolume struct {
	addMountInfo    func(string, directvolume.MountInfo) error
	remove          func(string) error
	isVolumeMounted func(string) (bool, error)
	findMountInfo   func(string) (string, error)
}

func (f *kataStubDirectVolume) AddMountInfo(volumePath string, mountInfo directvolume.MountInfo) error {
	return f.addMountInfo(volumePath, mountInfo)
}

func (f *kataStubDirectVolume) Remove(volumePath string) error {
	return f.remove(volumePath)
}

func (f *kataStubDirectVolume) IsVolumeMounted(volumePath string) (bool, error) {
	return f.isVolumeMounted(volumePath)
}

// FindMountInfo delegates assignment lookup or reports an empty store.
func (f *kataStubDirectVolume) FindMountInfo(volumeID string) (string, error) {
	if f.findMountInfo != nil {
		return f.findMountInfo(volumeID)
	}
	return "", nil
}

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

func TestIncrementAttachSequenceAnnotationRetriesConflict(t *testing.T) {
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-pv",
			ResourceVersion: "1",
			Annotations: map[string]string{
				consts.AttachSequenceAnnotation: "4",
			},
		},
	}
	kubeClient := fake.NewClientset(pv.DeepCopy())
	updateAttempts := 0
	kubeClient.PrependReactor("update", "persistentvolumes", func(clienttesting.Action) (bool, k8sruntime.Object, error) {
		updateAttempts++
		if updateAttempts > 1 {
			return false, nil, nil
		}

		latestPV := pv.DeepCopy()
		latestPV.ResourceVersion = "2"
		latestPV.Annotations[consts.AttachSequenceAnnotation] = "8"
		resource := schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}
		if err := kubeClient.Tracker().Update(resource, latestPV, ""); err != nil {
			return true, nil, err
		}
		return true, nil, apierrors.NewConflict(
			schema.GroupResource{Resource: "persistentvolumes"}, pv.Name, errors.New("concurrent update"))
	})

	updatedSequence, err := incrementAttachSequenceAnnotation(context.Background(), kubeClient, pv)
	require.NoError(t, err)
	assert.Equal(t, 9, updatedSequence)
	assert.Equal(t, 2, updateAttempts)
	assert.Equal(t, "4", pv.Annotations[consts.AttachSequenceAnnotation])

	updatedPV, err := kubeClient.CoreV1().PersistentVolumes().Get(context.Background(), pv.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "9", updatedPV.Annotations[consts.AttachSequenceAnnotation])
}

func TestIncrementAttachSequenceAnnotationRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "malformed", value: "invalid"},
		{name: "negative", value: "-1"},
		{name: "parse overflow", value: "999999999999999999999999999999"},
		{name: "increment overflow", value: strconv.Itoa(int(^uint(0) >> 1))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pv := &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pv",
					Annotations: map[string]string{
						consts.AttachSequenceAnnotation: test.value,
					},
				},
			}
			kubeClient := fake.NewClientset(pv.DeepCopy())
			updateAttempts := 0
			kubeClient.PrependReactor("update", "persistentvolumes", func(clienttesting.Action) (bool, k8sruntime.Object, error) {
				updateAttempts++
				return false, nil, nil
			})

			updatedSequence, err := incrementAttachSequenceAnnotation(context.Background(), kubeClient, pv)
			require.Error(t, err)
			assert.Zero(t, updatedSequence)
			assert.Zero(t, updateAttempts)

			updatedPV, getErr := kubeClient.CoreV1().PersistentVolumes().Get(context.Background(), pv.Name, metav1.GetOptions{})
			require.NoError(t, getErr)
			assert.Equal(t, test.value, updatedPV.Annotations[consts.AttachSequenceAnnotation])
		})
	}
}

func TestNodeGetCapabilities(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
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

func TestGetMaxDataDiskCount(t *testing.T) {
	tests := []struct {
		instanceType string
		expectResult int64
		expectExists bool
	}{
		{
			instanceType: "standard_d2_v2",
			expectResult: 8,
			expectExists: true,
		},
		{
			instanceType: "Standard_DS14_V2",
			expectResult: 64,
			expectExists: true,
		},
		{
			instanceType: "NOT_EXISTING",
			expectResult: defaultAzureVolumeLimit,
			expectExists: false,
		},
		{
			instanceType: "",
			expectResult: defaultAzureVolumeLimit,
			expectExists: false,
		},
	}

	for _, test := range tests {
		result, exists := GetMaxDataDiskCount(test.instanceType)
		assert.Equal(t, test.expectResult, result)
		assert.Equal(t, test.expectExists, exists)
	}
}

func TestEnsureMountPoint(t *testing.T) {
	errorTarget, err := testutil.GetWorkDirPath("error_is_likely_target")
	assert.NoError(t, err)
	alreadyExistTarget, err := testutil.GetWorkDirPath("false_is_likely_exist_target")
	assert.NoError(t, err)
	azuredisk, err := testutil.GetWorkDirPath("azuredisk.go")
	assert.NoError(t, err)

	tests := []struct {
		desc          string
		target        string
		skipOnWindows bool
		skipOnDarwin  bool
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
			skipOnDarwin:  true,
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

	for _, test := range tests {
		// Setup
		cntl := gomock.NewController(t)
		_ = makeDir(alreadyExistTarget)
		d, _ := NewFakeDriver(cntl)
		fakeMounter, err := mounter.NewFakeSafeMounter()
		assert.NoError(t, err)
		d.setMounter(fakeMounter)
		if !(runtime.GOOS == "windows" && test.skipOnWindows) && !(runtime.GOOS == "darwin" && test.skipOnDarwin) {
			mnt, err := d.ensureMountPoint(test.target)
			if !testutil.AssertError(&test.expectedErr, err) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
			}
			if err == nil {
				assert.Equal(t, test.expectedMnt, mnt)
			}
		}
		// Clean up
		err = os.RemoveAll(alreadyExistTarget)
		assert.NoError(t, err)
		err = os.RemoveAll(targetTest)
		assert.NoError(t, err)
		cntl.Finish()
	}

}

func TestNodeGetInfo(t *testing.T) {
	notFoundErr := errors.New("not found")

	tests := []struct {
		desc         string
		expectedErr  error
		skipOnDarwin bool
		setupFunc    func(_ *testing.T, _ FakeDriver)
		validateFunc func(_ *testing.T, _ *csi.NodeGetInfoResponse)
	}{
		{
			desc:         "[Success] Get node information for existing VM",
			expectedErr:  nil,
			skipOnDarwin: true,
			setupFunc: func(t *testing.T, d FakeDriver) {
				mockVMClient := d.getCloud().ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
				mockVMClient.EXPECT().
					Get(gomock.Any(), testResourceGroup, testVMName, gomock.Any()).
					Return(&testVM, nil).
					AnyTimes()

				// cloud-provider-azure's GetZone function assumes the host is a VM and returns it zones.
				// We therefore mock a return of the testVM if the hostname is used.
				hostname, err := os.Hostname()
				require.NoError(t, err)

				mockVMClient.EXPECT().
					Get(gomock.Any(), testResourceGroup, hostname, gomock.Any()).
					Return(&testVM, nil).
					AnyTimes()
				mockVMClient.EXPECT().
					Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(&armcompute.VirtualMachine{}, notFoundErr).
					AnyTimes()
			},
			validateFunc: func(t *testing.T, resp *csi.NodeGetInfoResponse) {
				assert.Equal(t, testVMName, resp.NodeId)
				maxDiskdataCount, _ := GetMaxDataDiskCount(string(testVMSize))
				assert.Equal(t, maxDiskdataCount, resp.MaxVolumesPerNode)
				assert.Len(t, resp.AccessibleTopology.Segments, 2)
			},
		},
		{
			desc:        "[Failure] Get node information for non-existing VM",
			expectedErr: status.Error(codes.Unavailable, "topology is not initialized for node(fakeNodeID): cloud lookup: not found, label lookup: kubeClient is nil"),
			setupFunc: func(_ *testing.T, d FakeDriver) {
				mockVMClient := d.getCloud().ComputeClientFactory.GetVirtualMachineClient().(*mockvmclient.MockInterface)
				mockVMClient.EXPECT().
					Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(&armcompute.VirtualMachine{}, notFoundErr).
					AnyTimes()
			},
			validateFunc: func(t *testing.T, resp *csi.NodeGetInfoResponse) {
				assert.Equal(t, testVMName, resp.NodeId)
				assert.Equal(t, int64(defaultAzureVolumeLimit), resp.MaxVolumesPerNode)
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			cntl := gomock.NewController(t)
			defer cntl.Finish()
			if test.skipOnDarwin && runtime.GOOS == "darwin" {
				t.Skip("Skip test case on Darwin")
			}
			d, err := NewFakeDriver(cntl)
			require.NoError(t, err)

			test.setupFunc(t, d)

			resp, err := d.NodeGetInfo(context.TODO(), &csi.NodeGetInfoRequest{})
			require.Equal(t, test.expectedErr, err)
			if err == nil {
				test.validateFunc(t, resp)
			}
		})
	}
}

// Cloud-backed registration must not confuse unknown topology with a non-zonal
// VM. Pure label-only mode retains compatibility when no cloud is configured.
func TestNodeGetInfoTopologyReadiness(t *testing.T) {
	for _, fromLabels := range []bool{true, false} {
		t.Run(fmt.Sprintf("fromLabels=%t", fromLabels), func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				labels map[string]string
				zone   string
				code   codes.Code
			}{
				{name: "no labels", code: codes.Internal},
				{name: "hostname only", labels: map[string]string{corev1.LabelHostname: "node1"}},
				{name: "instance type only", labels: map[string]string{consts.InstanceTypeKey: "Standard_E8s_v3"}},
				{name: "empty zone", labels: map[string]string{consts.WellKnownTopologyKey: ""}},
				{name: "zonal", labels: map[string]string{consts.WellKnownTopologyKey: "centralus-1"}, zone: "centralus-1"},
				// Azure's GetZoneByNodeName reports numeric fault domains for non-zonal VMs.
				{name: "non-zonal fault domain zero", labels: map[string]string{consts.WellKnownTopologyKey: "0"}},
				{name: "non-zonal fault domain one", labels: map[string]string{consts.WellKnownTopologyKey: "1"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					d := &Driver{supportZone: true, getNodeInfoFromLabels: fromLabels, cloud: &azure.Cloud{}}
					d.NodeID = "node1"
					if !fromLabels {
						mockVMSet := azure.NewMockVMSet(gomock.NewController(t))
						d.cloud.VMSet = mockVMSet
						d.cloud.ComputeClientFactory = mock_azclient.NewMockClientFactory(gomock.NewController(t))
						// Windows queries the node ID directly; GetZone uses the local hostname elsewhere.
						nodeName := d.NodeID
						if runtime.GOOS != "windows" {
							hostname, err := os.Hostname()
							require.NoError(t, err)
							nodeName = strings.ToLower(hostname)
						}
						// Explicitly fail the cloud lookup to exercise the label fallback on every OS.
						mockVMSet.EXPECT().GetZoneByNodeName(gomock.Any(), nodeName).
							Return(cloudprovider.Zone{}, errors.New("zone lookup failed"))
					}
					d.cloud.KubeClient = fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: d.NodeID, Labels: tc.labels}})
					resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
					wantCode := tc.code
					if !fromLabels && tc.labels[consts.WellKnownTopologyKey] == "" {
						wantCode = codes.Unavailable
					}
					require.Equal(t, wantCode, status.Code(err), "error: %v", err)
					if wantCode != codes.OK {
						require.Nil(t, resp, "unknown topology must not be published")
						return
					}
					want := map[string]string{topologyKey: tc.zone}
					if tc.zone != "" {
						want[consts.WellKnownTopologyKey] = tc.zone
					}
					require.Equal(t, want, resp.AccessibleTopology.Segments)
				})
			}
		})
	}
}

func TestNodeGetInfoTopologyFromMetadata(t *testing.T) {
	for _, fromLabels := range []bool{false, true} {
		t.Run(fmt.Sprintf("fromLabels=%t", fromLabels), func(t *testing.T) {
			for _, tc := range []struct {
				name, metadata, zone, label string
				code                        codes.Code
				httpStatus                  int
			}{
				{name: "zonal", metadata: `{"compute":{"location":"centralus","zone":"2"}}`, zone: "centralus-2"},
				{name: "non-zonal", metadata: `{"compute":{"location":"centralus","zone":"","platformFaultDomain":"0"}}`},
				{name: "non-zonal fault domain one", metadata: `{"compute":{"location":"centralus","zone":"","platformFaultDomain":"1"}}`},
				{name: "incomplete metadata", metadata: `{"compute":{"location":"centralus"}}`, code: codes.Unavailable},
				{name: "empty metadata fields", metadata: `{"compute":{"location":"centralus","zone":"","platformFaultDomain":""}}`, code: codes.Unavailable},
				{name: "invalid zone", metadata: `{"compute":{"location":"centralus","zone":"invalid"}}`, code: codes.Unavailable},
				{name: "missing compute", metadata: `{}`, code: codes.Unavailable},
				{name: "missing location", metadata: `{"compute":{"zone":"2"}}`, code: codes.Unavailable},
				{name: "missing location with label", metadata: `{"compute":{"zone":"2"}}`, label: "centralus-3", zone: "centralus-3"},
				{name: "negative fault domain", metadata: `{"compute":{"location":"centralus","platformFaultDomain":"-1"}}`, code: codes.Unavailable},
				{name: "invalid fault domain", metadata: `{"compute":{"location":"centralus","platformFaultDomain":"unknown"}}`, code: codes.Unavailable},
				{name: "metadata unavailable", httpStatus: http.StatusServiceUnavailable, code: codes.Unavailable},
				{name: "metadata unauthorized", httpStatus: http.StatusUnauthorized, code: codes.Unavailable},
				{name: "incomplete metadata with label", metadata: `{"compute":{"location":"centralus"}}`, label: "centralus-3", zone: "centralus-3"},
				{name: "unavailable metadata with label", httpStatus: http.StatusServiceUnavailable, label: "centralus-3", zone: "centralus-3"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						if tc.httpStatus != 0 {
							w.WriteHeader(tc.httpStatus)
							return
						}
						_, _ = io.WriteString(w, tc.metadata)
					}))
					defer server.Close()
					metadata, err := azure.NewInstanceMetadataService(server.URL)
					require.NoError(t, err)
					d := &Driver{supportZone: true, getNodeInfoFromLabels: fromLabels, cloud: &azure.Cloud{}}
					d.NodeID = "node1"
					d.cloud.UseInstanceMetadata = true
					d.cloud.Metadata = metadata
					// Cloud-first mode also works without a Kubernetes client.
					if fromLabels || tc.label != "" {
						d.cloud.KubeClient = fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
							Name: d.NodeID, Labels: map[string]string{consts.InstanceTypeKey: "Standard_E8s_v3", consts.WellKnownTopologyKey: tc.label},
						}})
					}
					resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
					require.Equal(t, tc.code, status.Code(err), "error: %v", err)
					if tc.code != codes.OK {
						require.Nil(t, resp)
						return
					}
					want := map[string]string{topologyKey: tc.zone}
					if tc.zone != "" {
						want[consts.WellKnownTopologyKey] = tc.zone
					}
					require.Equal(t, want, resp.AccessibleTopology.Segments)
				})
			}
		})
	}
}

// Advance the real provider cache's TTL in virtual time, without replacing the
// metadata client or requiring a minute-long sleep or a live IMDS endpoint.
func TestNodeGetInfoTopologyMetadataCacheRecovery(t *testing.T) {
	for _, fromLabels := range []bool{false, true} {
		for _, recoveredMetadata := range []string{
			`{"compute":{"location":"centralus","zone":"2"}}`,
			`{"compute":{"location":"centralus","platformFaultDomain":"0"}}`,
		} {
			t.Run(fmt.Sprintf("fromLabels=%t/%s", fromLabels, recoveredMetadata), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					calls := 0
					body := `{"compute":{"location":"centralus"}}`
					originalTransport := http.DefaultTransport
					http.DefaultTransport = testRoundTripper(func(req *http.Request) (*http.Response, error) {
						calls++
						return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: req}, nil
					})
					defer func() { http.DefaultTransport = originalTransport }()
					metadata, err := azure.NewInstanceMetadataService("http://imds.test")
					require.NoError(t, err)
					d := &Driver{supportZone: true, getNodeInfoFromLabels: fromLabels, cloud: &azure.Cloud{}}
					d.NodeID = "node1"
					d.cloud.UseInstanceMetadata = true
					d.cloud.Metadata = metadata
					d.cloud.KubeClient = fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
						Name: d.NodeID, Labels: map[string]string{consts.InstanceTypeKey: "Standard_E8s_v3"},
					}})
					ctx := context.Background()
					resp, err := d.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.Nil(t, resp)
					require.Equal(t, 1, calls)

					body = recoveredMetadata
					// Retries before expiry still use the incomplete cached response.
					resp, err = d.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.Nil(t, resp)
					require.Equal(t, 1, calls)

					time.Sleep(azureconsts.MetadataCacheTTL + time.Second)
					resp, err = d.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
					require.NoError(t, err)
					require.Equal(t, 2, calls, "expired metadata must be refreshed by topology lookup")
					want := map[string]string{topologyKey: ""}
					if strings.Contains(recoveredMetadata, `"zone":"2"`) {
						want = map[string]string{topologyKey: "centralus-2", consts.WellKnownTopologyKey: "centralus-2"}
					}
					require.Equal(t, want, resp.AccessibleTopology.Segments)
				})
			})
		}
	}
}

func TestNodeGetInfoTopologyFromARM(t *testing.T) {
	for _, fromLabels := range []bool{false, true} {
		for _, tc := range []struct {
			name, failureDomain string
			code                codes.Code
		}{
			{name: "zonal", failureDomain: "centralus-2"},
			{name: "non-zonal zero", failureDomain: "0"},
			{name: "non-zonal one", failureDomain: "1"},
			{name: "incomplete", code: codes.Unavailable},
			{name: "missing location", failureDomain: "-2", code: codes.Unavailable},
			{name: "negative fault domain", failureDomain: "-1", code: codes.Unavailable},
			{name: "invalid fault domain", failureDomain: "unknown", code: codes.Unavailable},
		} {
			t.Run(fmt.Sprintf("fromLabels=%t/%s", fromLabels, tc.name), func(t *testing.T) {
				ctrl := gomock.NewController(t)
				d := &Driver{supportZone: true, getNodeInfoFromLabels: fromLabels, cloud: &azure.Cloud{}}
				d.NodeID = "node1"
				vmSet := azure.NewMockVMSet(ctrl)
				d.cloud.VMSet = vmSet
				d.cloud.ComputeClientFactory = mock_azclient.NewMockClientFactory(ctrl)
				nodeName := d.NodeID
				if runtime.GOOS != "windows" {
					hostname, err := os.Hostname()
					require.NoError(t, err)
					nodeName = strings.ToLower(hostname)
				}
				vmSet.EXPECT().GetZoneByNodeName(gomock.Any(), nodeName).
					Return(cloudprovider.Zone{FailureDomain: tc.failureDomain, Region: "centralus"}, nil)
				d.cloud.KubeClient = fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
					Name: d.NodeID, Labels: map[string]string{consts.InstanceTypeKey: "Standard_E8s_v3"},
				}})
				resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
				require.Equal(t, tc.code, status.Code(err), "error: %v", err)
				if tc.code != codes.OK {
					require.Nil(t, resp)
					return
				}
				want := map[string]string{topologyKey: ""}
				if tc.failureDomain == "centralus-2" {
					want = map[string]string{topologyKey: tc.failureDomain, consts.WellKnownTopologyKey: tc.failureDomain}
				}
				require.Equal(t, want, resp.AccessibleTopology.Segments)
			})
		}
	}
}

func TestNodeGetInfoLabelPrecedence(t *testing.T) {
	for _, zone := range []string{"centralus-1", "0", "1"} {
		t.Run(zone, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()
			metadata, err := azure.NewInstanceMetadataService(server.URL)
			require.NoError(t, err)
			d := &Driver{supportZone: true, getNodeInfoFromLabels: true, cloud: &azure.Cloud{}}
			d.NodeID = "node1"
			d.VolumeAttachLimit = -1
			d.cloud.UseInstanceMetadata = true
			d.cloud.Metadata = metadata
			d.cloud.KubeClient = fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: d.NodeID, Labels: map[string]string{consts.InstanceTypeKey: "Standard_E8s_v3", consts.WellKnownTopologyKey: zone},
			}})
			resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
			require.NoError(t, err)
			require.Zero(t, calls.Load(), "nonempty labels must not depend on cloud access")
			want := map[string]string{topologyKey: ""}
			if zone == "centralus-1" {
				want = map[string]string{topologyKey: zone, consts.WellKnownTopologyKey: zone}
			}
			require.Equal(t, want, resp.AccessibleTopology.Segments)
			limit, _ := GetMaxDataDiskCount("Standard_E8s_v3")
			require.Equal(t, limit, resp.MaxVolumesPerNode)
		})
	}
}

func TestNodeGetInfoTopologyUninitializedClients(t *testing.T) {
	for _, fromLabels := range []bool{false, true} {
		for _, capability := range []string{"none", "imds", "arm", "vmset"} {
			t.Run(fmt.Sprintf("fromLabels=%t/%s", fromLabels, capability), func(t *testing.T) {
				d := &Driver{supportZone: true, getNodeInfoFromLabels: fromLabels, cloud: &azure.Cloud{}}
				d.NodeID = "node1"
				switch capability {
				case "imds":
					d.cloud.UseInstanceMetadata = true // nil metadata client must not panic
				case "arm":
					d.cloud.SubscriptionID = "configured" // nil VMSet must not panic
				case "vmset":
					// A partially initialized ARM VMSet must not be called without its factory.
					d.cloud.VMSet = azure.NewMockVMSet(gomock.NewController(t))
				}
				d.cloud.KubeClient = fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
					Name: d.NodeID, Labels: map[string]string{consts.InstanceTypeKey: "Standard_E8s_v3"},
				}})
				resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
				if fromLabels && capability == "none" {
					require.NoError(t, err)
					require.Equal(t, map[string]string{topologyKey: ""}, resp.AccessibleTopology.Segments)
				} else {
					require.Equal(t, codes.Unavailable, status.Code(err))
					require.Nil(t, resp)
				}
				// Nonempty labels remain usable even with uninitialized cloud clients.
				node, err := d.cloud.KubeClient.CoreV1().Nodes().Get(context.Background(), d.NodeID, metav1.GetOptions{})
				require.NoError(t, err)
				node.Labels[consts.WellKnownTopologyKey] = "centralus-1"
				_, err = d.cloud.KubeClient.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
				require.NoError(t, err)
				resp, err = d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
				require.NoError(t, err)
				require.Equal(t, "centralus-1", resp.AccessibleTopology.Segments[topologyKey])
			})
		}
	}
}

// A deployment with no cloud config may permanently lack zone labels. Do not
// turn that supported configuration into an endless registration retry loop.
func TestNodeGetInfoLabelOnlyCompatibility(t *testing.T) {
	for _, emptyLabel := range []bool{false, true} {
		t.Run(fmt.Sprintf("emptyLabel=%t", emptyLabel), func(t *testing.T) {
			labels := map[string]string{consts.InstanceTypeKey: "Standard_E8s_v3"}
			if emptyLabel {
				labels[consts.WellKnownTopologyKey] = ""
			}
			d := &Driver{supportZone: true, getNodeInfoFromLabels: true, cloud: &azure.Cloud{}}
			d.NodeID = "node1"
			d.VolumeAttachLimit = -1
			d.ReservedDataDiskSlotNum = 1
			d.cloud.KubeClient = fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: d.NodeID, Labels: labels}})
			limit, _ := GetMaxDataDiskCount("Standard_E8s_v3")
			for i := 0; i < 3; i++ {
				resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
				require.NoError(t, err)
				require.Equal(t, map[string]string{topologyKey: ""}, resp.AccessibleTopology.Segments)
				require.Equal(t, limit-1, resp.MaxVolumesPerNode)
			}
		})
	}
}

func TestNodeGetInfoTopologyRetry(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node1", Labels: map[string]string{consts.InstanceTypeKey: "Standard_E8s_v3"},
	}})
	d := &Driver{supportZone: true, getNodeInfoFromLabels: true, cloud: &azure.Cloud{}}
	d.NodeID = "node1"
	d.VolumeAttachLimit = -1
	d.cloud.KubeClient = client
	d.cloud.VMSet = azure.NewMockVMSet(gomock.NewController(t))
	d.cloud.ComputeClientFactory = mock_azclient.NewMockClientFactory(gomock.NewController(t))
	gomock.InOrder(
		d.cloud.VMSet.(*azure.MockVMSet).EXPECT().GetZoneByNodeName(gomock.Any(), gomock.Any()).Return(cloudprovider.Zone{}, errors.New("metadata pending")),
		d.cloud.VMSet.(*azure.MockVMSet).EXPECT().GetZoneByNodeName(gomock.Any(), gomock.Any()).Return(cloudprovider.Zone{FailureDomain: "centralus-1"}, nil),
	)
	resp, err := d.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Nil(t, resp)
	require.Equal(t, int64(-1), d.VolumeAttachLimit)

	// Cloud metadata becomes ready; labels remain permanently zone-less.
	resp, err = d.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{topologyKey: "centralus-1", consts.WellKnownTopologyKey: "centralus-1"}, resp.AccessibleTopology.Segments)
	limit, _ := GetMaxDataDiskCount("Standard_E8s_v3")
	require.Equal(t, limit, resp.MaxVolumesPerNode)
}

func TestNodeGetInfoWithoutZoneSupport(t *testing.T) {
	d := &Driver{getNodeInfoFromLabels: true, cloud: &azure.Cloud{}}
	d.cloud.UseInstanceMetadata = true // no metadata client: topology lookup must be bypassed
	d.NodeID = "node1"
	resp, err := d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{topologyKey: ""}, resp.AccessibleTopology.Segments)

	// Instance-type lookup is independent of topology when zone support is off.
	d.VolumeAttachLimit = -1
	d.cloud.KubeClient = fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: d.NodeID, Labels: map[string]string{consts.InstanceTypeKey: "Standard_E8s_v3"},
	}})
	resp, err = d.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	require.NoError(t, err)
	limit, _ := GetMaxDataDiskCount("Standard_E8s_v3")
	require.Equal(t, limit, resp.MaxVolumesPerNode)
	require.Equal(t, map[string]string{topologyKey: ""}, resp.AccessibleTopology.Segments)
}

func TestNodeGetVolumeStats(t *testing.T) {
	nonexistedPath := "/not/a/real/directory"
	fakePath := "/tmp/fake-volume-path"
	blockVolumePath := "/tmp/block-volume-path"
	blockdevAction := func() ([]byte, []byte, error) {
		return []byte(fmt.Sprintf("%d", stdCapacityRange.RequiredBytes)), []byte{}, nil
	}
	tests := []struct {
		desc          string
		setupFunc     func(*testing.T, FakeDriver)
		req           *csi.NodeGetVolumeStatsRequest
		expectedErr   error
		skipOnDarwin  bool
		skipOnWindows bool
	}{
		{
			desc:        "Volume ID missing",
			req:         &csi.NodeGetVolumeStatsRequest{VolumePath: targetTest},
			expectedErr: status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume ID was empty"),
		},
		{
			desc:        "VolumePath missing",
			req:         &csi.NodeGetVolumeStatsRequest{VolumeId: "vol_1"},
			expectedErr: status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume path was empty"),
		},
		{
			desc:          "Not existed volume path",
			req:           &csi.NodeGetVolumeStatsRequest{VolumePath: nonexistedPath, VolumeId: "vol_1"},
			expectedErr:   status.Errorf(codes.NotFound, "path /not/a/real/directory does not exist"),
			skipOnWindows: true,
			skipOnDarwin:  true,
		},
		{
			desc: "Block volume path success",
			setupFunc: func(_ *testing.T, d FakeDriver) {
				d.getHostUtil().(*azureutils.FakeHostUtil).SetPathIsDeviceResult(blockVolumePath, true, nil)
				d.setNextCommandOutputScripts(blockdevAction)
			},
			req:           &csi.NodeGetVolumeStatsRequest{VolumePath: blockVolumePath, VolumeId: "vol_1"},
			skipOnDarwin:  true,
			skipOnWindows: true,
			expectedErr:   nil,
		},
		{
			desc:          "standard success",
			req:           &csi.NodeGetVolumeStatsRequest{VolumePath: fakePath, VolumeId: "vol_1"},
			skipOnDarwin:  true,
			skipOnWindows: true,
			expectedErr:   nil,
		},
		{
			desc: "failed to determine block device",
			setupFunc: func(_ *testing.T, d FakeDriver) {
				d.getHostUtil().(*azureutils.FakeHostUtil).SetPathIsDeviceResult(fakePath, true, fmt.Errorf("host util is not device path"))
			},
			req:           &csi.NodeGetVolumeStatsRequest{VolumePath: fakePath, VolumeId: "vol_1"},
			skipOnDarwin:  true,
			skipOnWindows: true,
			expectedErr:   status.Errorf(codes.NotFound, "failed to determine whether %s is block device: %v", fakePath, fmt.Errorf("host util is not device path")),
		},
	}

	for _, test := range tests {
		cntl := gomock.NewController(t)
		// Setup
		_ = makeDir(fakePath)
		_ = makeDir(blockVolumePath)
		d, _ := NewFakeDriver(cntl)
		mounter, err := mounter.NewFakeSafeMounter()
		assert.NoError(t, err)
		d.setMounter(mounter)
		if !(test.skipOnDarwin && runtime.GOOS == "darwin") && !(test.skipOnWindows && runtime.GOOS == "windows") {
			if test.setupFunc != nil {
				test.setupFunc(t, d)
			}
			_, err := d.NodeGetVolumeStats(context.Background(), test.req)
			if !reflect.DeepEqual(err, test.expectedErr) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
			}
		}
		// Clean up
		err = os.RemoveAll(fakePath)
		assert.NoError(t, err)
		err = os.RemoveAll(blockVolumePath)
		assert.NoError(t, err)
		cntl.Finish()
	}
}

func TestKataVolumeStatsAndResizeUnimplemented(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Kata filesystem mounts are only supported on Linux")
	}

	cntl := gomock.NewController(t)
	d, _ := NewFakeDriver(cntl)
	d.(*fakeDriver).enableKataMount = true
	d.(*fakeDriver).isKataNode = true
	target, err := testutil.GetWorkDirPath("direct_volume_operation_target")
	require.NoError(t, err)
	fallbackStatsTarget, err := testutil.GetWorkDirPath("direct_volume_stats_probe_error_target")
	require.NoError(t, err)
	require.NoError(t, makeDir(fallbackStatsTarget))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(fallbackStatsTarget)) })
	fallbackResizeTarget := "/tmp/direct-volume-resize-probe-error-target"
	d.getHostUtil().(*azureutils.FakeHostUtil).SetPathIsDeviceResult(fallbackResizeTarget, true, nil)
	fakeMounter, err := mounter.NewFakeSafeMounter()
	require.NoError(t, err)
	d.setMounter(fakeMounter)

	d.(*fakeDriver).kataDirectVolume = &kataStubDirectVolume{
		findMountInfo: func(volumeID string) (string, error) {
			switch volumeID {
			case "vol_1":
				return target, nil
			case "lookup_error":
				return "", errors.New("fake direct volume probe error")
			default:
				return "", nil
			}
		},
		isVolumeMounted: func(string) (bool, error) {
			t.Fatal("stats and resize must query assignments by volume ID")
			return false, nil
		},
	}

	stagingTarget := filepath.Join(t.TempDir(), "removed-staging")
	for _, volumePath := range []string{target, stagingTarget} {
		_, err = d.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId:   "vol_1",
			VolumePath: volumePath,
		})
		assert.Equal(t, codes.Unimplemented, status.Code(err))

		_, err = d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
			VolumeId:   "vol_1",
			VolumePath: volumePath,
		})
		assert.Equal(t, codes.Unimplemented, status.Code(err))
	}

	for _, volumeID := range []string{"unassigned", "lookup_error"} {
		_, err = d.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{
			VolumeId:   volumeID,
			VolumePath: fallbackStatsTarget,
		})
		assert.NoError(t, err)
	}

	_, err = d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:   "unassigned",
		VolumePath: fallbackResizeTarget,
	})
	assert.NoError(t, err)

	_, err = d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:   "lookup_error",
		VolumePath: fallbackResizeTarget,
	})
	assert.Equal(t, codes.Internal, status.Code(err))

	for _, test := range []struct {
		volumeID string
		wantCode codes.Code
	}{
		{volumeID: "vol_1", wantCode: codes.Unimplemented},
		{volumeID: "unassigned", wantCode: codes.OK},
		{volumeID: "lookup_error", wantCode: codes.Internal},
	} {
		_, err = d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
			VolumeId:   test.volumeID,
			VolumePath: fallbackResizeTarget,
			VolumeCapability: &csi.VolumeCapability{
				AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			},
		})
		assert.Equal(t, test.wantCode, status.Code(err), test.volumeID)
	}
}

// TestResolveFSType runs on every platform, including the Windows CI runner, because it
// needs no mounter: NewFakeSafeMounter returns a real csi-proxy mounter on Windows, which
// is why the NodeStageVolume cases below are all skipped there.
func TestResolveFSType(t *testing.T) {
	mountCap := func(fsType string, flags ...string) *csi.VolumeCapability {
		return &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: fsType, MountFlags: flags},
			},
		}
	}
	blockCap := &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
	}

	t.Run("block access type has no fsType", func(t *testing.T) {
		fstype, flags, err := resolveFSType(blockCap, map[string]string{consts.FsTypeField: "vfat"})
		require.NoError(t, err)
		assert.Empty(t, fstype)
		assert.Empty(t, flags)
	})

	t.Run("default is used when nothing is set", func(t *testing.T) {
		fstype, _, err := resolveFSType(mountCap(""), nil)
		require.NoError(t, err)
		assert.Equal(t, getDefaultFsType(), fstype)
	})

	t.Run("mount flags are preserved", func(t *testing.T) {
		_, flags, err := resolveFSType(mountCap("", "barrier=1", "acl"), nil)
		require.NoError(t, err)
		assert.Equal(t, []string{"barrier=1", "acl"}, flags)
	})

	if runtime.GOOS == "windows" {
		// The Windows mounters ignore fsType and always format NTFS, so an unsupported
		// value must degrade to the default rather than fail staging.
		t.Run("unsupported fsType falls back to ntfs", func(t *testing.T) {
			for _, fsType := range []string{"ext4", "xfs", "btrfs", "vfat", "ext4;rm -rf /"} {
				fstype, flags, err := resolveFSType(mountCap(fsType, "acl"), nil)
				require.NoError(t, err, "fsType %q must not fail staging on windows", fsType)
				assert.Equal(t, defaultWindowsFsType, fstype, "fsType %q", fsType)
				assert.Equal(t, []string{"acl"}, flags, "fsType %q", fsType)
			}
		})

		t.Run("unsupported fsType from volumeAttributes falls back to ntfs", func(t *testing.T) {
			fstype, _, err := resolveFSType(mountCap("ntfs"), map[string]string{consts.FsTypeField: "ext4"})
			require.NoError(t, err)
			assert.Equal(t, defaultWindowsFsType, fstype)
		})

		t.Run("ntfs is accepted", func(t *testing.T) {
			fstype, _, err := resolveFSType(mountCap("NTFS"), nil)
			require.NoError(t, err)
			assert.Equal(t, defaultWindowsFsType, fstype)
		})
	} else {
		t.Run("unsupported fsType is rejected", func(t *testing.T) {
			for _, fsType := range []string{"ntfs", "vfat", "ext4;rm -rf /"} {
				_, _, err := resolveFSType(mountCap(fsType), nil)
				require.Error(t, err, "fsType %q must be rejected", fsType)
			}
		})

		t.Run("volumeAttributes overrides the capability", func(t *testing.T) {
			fstype, _, err := resolveFSType(mountCap("ext4"), map[string]string{consts.FsTypeField: "xfs"})
			require.NoError(t, err)
			assert.Equal(t, "xfs", fstype)
		})

		t.Run("casing is normalized", func(t *testing.T) {
			fstype, _, err := resolveFSType(mountCap("XFS"), nil)
			require.NoError(t, err)
			assert.Equal(t, "xfs", fstype)
		})
	}
}

// assertMountedXFSWithNouuid checks that the fstype resolved by NodeStageVolume is what
// actually reached mount, and that collectMountOptions saw it (xfs implies "nouuid").
func assertMountedXFSWithNouuid(t *testing.T, d FakeDriver) {
	t.Helper()
	fake, ok := d.getMounter().Interface.(*mounter.FakeSafeMounter)
	require.True(t, ok, "expected a FakeSafeMounter")
	require.Len(t, fake.MountCalls, 1, "expected exactly one mount")
	assert.Equal(t, "xfs", fake.MountCalls[0].FSType)
	assert.Contains(t, fake.MountCalls[0].Options, "nouuid")
}

func TestNodeStageVolumeWithoutKubeClient(t *testing.T) {
	newRequest := func() *csi.NodeStageVolumeRequest {
		return &csi.NodeStageVolumeRequest{
			VolumeId:          "vol_1",
			StagingTargetPath: t.TempDir(),
			VolumeCapability: &csi.VolumeCapability{
				AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
				AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
			},
		}
	}

	t.Run("controller-driven volume uses PublishContext fallback", func(t *testing.T) {
		controller := gomock.NewController(t)
		driver, _ := NewFakeDriver(controller)
		driver.(*fakeDriver).kubeClient = nil

		_, err := driver.NodeStageVolume(context.Background(), newRequest())

		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.Contains(t, err.Error(), "lun not provided")
	})

	t.Run("QAD volume reports metadata lookup error", func(t *testing.T) {
		controller := gomock.NewController(t)
		driver, _ := NewFakeDriver(controller)
		driver.(*fakeDriver).kubeClient = nil
		driver.(*fakeDriver).nodeDrivenAttachDetachEnabled = true

		_, err := driver.NodeStageVolume(context.Background(), newRequest())

		assert.Equal(t, codes.Internal, status.Code(err))
		assert.Contains(t, err.Error(), errPVMetadataUnavailable.Error())
	})

	t.Run("controller-driven volume uses PublishContext when QAD is enabled", func(t *testing.T) {
		controller := gomock.NewController(t)
		driver, _ := NewFakeDriver(controller)
		driver.(*fakeDriver).kubeClient = nil
		driver.(*fakeDriver).nodeDrivenAttachDetachEnabled = true
		request := newRequest()
		request.PublishContext = map[string]string{consts.LUN: "invalid-lun"}

		_, err := driver.NodeStageVolume(context.Background(), request)

		assert.NotEqual(t, codes.FailedPrecondition, status.Code(err))
	})
}

func TestNodeUnstageVolumeWithoutKubeClient(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake mounter cannot unmount a missing staging path on Windows")
	}

	newDriver := func(t *testing.T, qadEnabled bool) FakeDriver {
		t.Helper()
		controller := gomock.NewController(t)
		driver, _ := NewFakeDriver(controller)
		driver.(*fakeDriver).kubeClient = nil
		driver.(*fakeDriver).nodeDrivenAttachDetachEnabled = qadEnabled
		fakeMounter, err := mounter.NewFakeSafeMounter()
		require.NoError(t, err)
		driver.setMounter(fakeMounter)
		return driver
	}

	t.Run("controller-driven volume completes cleanup", func(t *testing.T) {
		driver := newDriver(t, false)

		_, err := driver.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
			VolumeId:          "vol_1",
			StagingTargetPath: filepath.Join(t.TempDir(), "missing-staging-path"),
		})

		assert.NoError(t, err)
	})

	t.Run("QAD volume requires Kubernetes metadata", func(t *testing.T) {
		driver := newDriver(t, true)

		_, err := driver.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
			VolumeId:          "vol_1",
			StagingTargetPath: filepath.Join(t.TempDir(), "missing-staging-path"),
		})

		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}

func TestNodeStageVolume(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
	d.(*fakeDriver).kubeClient = fake.NewClientset(newTestPV("vol_1"))

	stdVolCap := &csi.VolumeCapability_Mount{
		Mount: &csi.VolumeCapability_MountVolume{
			FsType: defaultLinuxFsType,
		},
	}
	volumeContext := map[string]string{
		consts.FsTypeField: defaultLinuxFsType,
	}
	volumeContextWithResize := map[string]string{
		consts.ResizeRequired: "true",
	}
	volumeContextWithMaxShare := map[string]string{
		consts.MaxSharesField: "0.1",
	}
	volumeContextWithPerfProfileField := map[string]string{
		consts.PerfProfileField: "wrong",
	}
	volumeContextWithInvalidFSType := map[string]string{
		consts.FsTypeField: "vfat",
	}
	volumeContextWithXFS := map[string]string{
		consts.FsTypeField: "xfs",
	}
	volumeContextWithNTFS := map[string]string{
		consts.FsTypeField: "ntfs",
	}

	// fsType casing is normalized before it reaches mkfs/mount and collectMountOptions.
	stdVolCapUpperXFS := &csi.VolumeCapability_Mount{
		Mount: &csi.VolumeCapability_MountVolume{
			FsType: "XFS",
		},
	}

	stdVolCapBlock := &csi.VolumeCapability_Block{
		Block: &csi.VolumeCapability_BlockVolume{},
	}

	volumeCap := csi.VolumeCapability_AccessMode{Mode: 2}
	volumeCapWrong := csi.VolumeCapability_AccessMode{Mode: 10}
	invalidLUN := map[string]string{
		consts.LUN: "/dev/01",
	}
	publishContext := map[string]string{
		consts.LUN: "/dev/disk/azure/scsi1/lun1",
	}

	blkidAction := func() ([]byte, []byte, error) {
		return []byte("DEVICE=/dev/sdd\nTYPE=ext4"), []byte{}, nil
	}
	xfsBlkidAction := func() ([]byte, []byte, error) {
		return []byte("DEVICE=/dev/sdd\nTYPE=xfs"), []byte{}, nil
	}
	fsckAction := func() ([]byte, []byte, error) {
		return []byte{}, []byte{}, nil
	}
	blockSizeAction := func() ([]byte, []byte, error) {
		return []byte(fmt.Sprintf("%d", stdCapacityRange.RequiredBytes)), []byte{}, nil
	}
	resize2fsAction := func() ([]byte, []byte, error) {
		return []byte{}, []byte{}, nil
	}

	tests := []struct {
		desc        string
		setupFunc   func(*testing.T, FakeDriver)
		req         *csi.NodeStageVolumeRequest
		expectedErr error
		// expectedCode/expectedErrContains assert on the status code plus message
		// substrings, for cases where the exact wording is not what's under test.
		expectedCode        codes.Code
		expectedErrContains []string
		skipOnDarwin        bool
		skipOnWindows       bool
		validateFunc        func(*testing.T, FakeDriver)
		cleanupFunc         func(*testing.T, FakeDriver)
	}{
		{
			desc:        "Volume ID missing",
			req:         &csi.NodeStageVolumeRequest{},
			expectedErr: status.Error(codes.InvalidArgument, "Volume ID not provided"),
		},
		{
			desc:        "Stage target path missing",
			req:         &csi.NodeStageVolumeRequest{VolumeId: "vol_1"},
			expectedErr: status.Error(codes.InvalidArgument, "Staging target not provided"),
		},
		{
			desc:        "Volume capabilities missing",
			req:         &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest},
			expectedErr: status.Error(codes.InvalidArgument, "Volume capability not provided"),
		},
		{
			desc:        "Volume capabilities not supported",
			req:         &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest, VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCapWrong}},
			expectedErr: status.Error(codes.InvalidArgument, "invalid access mode: [access_mode:{mode:10}]"),
		},
		{
			desc: "MaxShares value not supported",
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest, VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
				AccessType: stdVolCapBlock}, VolumeContext: volumeContextWithMaxShare},
			expectedErr: status.Error(codes.InvalidArgument, "MaxShares value not supported"),
		},
		{
			desc: "Volume operation in progress",
			setupFunc: func(_ *testing.T, d FakeDriver) {
				d.getVolumeLocks().TryAcquire("vol_1")
			},
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest, VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
				AccessType: stdVolCapBlock}},
			expectedErr: status.Error(codes.Aborted, fmt.Sprintf(volumeOperationAlreadyExistsFmt, "vol_1")),
			cleanupFunc: func(_ *testing.T, d FakeDriver) {
				d.getVolumeLocks().Release("vol_1")
			},
		},
		{
			desc: "Lun not provided",
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest, VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
				AccessType: stdVolCap}},
			expectedErr: status.Error(codes.InvalidArgument, "lun not provided"),
		},
		{
			desc:         "Invalid Lun",
			skipOnDarwin: true,
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCap},
				PublishContext: invalidLUN,
				VolumeContext:  volumeContext,
			},
			expectedErr: status.Error(codes.Internal, "failed to find disk on lun /dev/01. cannot parse deviceInfo: /dev/01"),
		},
		{
			desc:          "Unsupported fsType",
			skipOnDarwin:  true,
			skipOnWindows: true,
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCap},
				PublishContext: publishContext,
				VolumeContext:  volumeContextWithInvalidFSType,
			},
			expectedCode:        codes.InvalidArgument,
			expectedErrContains: []string{"vfat", "not supported"},
		},
		{
			// No PublishContext: fsType must be rejected before the LUN is even read, so
			// that a bad value cannot first trigger a device rescan that polls for minutes.
			desc:          "invalid fsType rejected before LUN handling",
			skipOnDarwin:  true,
			skipOnWindows: true,
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCap},
				VolumeContext: volumeContextWithInvalidFSType,
			},
			expectedCode:        codes.InvalidArgument,
			expectedErrContains: []string{"vfat", "not supported"},
		},
		{
			// The Linux image ships no NTFS formatter. Windows takes the opposite path and
			// falls back to the default instead of erroring, which needs a Windows runner.
			desc:          "ntfs rejected on Linux",
			skipOnDarwin:  true,
			skipOnWindows: true,
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCap},
				PublishContext: publishContext,
				VolumeContext:  volumeContextWithNTFS,
			},
			expectedCode:        codes.InvalidArgument,
			expectedErrContains: []string{"ntfs", "not supported"},
		},
		{
			// volumeAttributes.fsType overrides the volume capability, so "nouuid" must be
			// derived from the override rather than from the capability's ext4.
			desc:          "xfs from volumeAttributes overrides volume capability and gets nouuid",
			skipOnDarwin:  true,
			skipOnWindows: true,
			setupFunc: func(_ *testing.T, d FakeDriver) {
				d.setNextCommandOutputScripts(xfsBlkidAction, fsckAction, blockSizeAction, xfsBlkidAction, blockSizeAction, xfsBlkidAction)
			},
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCap},
				PublishContext: publishContext,
				VolumeContext:  volumeContextWithXFS,
			},
			expectedErr:  nil,
			validateFunc: assertMountedXFSWithNouuid,
		},
		{
			desc:          "uppercase XFS in volume capability is normalized and gets nouuid",
			skipOnDarwin:  true,
			skipOnWindows: true,
			setupFunc: func(_ *testing.T, d FakeDriver) {
				d.setNextCommandOutputScripts(xfsBlkidAction, fsckAction, blockSizeAction, xfsBlkidAction, blockSizeAction, xfsBlkidAction)
			},
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCapUpperXFS},
				PublishContext: publishContext,
			},
			expectedErr:  nil,
			validateFunc: assertMountedXFSWithNouuid,
		},
		{
			desc:          "Successfully staged",
			skipOnDarwin:  true,
			skipOnWindows: true,
			setupFunc: func(_ *testing.T, d FakeDriver) {
				d.setNextCommandOutputScripts(blkidAction, fsckAction, blockSizeAction, blkidAction, blockSizeAction, blkidAction)
			},
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCap},
				PublishContext: publishContext,
				VolumeContext:  volumeContext,
			},
			expectedErr: nil,
		},
		{
			desc:          "Successfully with resize",
			skipOnDarwin:  true,
			skipOnWindows: true,
			setupFunc: func(_ *testing.T, d FakeDriver) {
				d.setNextCommandOutputScripts(blkidAction, fsckAction, blkidAction, resize2fsAction)
			},
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCap},
				PublishContext: publishContext,
				VolumeContext:  volumeContextWithResize,
			},
			expectedErr: nil,
		},
		{
			desc:          "failed to get perf attributes",
			skipOnDarwin:  true,
			skipOnWindows: true,
			setupFunc: func(_ *testing.T, d FakeDriver) {
				d.setPerfOptimizationEnabled(true)
				d.setNextCommandOutputScripts(blkidAction, fsckAction, blockSizeAction, blockSizeAction)
			},
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCap},
				PublishContext: publishContext,
				VolumeContext:  volumeContextWithPerfProfileField,
			},
			cleanupFunc: func(_ *testing.T, d FakeDriver) {
				d.setPerfOptimizationEnabled(false)
			},
			expectedErr: status.Errorf(codes.Internal, "failed to get perf attributes for /dev/sdd. Error: %v", fmt.Errorf("Perf profile wrong is invalid")),
		},
		{
			desc:          "Successfully staged with performance optimizations",
			skipOnDarwin:  true,
			skipOnWindows: true,
			setupFunc: func(_ *testing.T, d FakeDriver) {
				d.setPerfOptimizationEnabled(true)
				mockoptimization := d.getDeviceHelper().(*mockoptimization.MockInterface)
				diskSupportsPerfOptimizationCall := mockoptimization.EXPECT().
					DiskSupportsPerfOptimization(gomock.Any(), gomock.Any()).
					Return(true)
				mockoptimization.EXPECT().
					OptimizeDiskPerformance(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil).
					After(diskSupportsPerfOptimizationCall)

				d.setNextCommandOutputScripts(blkidAction, fsckAction, blockSizeAction, blockSizeAction)
			},
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCap},
				PublishContext: publishContext,
				VolumeContext:  volumeContext,
			},
			cleanupFunc: func(_ *testing.T, d FakeDriver) {
				d.setPerfOptimizationEnabled(false)
			},
			expectedErr: nil,
		},
		{
			desc:          "failed to optimize device performance",
			skipOnDarwin:  true,
			skipOnWindows: true,
			setupFunc: func(_ *testing.T, d FakeDriver) {
				d.setPerfOptimizationEnabled(true)
				mockoptimization := d.getDeviceHelper().(*mockoptimization.MockInterface)
				diskSupportsPerfOptimizationCall := mockoptimization.EXPECT().
					DiskSupportsPerfOptimization(gomock.Any(), gomock.Any()).
					Return(true)
				mockoptimization.EXPECT().
					OptimizeDiskPerformance(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(fmt.Errorf("failed to optimize device performance")).
					After(diskSupportsPerfOptimizationCall)

				d.setNextCommandOutputScripts(blkidAction, fsckAction, blockSizeAction, blockSizeAction)
			},
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCap},
				PublishContext: publishContext,
				VolumeContext:  volumeContext,
			},
			cleanupFunc: func(_ *testing.T, d FakeDriver) {
				d.setPerfOptimizationEnabled(false)
			},
			expectedErr: status.Errorf(codes.Internal, "failed to optimize device performance for target(/dev/sdd) error(%s)", fmt.Errorf("failed to optimize device performance")),
		},
		{
			desc:          "Successfully staged with perf optimization is disabled",
			skipOnDarwin:  true,
			skipOnWindows: true,
			setupFunc: func(_ *testing.T, d FakeDriver) {
				d.setPerfOptimizationEnabled(true)
				mockoptimization := d.getDeviceHelper().(*mockoptimization.MockInterface)
				mockoptimization.EXPECT().
					DiskSupportsPerfOptimization(gomock.Any(), gomock.Any()).
					Return(false)

				d.setNextCommandOutputScripts(blkidAction, fsckAction, blockSizeAction, blockSizeAction)
			},
			req: &csi.NodeStageVolumeRequest{VolumeId: "vol_1", StagingTargetPath: sourceTest,
				VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap,
					AccessType: stdVolCap},
				PublishContext: publishContext,
				VolumeContext:  volumeContext,
			},
			cleanupFunc: func(_ *testing.T, d FakeDriver) {
				d.setPerfOptimizationEnabled(false)
			},
			expectedErr: nil,
		},
	}

	for _, test := range tests {
		// Setup
		_ = makeDir(sourceTest)
		_ = makeDir(targetTest)
		fakeMounter, err := mounter.NewFakeSafeMounter()
		assert.NoError(t, err)
		d.setMounter(fakeMounter)
		if !(test.skipOnDarwin && runtime.GOOS == "darwin") && !(test.skipOnWindows && runtime.GOOS == "windows") {
			if test.setupFunc != nil {
				test.setupFunc(t, d)
			}
			_, err := d.NodeStageVolume(context.Background(), test.req)
			if test.desc == "Failed volume mount" {
				assert.Error(t, err)
			} else if len(test.expectedErrContains) > 0 {
				require.Error(t, err)
				assert.Equal(t, test.expectedCode, status.Code(err), "desc: %s", test.desc)
				for _, substr := range test.expectedErrContains {
					assert.Contains(t, status.Convert(err).Message(), substr, "desc: %s", test.desc)
				}
			} else if !reflect.DeepEqual(err, test.expectedErr) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
			}
			if test.validateFunc != nil {
				test.validateFunc(t, d)
			}
			if test.cleanupFunc != nil {
				test.cleanupFunc(t, d)
			}
		}
		// Clean up
		err = os.RemoveAll(sourceTest)
		assert.NoError(t, err)
		err = os.RemoveAll(targetTest)
		assert.NoError(t, err)
	}

}

func TestNodeUnstageVolume(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
	d.(*fakeDriver).kubeClient = fake.NewClientset(newTestPV("vol_1"))
	errorTarget, err := testutil.GetWorkDirPath("error_is_likely_target")
	assert.NoError(t, err)
	targetFile, err := testutil.GetWorkDirPath("abc.go")
	assert.NoError(t, err)

	tests := []struct {
		setup         func()
		desc          string
		req           *csi.NodeUnstageVolumeRequest
		skipOnWindows bool
		skipOnDarwin  bool
		expectedErr   testutil.TestError
		cleanup       func()
	}{
		{
			desc: "Volume ID missing",
			req:  &csi.NodeUnstageVolumeRequest{StagingTargetPath: targetTest},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Volume ID not provided"),
			},
		},
		{
			desc: "Staging target missing ",
			req:  &csi.NodeUnstageVolumeRequest{VolumeId: "vol_1"},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Staging target not provided"),
			},
		},
		{
			desc:          "[Error] CleanupMountPoint error mocked by IsLikelyNotMountPoint",
			req:           &csi.NodeUnstageVolumeRequest{StagingTargetPath: errorTarget, VolumeId: "vol_1"},
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
			req: &csi.NodeUnstageVolumeRequest{StagingTargetPath: targetFile, VolumeId: "vol_1"},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.Aborted, fmt.Sprintf(volumeOperationAlreadyExistsFmt, "vol_1")),
			},
			cleanup: func() {
				d.getVolumeLocks().Release("vol_1")
			},
		},
		{
			desc:          "[Success] Valid request",
			req:           &csi.NodeUnstageVolumeRequest{StagingTargetPath: targetFile, VolumeId: "vol_1"},
			skipOnWindows: true, // error on Windows
			expectedErr:   testutil.TestError{},
		},
	}

	//Setup
	_ = makeDir(errorTarget)
	fakeMounter, err := mounter.NewFakeSafeMounter()
	assert.NoError(t, err)
	d.setMounter(fakeMounter)

	for _, test := range tests {
		if test.setup != nil {
			test.setup()
		}
		if !(runtime.GOOS == "windows" && test.skipOnWindows) &&
			!(runtime.GOOS == "darwin" && test.skipOnDarwin) {
			_, err := d.NodeUnstageVolume(context.Background(), test.req)
			if !testutil.AssertError(&test.expectedErr, err) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
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

func TestNodeUnstageVolumePVLookup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake mounter cannot unmount a missing staging path on Windows")
	}

	tests := []struct {
		desc              string
		qadEnabled        bool
		pvListErr         error
		expectedErrorCode codes.Code
	}{
		{
			desc: "controller-driven mode succeeds when PV is missing",
		},
		{
			desc:              "QAD mode retries when PV is missing",
			qadEnabled:        true,
			expectedErrorCode: codes.Unavailable,
		},
		{
			desc:      "controller-driven mode skips PV lookup errors",
			pvListErr: errors.New("Kubernetes API unavailable"),
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			cntl := gomock.NewController(t)
			d, err := NewFakeDriver(cntl)
			require.NoError(t, err)
			driver := d.(*fakeDriver)
			driver.kubeClient = fake.NewClientset()
			driver.nodeDrivenAttachDetachEnabled = test.qadEnabled
			if test.pvListErr != nil {
				driver.kubeClient.(*fake.Clientset).PrependReactor("list", "persistentvolumes", func(clienttesting.Action) (bool, k8sruntime.Object, error) {
					return true, nil, test.pvListErr
				})
			}
			fakeMounter, err := mounter.NewFakeSafeMounter()
			require.NoError(t, err)
			d.setMounter(fakeMounter)

			result, err := d.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
				VolumeId:          "missing-volume",
				StagingTargetPath: filepath.Join(t.TempDir(), "missing-staging-path"),
			})
			if test.expectedErrorCode != codes.OK {
				require.Equal(t, test.expectedErrorCode, status.Code(err))
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.Equal(t, &csi.NodeUnstageVolumeResponse{}, result)
		})
	}
}

func TestNodeUnstageVolumeQADDetachedResponses(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("QAD is not supported on Windows")
	}

	const volumeID = "qad-volume"
	tests := []struct {
		name              string
		postStatus        AttachmentStatus
		getResponseStatus AttachmentStatus
		emptyPostResponse bool
		nullGetResponse   bool
		expectedRequests  []string
		expectedCode      codes.Code
	}{
		{
			name:             "immediate detached response",
			postStatus:       AttachmentStatusDetached,
			expectedRequests: []string{http.MethodPost},
		},
		{
			name:              "missing response means detached",
			emptyPostResponse: true,
			expectedRequests:  []string{http.MethodPost},
		},
		{
			name:              "polling observes explicit detached response",
			postStatus:        AttachmentStatusDetaching,
			getResponseStatus: AttachmentStatusDetached,
			expectedRequests:  []string{http.MethodPost, http.MethodGet},
		},
		{
			name:             "polling null response means detached",
			postStatus:       AttachmentStatusDetaching,
			nullGetResponse:  true,
			expectedRequests: []string{http.MethodPost, http.MethodGet},
		},
		{
			name:              "polling observes failed response",
			postStatus:        AttachmentStatusDetaching,
			getResponseStatus: AttachmentStatusError,
			expectedRequests:  []string{http.MethodPost, http.MethodGet},
			expectedCode:      codes.Internal,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cntl := gomock.NewController(t)
			d, err := NewFakeDriver(cntl)
			require.NoError(t, err)
			driver := d.(*fakeDriver)
			driver.nodeDrivenAttachDetachEnabled = true

			pv := newTestPV(volumeID)
			pv.Annotations = map[string]string{
				consts.AttachSequenceAnnotation:  "0",
				consts.BlobURLAnnotation:         "blob-url",
				consts.ClaimIdentifierAnnotation: "claim-id",
				consts.QADCachePolicyAnnotation:  "None",
			}
			pv.Spec.CSI.VolumeAttributes = map[string]string{
				consts.CachingModeField: "ReadOnly",
			}
			driver.kubeClient = fake.NewClientset(pv)
			fakeMounter, err := mounter.NewFakeSafeMounter()
			require.NoError(t, err)
			driver.setMounter(fakeMounter)

			credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
			credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil)

			requestCount := 0
			driver.httpClient = &http.Client{Transport: testRoundTripper(func(request *http.Request) (*http.Response, error) {
				require.Less(t, requestCount, len(test.expectedRequests))
				assert.Equal(t, test.expectedRequests[requestCount], request.Method)
				if request.Method == http.MethodPost {
					var requestBody WireserverRequest
					require.NoError(t, json.NewDecoder(request.Body).Decode(&requestBody))
					require.Contains(t, requestBody.DiskOps, volumeID)
					assert.Equal(t, "None", requestBody.DiskOps[volumeID].CachePolicy)
				}
				responseStatus := test.postStatus
				if request.Method == http.MethodGet {
					responseStatus = test.getResponseStatus
				}
				requestCount++
				body := fmt.Sprintf(`{"%s":{"status":"%s"}}`, volumeID, responseStatus)
				if request.Method == http.MethodPost && test.emptyPostResponse {
					body = `{}`
				} else if request.Method == http.MethodGet && test.nullGetResponse {
					body = fmt.Sprintf(`{"%s":null}`, volumeID)
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(body)),
					Header:     make(http.Header),
				}, nil
			})}

			result, err := driver.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
				VolumeId:          volumeID,
				StagingTargetPath: filepath.Join(t.TempDir(), "missing-staging-path"),
			})
			if test.expectedCode != codes.OK {
				require.Equal(t, test.expectedCode, status.Code(err))
				require.Nil(t, result)
				assert.Equal(t, len(test.expectedRequests), requestCount)
				return
			}
			require.NoError(t, err)
			require.Equal(t, &csi.NodeUnstageVolumeResponse{}, result)
			assert.Equal(t, len(test.expectedRequests), requestCount)
		})
	}
}

func TestGetQADCachePolicy(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		expected   string
	}{
		{
			name:       "effective policy annotation overrides volume attributes",
			annotation: "None",
			expected:   "None",
		},
		{
			name:     "volume attributes support existing PVs",
			expected: "ReadOnly",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pv := newTestPV("qad-volume")
			pv.Spec.CSI.VolumeAttributes = map[string]string{
				consts.CachingModeField: "ReadOnly",
			}
			if test.annotation != "" {
				pv.Annotations = map[string]string{
					consts.QADCachePolicyAnnotation: test.annotation,
				}
			}

			cachePolicy, err := getQADCachePolicy(pv)
			require.NoError(t, err)
			assert.Equal(t, test.expected, string(cachePolicy))
		})
	}
}

// TestPostAttachErrorCode verifies that failures after a successful QAD attach
// are retryable (so kubelet keeps the device uncertain and still runs
// NodeUnstage), while controller-driven volumes keep a terminal Internal code.
func TestPostAttachErrorCode(t *testing.T) {
	assert.Equal(t, codes.Unavailable, postAttachErrorCode(true))
	assert.Equal(t, codes.Internal, postAttachErrorCode(false))
}

func TestExecuteQADDiskOperationNullStatus(t *testing.T) {
	tests := []struct {
		name          string
		operationType string
		expectedCode  codes.Code
	}{
		{
			name:          "attach returns internal error",
			operationType: attachOperation,
			expectedCode:  codes.Internal,
		},
		{
			name:          "detach treats null as already detached",
			operationType: detachOperation,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cntl := gomock.NewController(t)
			d, err := NewFakeDriver(cntl)
			require.NoError(t, err)
			driver := d.(*fakeDriver)

			credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
			credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil)
			driver.httpClient = &http.Client{Transport: testRoundTripper(func(_ *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"qad-volume":null}`)),
					Header:     make(http.Header),
				}, nil
			})}

			diskStatus, err := driver.executeQADDiskOperation(context.Background(), DiskOperationRequest{DiskURI: "qad-volume"}, test.operationType)
			require.Equal(t, test.expectedCode, status.Code(err))
			require.Nil(t, diskStatus)
		})
	}
}

func TestAttachOrDetachDiskInternalWithoutCredential(t *testing.T) {
	response, err := attachOrDetachDiskInternal(context.Background(), http.Client{}, DiskOperationRequest{
		DiskURI: "qad-volume",
	}, nil, attachOperation)

	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, "Azure credential is required for QAD disk operations")
	assert.Empty(t, response)
}
func TestExecuteQADDiskOperationFindsResponseCaseInsensitively(t *testing.T) {
	tests := []struct {
		name           string
		operationType  string
		responseStatus AttachmentStatus
	}{
		{
			name:           "attach",
			operationType:  attachOperation,
			responseStatus: AttachmentStatusAttached,
		},
		{
			name:           "detach",
			operationType:  detachOperation,
			responseStatus: AttachmentStatusDetached,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cntl := gomock.NewController(t)
			d, err := NewFakeDriver(cntl)
			require.NoError(t, err)
			driver := d.(*fakeDriver)

			credential := driver.cloud.AuthProvider.GetAzIdentity().(*mock_azclient.MockTokenCredential)
			credential.EXPECT().GetToken(gomock.Any(), gomock.Any()).Return(azcore.AccessToken{Token: "token"}, nil)
			driver.httpClient = &http.Client{Transport: testRoundTripper(func(request *http.Request) (*http.Response, error) {
				assert.Equal(t, http.MethodPost, request.Method)
				body := fmt.Sprintf(`{"QAD-VOLUME":{"status":"%s","lun":1}}`, test.responseStatus)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(body)),
					Header:     make(http.Header),
				}, nil
			})}

			diskStatus, err := driver.executeQADDiskOperation(context.Background(), DiskOperationRequest{DiskURI: "qad-volume"}, test.operationType)
			require.NoError(t, err)
			require.NotNil(t, diskStatus)
			assert.Equal(t, test.responseStatus, diskStatus.Status)
			assert.Equal(t, 1, diskStatus.LUN)
		})
	}
}

func TestMapHTTPStatusToCodeTooManyRequests(t *testing.T) {
	assert.Equal(t, codes.ResourceExhausted, mapHTTPStatusToCode(http.StatusTooManyRequests))
}

func TestGetDiskState(t *testing.T) {
	tests := []struct {
		name           string
		responseStatus int
		responseBody   string
		expectedNil    bool
		expectedCode   codes.Code
		expectedError  string
	}{
		{
			name:           "disk status found case insensitively",
			responseStatus: http.StatusOK,
			responseBody:   `{"QAD-VOLUME":{"status":"DISK_STATUS_ATTACHED","lun":1}}`,
		},
		{
			name:           "null disk status",
			responseStatus: http.StatusOK,
			responseBody:   `{"qad-volume":null}`,
			expectedNil:    true,
		},
		{
			name:           "disk status not found",
			responseStatus: http.StatusOK,
			responseBody:   `{}`,
			expectedNil:    true,
			expectedCode:   codes.NotFound,
		},
		{
			name:           "non-success status is checked before unmarshal",
			responseStatus: http.StatusServiceUnavailable,
			responseBody:   `not-json`,
			expectedNil:    true,
			expectedCode:   codes.Unavailable,
			expectedError:  "wireserver returned HTTP 503: not-json",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := http.Client{Transport: testRoundTripper(func(_ *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: test.responseStatus,
					Body:       io.NopCloser(strings.NewReader(test.responseBody)),
					Header:     make(http.Header),
				}, nil
			})}

			diskStatus, err := getDiskState(context.Background(), client, "qad-volume")
			require.Equal(t, test.expectedCode, status.Code(err))
			if test.expectedError != "" {
				require.ErrorContains(t, err, test.expectedError)
			}
			if test.expectedNil {
				require.Nil(t, diskStatus)
				return
			}
			require.NotNil(t, diskStatus)
			assert.Equal(t, AttachmentStatusAttached, diskStatus.Status)
			assert.Equal(t, 1, diskStatus.LUN)
		})
	}
}

func TestNodePublishVolume(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)

	volumeCap := csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER}
	volumeCapRWO := csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER}
	volumeCapSingleWriter := csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}
	volumeCapReadOnlyMany := csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY}
	volumeCapWrong := csi.VolumeCapability_AccessMode{Mode: 10}
	volumeContextWithMaxShare := map[string]string{
		consts.MaxSharesField: "0.1",
	}
	publishContext := map[string]string{
		consts.LUN: "/dev/01",
	}
	validPublishContext := map[string]string{
		consts.LUN: "/dev/disk/azure/scsi1/lun1",
	}
	errorMountSource, err := testutil.GetWorkDirPath("error_mount_source")
	assert.NoError(t, err)
	alreadyMountedTarget, err := testutil.GetWorkDirPath("false_is_likely_exist_target")
	assert.NoError(t, err)

	azurediskPath := "azuredisk.go"

	// ".\azuredisk.go will get deleted on Windows"
	if runtime.GOOS == "windows" {
		azurediskPath = "testfiles\\azuredisk.go"
	}
	azuredisk, err := testutil.GetWorkDirPath(azurediskPath)
	assert.NoError(t, err)

	stdVolCap := &csi.VolumeCapability_Mount{
		Mount: &csi.VolumeCapability_MountVolume{},
	}
	stdVolCapDiscard := &csi.VolumeCapability_Mount{
		Mount: &csi.VolumeCapability_MountVolume{
			MountFlags: []string{"discard"},
		},
	}
	stdVolCapBlock := &csi.VolumeCapability_Block{
		Block: &csi.VolumeCapability_BlockVolume{},
	}

	directVolumeContext := map[string]string{
		podNameField:      "direct-volume-pod",
		podNamespaceField: "default", podUIDField: "test-pod-uid",
	}
	directFSGroupVolumeContext := map[string]string{
		podNameField:      "direct-volume-fsgroup-pod",
		podNamespaceField: "default", podUIDField: "test-pod-uid",
	}

	directVolumeErrorTarget, err := testutil.GetWorkDirPath("direct_volume_error_target")
	assert.NoError(t, err)
	standardTarget, err := testutil.GetWorkDirPath("standard_publish_target")
	assert.NoError(t, err)
	lookupErrorTarget, err := testutil.GetWorkDirPath("runtime_lookup_error_target")
	assert.NoError(t, err)
	directBlockTarget, err := testutil.GetWorkDirPath("direct_block_target")
	assert.NoError(t, err)
	blockStagingTarget := t.TempDir()
	directFSGroupTarget, err := testutil.GetWorkDirPath("direct_fsgroup_target")
	assert.NoError(t, err)

	tests := []struct {
		desc            string
		enableKataMount bool
		setup           func()
		req             *csi.NodePublishVolumeRequest
		skipOnWindows   bool
		skipOnDarwin    bool
		expectedErr     testutil.TestError
		expectedInfo    *directvolume.MountInfo
		expectedMount   bool
		expectTarget    bool
		cleanup         func()
	}{
		{
			desc: "Volume capabilities missing",
			req:  &csi.NodePublishVolumeRequest{VolumeId: "vol_1"},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Volume capability missing in request"),
			},
		},
		{
			desc: "Volume ID missing",
			req:  &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapBlock}},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Volume ID missing in the request"),
			},
		},
		{
			desc: "MaxShares value not supported",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapBlock},
				VolumeId:      "vol_1",
				VolumeContext: volumeContextWithMaxShare},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "MaxShares value not supported"),
			},
		},
		{
			desc: "Volume capability not supported",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCapWrong, AccessType: stdVolCapBlock},
				VolumeId: "vol_1"},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "invalid access mode: [block:{}  access_mode:{mode:10}]"),
			},
		},
		{
			desc: "Staging target path missing",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapBlock},
				VolumeId: "vol_1"},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Staging target not provided"),
			},
		},
		{
			desc: "Target path missing",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapBlock},
				VolumeId:          "vol_1",
				StagingTargetPath: sourceTest},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Target path not provided"),
			},
		},
		{
			desc: "[Error] Not a directory",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCap},
				VolumeId:          "vol_1",
				TargetPath:        azuredisk,
				StagingTargetPath: sourceTest,
				Readonly:          true},
			skipOnWindows: true, // permission issues
			skipOnDarwin:  true,
			expectedErr: testutil.TestError{
				DefaultError: status.Errorf(codes.Internal, "could not mount target \"%s\": "+
					"mkdir %s: not a directory", azuredisk, azuredisk),
			},
		},
		{
			desc: "[Error] Lun not provided",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapBlock},
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
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapBlock},
				VolumeId:          "vol_1",
				TargetPath:        azuredisk,
				StagingTargetPath: sourceTest,
				PublishContext:    publishContext,
				Readonly:          true},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.Internal, "failed to find device path with lun /dev/01. cannot parse deviceInfo: /dev/01"),
			},
		},
		{
			desc: "[Error] Mount error mocked by Mount",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCap},
				VolumeId:          "vol_1",
				TargetPath:        targetTest,
				StagingTargetPath: errorMountSource,
				Readonly:          true},
			skipOnWindows: true, // permission issues
			expectedErr: testutil.TestError{
				DefaultError: status.Errorf(codes.Internal, "could not mount \"%s\" at \"%s\": "+
					"fake Mount: source error", errorMountSource, targetTest),
			},
		},
		{
			desc: "[Success] Valid request already mounted",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCap},
				VolumeId:          "vol_1",
				TargetPath:        alreadyMountedTarget,
				StagingTargetPath: sourceTest,
				Readonly:          true},
			skipOnWindows: true, // permission issues
			expectedErr:   testutil.TestError{},
		},
		{
			desc: "[Success] Valid request",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCap},
				VolumeId:          "vol_1",
				TargetPath:        targetTest,
				StagingTargetPath: sourceTest,
				Readonly:          true},
			skipOnWindows: true, // permission issues
			expectedErr:   testutil.TestError{},
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Success] Standard mount for pod without runtime metadata",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCapRWO, AccessType: stdVolCapDiscard},
				VolumeId:          "vol_1",
				TargetPath:        standardTarget,
				StagingTargetPath: sourceTest,
				Readonly:          true},
			expectedErr:   testutil.TestError{},
			expectedMount: true,
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Error] Kata filesystem volume with ReadWriteOnce access mode",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCapRWO, AccessType: stdVolCapDiscard},
				VolumeId:          "vol_1",
				TargetPath:        targetTest,
				StagingTargetPath: sourceTest,
				VolumeContext:     directVolumeContext},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.FailedPrecondition, "volume needs ReadWriteOncePod access mode with Kata"),
			},
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Error] Kata filesystem volume with legacy single-writer access mode",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCapSingleWriter, AccessType: stdVolCapDiscard},
				VolumeId:          "vol_1",
				TargetPath:        targetTest,
				StagingTargetPath: sourceTest,
				VolumeContext:     directVolumeContext},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.FailedPrecondition, "volume needs ReadWriteOncePod access mode with Kata"),
			},
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Error] Kata filesystem volume with ReadOnlyMany access mode",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCapReadOnlyMany, AccessType: stdVolCapDiscard},
				VolumeId:          "vol_1",
				TargetPath:        targetTest,
				StagingTargetPath: sourceTest,
				VolumeContext:     directVolumeContext},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "mountVolume is not supported for access mode: MULTI_NODE_READER_ONLY"),
			},
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Success] Kata raw block volume with ReadWriteOnce uses regular mount",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCapRWO, AccessType: stdVolCapBlock},
				VolumeId:          "vol_1",
				TargetPath:        directBlockTarget,
				StagingTargetPath: blockStagingTarget,
				PublishContext:    validPublishContext,
				VolumeContext:     directFSGroupVolumeContext},
			expectedErr:   testutil.TestError{},
			expectedMount: true,
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Success] Pod lookup error falls back to standard mount",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapDiscard},
				VolumeId:          "vol_1",
				TargetPath:        lookupErrorTarget,
				StagingTargetPath: sourceTest,
				VolumeContext: map[string]string{
					podNameField:      "missing-pod",
					podNamespaceField: "default", podUIDField: "test-pod-uid",
				},
				Readonly: true},
			expectedErr:   testutil.TestError{},
			expectedMount: true,
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Error] Add direct volume",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapDiscard},
				VolumeId:          "vol_1",
				TargetPath:        directVolumeErrorTarget,
				StagingTargetPath: sourceTest,
				PublishContext:    validPublishContext,
				VolumeContext:     directFSGroupVolumeContext,
				Readonly:          true},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.Internal, "failed to add direct volume: fake add direct volume error"),
			},
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Success] Kata read-only filesystem volume with fsGroup",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapDiscard},
				VolumeId:          "vol_1",
				TargetPath:        targetTest,
				StagingTargetPath: sourceTest,
				PublishContext:    validPublishContext,
				VolumeContext:     directFSGroupVolumeContext,
				Readonly:          true},
			expectedErr: testutil.TestError{},
			expectedInfo: &directvolume.MountInfo{
				VolumeType: kataDirectVolumeType,
				Device:     "/dev/sdd",
				FsType:     defaultLinuxFsType,
				Metadata:   map[string]string{},
				Options:    []string{"discard", "ro"},
			},
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Success] Kata filesystem volume with partition",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapDiscard},
				VolumeId:          "vol_1",
				TargetPath:        targetTest,
				StagingTargetPath: sourceTest,
				PublishContext:    validPublishContext,
				VolumeContext: map[string]string{
					podNameField:      "direct-volume-pod",
					podNamespaceField: "default", podUIDField: "test-pod-uid",
					consts.VolumeAttributePartition: "1",
				}},
			expectedErr: testutil.TestError{},
			expectedInfo: &directvolume.MountInfo{
				VolumeType: kataDirectVolumeType,
				Device:     "/dev/sdd-part1",
				FsType:     defaultLinuxFsType,
				Metadata:   map[string]string{},
				Options:    []string{"discard"},
			},
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Success] Kata filesystem volume with fsGroup",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapDiscard},
				VolumeId:          "vol_1",
				TargetPath:        directFSGroupTarget,
				StagingTargetPath: sourceTest,
				PublishContext:    validPublishContext,
				VolumeContext:     directFSGroupVolumeContext},
			expectedErr: testutil.TestError{},
			expectedInfo: &directvolume.MountInfo{
				VolumeType: kataDirectVolumeType,
				Device:     "/dev/sdd",
				FsType:     defaultLinuxFsType,
				Metadata: map[string]string{
					directvolume.FSGroupMetadataKey: "3000",
				},
				Options: []string{"discard"},
			},
			expectTarget: true,
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Success] Kata read-only raw block volume uses regular mount",
			req: &csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCapBlock},
				VolumeId:          "vol_1",
				TargetPath:        directBlockTarget,
				StagingTargetPath: blockStagingTarget,
				PublishContext:    validPublishContext,
				VolumeContext: map[string]string{
					podNameField:      "direct-volume-fsgroup-pod",
					podNamespaceField: "default", podUIDField: "test-pod-uid",
					consts.VolumeAttributePartition: "1",
				},
				Readonly: true},
			expectedErr:   testutil.TestError{},
			expectedMount: true,
		},
	}

	// Setup
	require.NoError(t, makeDir(alreadyMountedTarget))
	fakeMounter, err := mounter.NewFakeSafeMounter()
	assert.NoError(t, err)
	d.setMounter(fakeMounter)
	kataRuntimeClassName := "kata"
	fsGroup := int64(3000)
	d.(*fakeDriver).kubeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "direct-volume-pod", Namespace: "default", UID: "test-pod-uid"},
			Spec:       corev1.PodSpec{RuntimeClassName: &kataRuntimeClassName},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "direct-volume-fsgroup-pod", Namespace: "default", UID: "test-pod-uid"},
			Spec: corev1.PodSpec{
				RuntimeClassName: &kataRuntimeClassName,
				SecurityContext:  &corev1.PodSecurityContext{FSGroup: &fsGroup},
			},
		},
		&nodev1.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:        kataRuntimeClassName,
				Annotations: map[string]string{kataAnnotationKey: kataAnnotationValue},
			},
			Handler: "kata",
		},
	)
	var addedTarget string
	var addedMountInfo directvolume.MountInfo
	d.(*fakeDriver).kataDirectVolume = &kataStubDirectVolume{
		addMountInfo: func(target string, mountInfo directvolume.MountInfo) error {
			if target == directVolumeErrorTarget {
				return errors.New("fake add direct volume error")
			}
			addedTarget = target
			addedMountInfo = mountInfo
			return nil
		},
	}

	for _, test := range tests {
		d.(*fakeDriver).enableKataMount = test.enableKataMount
		d.(*fakeDriver).isKataNode = true
		d.(*fakeDriver).kubeClient.(*fake.Clientset).ClearActions()
		addedTarget = ""
		addedMountInfo = directvolume.MountInfo{}
		if fm, ok := fakeMounter.Interface.(*mounter.FakeSafeMounter); ok {
			fm.MountCalls = nil
			fm.MountPoints = nil
			if test.expectedMount && test.req.VolumeCapability.GetMount() != nil {
				// Ordinary publish starts with a real staging mount in the CSI lifecycle.
				test.req.PublishContext = validPublishContext
				fm.MountPoints = []mount.MountPoint{{Device: "/dev/sdd", Path: test.req.StagingTargetPath, Type: "ext4", Opts: []string{"discard"}}}
			}
		}
		if test.setup != nil {
			test.setup()
		}
		if !(test.skipOnWindows && runtime.GOOS == "windows") && !(test.skipOnDarwin && runtime.GOOS == "darwin") {
			var err error
			_, err = d.NodePublishVolume(context.Background(), test.req)
			if !testutil.AssertError(&test.expectedErr, err) && (err == nil || !strings.Contains(err.Error(), "invalid access mode")) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
			}
			if test.expectedInfo != nil {
				assert.Equal(t, test.req.TargetPath, addedTarget, test.desc)
				test.expectedInfo.Metadata[kataVolumeIDKey] = test.req.VolumeId
				assert.Equal(t, *test.expectedInfo, addedMountInfo, test.desc)
			}
			if test.expectedMount {
				info, err := os.Stat(test.req.TargetPath)
				require.NoError(t, err, test.desc)
				if test.req.GetVolumeCapability().GetBlock() != nil {
					assert.True(t, info.Mode().IsRegular(), test.desc)
					assert.DirExists(t, test.req.StagingTargetPath, test.desc)
					fm := fakeMounter.Interface.(*mounter.FakeSafeMounter)
					require.Len(t, fm.MountCalls, 1, test.desc)
					options := []string{"bind"}
					if test.req.GetReadonly() {
						options = append(options, "ro")
					}
					assert.Equal(t, mounter.MountCall{
						Source:  "/dev/sdd",
						Target:  test.req.TargetPath,
						Options: options,
					}, fm.MountCalls[0], test.desc)
				} else {
					assert.True(t, info.IsDir(), test.desc)
				}
				assert.Empty(t, addedTarget, test.desc)
			}
			if test.expectTarget {
				info, err := os.Stat(test.req.TargetPath)
				require.NoError(t, err, test.desc)
				assert.True(t, info.IsDir(), test.desc)
			}
		}
		if test.cleanup != nil {
			test.cleanup()
		}
	}

	// Clean up
	err = os.RemoveAll(alreadyMountedTarget)
	assert.NoError(t, err)
	err = os.RemoveAll(targetTest)
	assert.NoError(t, err)
	err = os.RemoveAll(standardTarget)
	assert.NoError(t, err)
	err = os.RemoveAll(lookupErrorTarget)
	assert.NoError(t, err)
	err = os.RemoveAll(directBlockTarget)
	assert.NoError(t, err)
	err = os.RemoveAll(directFSGroupTarget)
	assert.NoError(t, err)
}

func TestNodePublishVolumeKataFSType(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Kata filesystem mounts are only supported on Linux")
	}

	tests := []struct {
		name          string
		fsType        string
		contextFSType string
		mountFlags    []string
		readOnly      bool
		wantFSType    string
		wantOptions   []string
		wantErr       bool
	}{
		{
			name:        "default filesystem",
			mountFlags:  []string{"discard"},
			wantFSType:  "ext4",
			wantOptions: []string{"discard"},
		},
		{
			name:        "uppercase capability gets nouuid",
			fsType:      "XFS",
			mountFlags:  []string{"discard"},
			wantFSType:  "xfs",
			wantOptions: []string{"discard", "nouuid"},
		},
		{
			name:          "volume context overrides capability and is normalized",
			fsType:        "ext4",
			contextFSType: " XfS ",
			mountFlags:    []string{"discard"},
			wantFSType:    "xfs",
			wantOptions:   []string{"discard", "nouuid"},
		},
		{
			name:          "valid context overrides unsupported capability",
			fsType:        "vfat",
			contextFSType: "ext4",
			mountFlags:    []string{"discard"},
			wantFSType:    "ext4",
			wantOptions:   []string{"discard"},
		},
		{
			name:        "read-only default filesystem without mount flags",
			readOnly:    true,
			wantFSType:  "ext4",
			wantOptions: []string{"ro"},
		},
		{
			name:        "read-only preserves discard",
			mountFlags:  []string{"discard"},
			readOnly:    true,
			wantFSType:  "ext4",
			wantOptions: []string{"discard", "ro"},
		},
		{
			name:        "read-only XFS preserves discard and nouuid",
			fsType:      "XFS",
			mountFlags:  []string{"discard"},
			readOnly:    true,
			wantFSType:  "xfs",
			wantOptions: []string{"discard", "nouuid", "ro"},
		},
		{
			name:        "read-only takes precedence over rw mount flag",
			mountFlags:  []string{"discard", "rw"},
			readOnly:    true,
			wantFSType:  "ext4",
			wantOptions: []string{"discard", "rw", "ro"},
		},
		{
			name:        "writable publication preserves explicit ro mount flag",
			mountFlags:  []string{"discard", "ro"},
			wantFSType:  "ext4",
			wantOptions: []string{"discard", "ro"},
		},
		{
			name:       "unsupported capability fails before device discovery",
			fsType:     "vfat",
			mountFlags: []string{"discard"},
			wantErr:    true,
		},
		{
			name:          "unsupported context fails before device discovery",
			fsType:        "ext4",
			contextFSType: "vfat",
			mountFlags:    []string{"discard"},
			wantErr:       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, err := NewFakeDriver(gomock.NewController(t))
			require.NoError(t, err)
			d.(*fakeDriver).enableKataMount = true
			d.(*fakeDriver).isKataNode = true
			fakeMounter, err := mounter.NewFakeSafeMounter()
			require.NoError(t, err)
			d.setMounter(fakeMounter)
			runtimeClassName := "kata"
			d.(*fakeDriver).kubeClient = fake.NewSimpleClientset(
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "default", UID: "test-pod-uid"},
					Spec:       corev1.PodSpec{RuntimeClassName: &runtimeClassName},
				},
				&nodev1.RuntimeClass{
					ObjectMeta: metav1.ObjectMeta{
						Name:        runtimeClassName,
						Annotations: map[string]string{kataAnnotationKey: kataAnnotationValue},
					},
					Handler: "kata",
				},
			)
			var addedTarget string
			var addedInfo directvolume.MountInfo
			d.(*fakeDriver).kataDirectVolume = &kataStubDirectVolume{
				addMountInfo: func(target string, info directvolume.MountInfo) error {
					addedTarget, addedInfo = target, info
					return nil
				},
			}
			req := &csi.NodePublishVolumeRequest{
				VolumeId:          "vol_1",
				TargetPath:        t.TempDir(),
				StagingTargetPath: t.TempDir(),
				Readonly:          test.readOnly,
				PublishContext:    map[string]string{consts.LUN: "/dev/disk/azure/scsi1/lun1"},
				VolumeContext: map[string]string{
					podNameField:      "pod",
					podNamespaceField: "default", podUIDField: "test-pod-uid",
					consts.FsTypeField: test.contextFSType,
				},
				VolumeCapability: &csi.VolumeCapability{
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER},
					AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{
						FsType:     test.fsType,
						MountFlags: append([]string(nil), test.mountFlags...),
					}},
				},
			}
			if test.wantErr {
				// An invalid filesystem must be rejected before the LUN is needed.
				req.PublishContext = nil
			}

			_, err = d.NodePublishVolume(context.Background(), req)
			if test.wantErr {
				require.Error(t, err)
				assert.Equal(t, codes.InvalidArgument, status.Code(err))
				assert.Contains(t, err.Error(), "not supported")
				assert.Empty(t, addedTarget)
				assert.DirExists(t, req.StagingTargetPath)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, req.TargetPath, addedTarget)
			assert.Equal(t, test.wantFSType, addedInfo.FsType)
			assert.Equal(t, test.wantOptions, addedInfo.Options)
			assert.Equal(t, test.mountFlags, req.VolumeCapability.GetMount().MountFlags)
		})
	}
}

func TestNodeUnpublishVolume(t *testing.T) {
	cntl := gomock.NewController(t)
	d, _ := NewFakeDriver(cntl)
	errorTarget, err := testutil.GetWorkDirPath("error_is_likely_target")
	assert.NoError(t, err)
	targetFile, err := testutil.GetWorkDirPath("abc.go")
	assert.NoError(t, err)
	removeErrorTarget := filepath.Join(t.TempDir(), "missing-target")
	standardTarget, err := testutil.GetWorkDirPath("standard_unpublish_target")
	assert.NoError(t, err)
	probeErrorTarget, err := testutil.GetWorkDirPath("direct_volume_probe_error_target")
	assert.NoError(t, err)
	directFSGroupTarget, err := testutil.GetWorkDirPath("direct_fsgroup_unpublish_target")
	assert.NoError(t, err)

	tests := []struct {
		enableKataMount bool
		setup           func()
		desc            string
		req             *csi.NodeUnpublishVolumeRequest
		skipOnWindows   bool
		skipOnDarwin    bool
		expectedErr     testutil.TestError
		expectedTarget  string
		expectRemoved   bool
		cleanup         func()
	}{
		{
			desc: "Volume ID missing",
			req:  &csi.NodeUnpublishVolumeRequest{TargetPath: targetTest},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Volume ID missing in the request"),
			},
		},
		{
			desc: "Target missing",
			req:  &csi.NodeUnpublishVolumeRequest{VolumeId: "vol_1"},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Target path missing in request"),
			},
		},
		{
			desc:          "[Error] Unmount error mocked by IsLikelyNotMountPoint",
			req:           &csi.NodeUnpublishVolumeRequest{TargetPath: errorTarget, VolumeId: "vol_1"},
			skipOnWindows: true, // no error reported in windows
			skipOnDarwin:  true, // no error reported in darwin
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.Internal, fmt.Sprintf("failed to unmount target \"%s\": fake IsLikelyNotMountPoint: fake error", errorTarget)),
			},
		},
		{
			desc:        "[Success] Valid request",
			req:         &csi.NodeUnpublishVolumeRequest{TargetPath: targetFile, VolumeId: "vol_1"},
			expectedErr: testutil.TestError{},
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Error] Remove direct volume",
			req:             &csi.NodeUnpublishVolumeRequest{TargetPath: removeErrorTarget, VolumeId: "vol_1"},
			expectedErr: testutil.TestError{
				DefaultError: status.Errorf(codes.Internal, "failed to remove direct volume %q: fake remove direct volume error", removeErrorTarget),
			},
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Success] Kata valid request",
			req:             &csi.NodeUnpublishVolumeRequest{TargetPath: targetFile, VolumeId: "vol_1"},
			expectedErr:     testutil.TestError{},
			expectedTarget:  targetFile,
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Success] Direct volume with fsGroup target",
			req:             &csi.NodeUnpublishVolumeRequest{TargetPath: directFSGroupTarget, VolumeId: "vol_1"},
			expectedErr:     testutil.TestError{},
			expectedTarget:  directFSGroupTarget,
			expectRemoved:   true,
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Success] Standard mount",
			req:             &csi.NodeUnpublishVolumeRequest{TargetPath: standardTarget, VolumeId: "vol_1"},
			expectedErr:     testutil.TestError{},
			expectedTarget:  standardTarget,
			expectRemoved:   true,
		},
		{
			enableKataMount: true,
			skipOnWindows:   true,
			skipOnDarwin:    true,
			desc:            "[Error] Direct volume removal failure after target cleanup",
			req:             &csi.NodeUnpublishVolumeRequest{TargetPath: probeErrorTarget, VolumeId: "vol_1"},
			expectedErr: testutil.TestError{
				DefaultError: status.Errorf(codes.Internal, "failed to remove direct volume %q: fake remove direct volume error", probeErrorTarget),
			},
			expectRemoved: true,
		},
	}

	// Setup
	_ = makeDir(errorTarget)
	_ = makeDir(standardTarget)
	_ = makeDir(probeErrorTarget)
	_ = makeDir(directFSGroupTarget)
	fakeMounter, err := mounter.NewFakeSafeMounter()
	assert.NoError(t, err)
	d.setMounter(fakeMounter)
	var removedTarget string
	d.(*fakeDriver).kataDirectVolume = &kataStubDirectVolume{
		isVolumeMounted: func(target string) (bool, error) {
			t.Fatal("unpublish must not parse metadata before removal")
			return false, nil
		},
		remove: func(target string) error {
			if target == removeErrorTarget || target == probeErrorTarget {
				return errors.New("fake remove direct volume error")
			}
			removedTarget = target
			return nil
		},
	}

	for _, test := range tests {
		d.(*fakeDriver).enableKataMount = test.enableKataMount
		d.(*fakeDriver).isKataNode = true
		removedTarget = ""
		if test.setup != nil {
			test.setup()
		}
		if !(test.skipOnWindows && runtime.GOOS == "windows") &&
			!(test.skipOnDarwin && runtime.GOOS == "darwin") {
			_, err := d.NodeUnpublishVolume(context.Background(), test.req)
			if !testutil.AssertError(&test.expectedErr, err) {
				t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
			}
			assert.Equal(t, test.expectedTarget, removedTarget, test.desc)
			if test.expectRemoved {
				_, err := os.Stat(test.req.TargetPath)
				assert.ErrorIs(t, err, os.ErrNotExist, test.desc)
			}
		}
		if test.cleanup != nil {
			test.cleanup()
		}
	}
	err = os.RemoveAll(errorTarget)
	assert.NoError(t, err)
	_ = os.RemoveAll(standardTarget)
	_ = os.RemoveAll(probeErrorTarget)
	_ = os.RemoveAll(directFSGroupTarget)
}

func TestNodeExpandVolume(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
	fakeMounter, err := mounter.NewFakeSafeMounter()
	assert.NoError(t, err)
	d.setMounter(fakeMounter)
	blockVolumePath := "/tmp/block-volume-path"
	_ = makeDir(blockVolumePath)
	_ = makeDir(targetTest)
	notFoundErr := errors.New("exit status 1")

	stdCapacityRange = &csi.CapacityRange{
		RequiredBytes: volumehelper.GiBToBytes(15),
		LimitBytes:    volumehelper.GiBToBytes(10),
	}

	invalidPathErr := testutil.TestError{
		DefaultError: status.Error(codes.NotFound, "failed to determine device path for volumePath [./test]: path \"./test\" does not exist"),
	}

	devicePathErr := testutil.TestError{
		DefaultError: status.Errorf(codes.NotFound, "could not determine device path(%s), error: %v", targetTest, notFoundErr),
		WindowsError: status.Errorf(codes.NotFound, "error reading link for mount D:\\a\\azuredisk-csi-driver\\azuredisk-csi-driver\\pkg\\azuredisk\\target_test. target  err: readlink D:\\a\\azuredisk-csi-driver\\azuredisk-csi-driver\\pkg\\azuredisk\\target_test: The file or directory is not a reparse point."),
	}
	blockSizeErr := testutil.TestError{
		DefaultError: status.Error(codes.Unavailable, "NodeExpandVolume: block device size did not match requested size: rpc error: code = Internal desc = block volume at path test size check failed: current 0 GiB < requested 15 GiB"),
		WindowsError: status.Errorf(codes.NotFound, "error reading link for mount D:\\a\\azuredisk-csi-driver\\azuredisk-csi-driver\\pkg\\azuredisk\\target_test. target  err: readlink D:\\a\\azuredisk-csi-driver\\azuredisk-csi-driver\\pkg\\azuredisk\\target_test: The file or directory is not a reparse point."),
	}
	resizeErr := testutil.TestError{
		DefaultError: status.Errorf(codes.Internal, "could not resize volume \"test\" (\"test\"):  resize of device test failed: %v. resize2fs output: ", notFoundErr),
		WindowsError: status.Errorf(codes.NotFound, "error reading link for mount D:\\a\\azuredisk-csi-driver\\azuredisk-csi-driver\\pkg\\azuredisk\\target_test. target  err: readlink D:\\a\\azuredisk-csi-driver\\azuredisk-csi-driver\\pkg\\azuredisk\\target_test: The file or directory is not a reparse point."),
	}
	sizeTooSmallErr := testutil.TestError{
		DefaultError: status.Error(codes.Internal, "NodeExpandVolume: block device size did not match requested size after filesystem resize: rpc error: code = Internal desc = block volume at path test size check failed: current 8 GiB < requested 15 GiB"),
		WindowsError: status.Errorf(codes.NotFound, "error reading link for mount D:\\a\\azuredisk-csi-driver\\azuredisk-csi-driver\\pkg\\azuredisk\\target_test. target  err: readlink D:\\a\\azuredisk-csi-driver\\azuredisk-csi-driver\\pkg\\azuredisk\\target_test: The file or directory is not a reparse point."),
	}

	notFoundErrAction := func() ([]byte, []byte, error) {
		return []byte{}, []byte{}, notFoundErr
	}
	findmntAction := func() ([]byte, []byte, error) {
		return []byte("test"), []byte{}, nil
	}
	blkidAction := func() ([]byte, []byte, error) {
		return []byte("DEVICE=test\nTYPE=ext4"), []byte{}, nil
	}
	resize2fsFailedAction := func() ([]byte, []byte, error) {
		return []byte{}, []byte{}, notFoundErr
	}
	resize2fsAction := func() ([]byte, []byte, error) {
		return []byte{}, []byte{}, nil
	}
	blockdevSizeTooSmallAction := func() ([]byte, []byte, error) {
		return []byte(fmt.Sprintf("%d", stdCapacityRange.RequiredBytes/2)), []byte{}, nil
	}
	blockdevAction := func() ([]byte, []byte, error) {
		return []byte(fmt.Sprintf("%d", stdCapacityRange.RequiredBytes)), []byte{}, nil
	}
	// Action that returns 0 bytes (invalid block size)
	blockdevZeroAction := func() ([]byte, []byte, error) {
		return []byte("0"), []byte{}, nil
	}

	tests := []struct {
		desc          string
		req           *csi.NodeExpandVolumeRequest
		expectedErr   testutil.TestError
		skipOnDarwin  bool
		skipOnWindows bool
		outputScripts []testingexec.FakeAction
	}{
		{
			desc: "Volume ID missing",
			req:  &csi.NodeExpandVolumeRequest{},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "Volume ID not provided"),
			},
		},
		{
			desc: "could not find path",
			req: &csi.NodeExpandVolumeRequest{
				CapacityRange: stdCapacityRange,
				VolumePath:    "./test",
				VolumeId:      "test",
			},
			expectedErr: invalidPathErr,
		},
		{
			desc: "volume path not provide",
			req: &csi.NodeExpandVolumeRequest{
				CapacityRange:     stdCapacityRange,
				StagingTargetPath: "test",
				VolumeId:          "test",
			},
			expectedErr: testutil.TestError{
				DefaultError: status.Error(codes.InvalidArgument, "volume path must be provided"),
			},
		},
		{
			desc: "Invalid device path",
			req: &csi.NodeExpandVolumeRequest{
				CapacityRange:     stdCapacityRange,
				VolumePath:        targetTest,
				VolumeId:          "test",
				StagingTargetPath: "",
			},
			expectedErr:   devicePathErr,
			outputScripts: []testingexec.FakeAction{notFoundErrAction},
		},
		{
			desc: "No block size at path",
			req: &csi.NodeExpandVolumeRequest{
				CapacityRange:     stdCapacityRange,
				VolumePath:        targetTest,
				VolumeId:          "test",
				StagingTargetPath: "test",
			},
			expectedErr:   blockSizeErr,
			skipOnDarwin:  true, // ResizeFs not supported on Darwin
			outputScripts: []testingexec.FakeAction{findmntAction, blockdevZeroAction},
		},
		{
			desc: "Resize failure",
			req: &csi.NodeExpandVolumeRequest{
				CapacityRange:     stdCapacityRange,
				VolumePath:        targetTest,
				VolumeId:          "test",
				StagingTargetPath: "test",
			},
			expectedErr:  resizeErr,
			skipOnDarwin: true, // ResizeFs not supported on Darwin
			// First blockdev call succeeds (pre-resize validation), blkid checks filesystem, resize2fs fails, second blockdev for post-resize validation
			outputScripts: []testingexec.FakeAction{findmntAction, blockdevAction, blkidAction, resize2fsFailedAction, blockdevSizeTooSmallAction},
		},
		{
			desc: "Resize too small failure",
			req: &csi.NodeExpandVolumeRequest{
				CapacityRange:     stdCapacityRange,
				VolumePath:        targetTest,
				VolumeId:          "test",
				StagingTargetPath: "test",
			},
			expectedErr:  sizeTooSmallErr,
			skipOnDarwin: true, // ResizeFs not supported on Darwin
			// First blockdev call succeeds (pre-resize validation), resize2fs succeeds, second blockdev shows size too small
			outputScripts: []testingexec.FakeAction{findmntAction, blockdevAction, blkidAction, resize2fsAction, blockdevSizeTooSmallAction},
		},
		{
			desc: "Successfully expanded",
			req: &csi.NodeExpandVolumeRequest{
				CapacityRange:     stdCapacityRange,
				VolumePath:        targetTest,
				VolumeId:          "test",
				StagingTargetPath: "test",
			},
			skipOnWindows: true,
			skipOnDarwin:  true, // ResizeFs not supported on Darwin
			// First blockdev call (pre-resize validation), resize2fs succeeds, second blockdev call (post-resize validation)
			outputScripts: []testingexec.FakeAction{findmntAction, blockdevAction, blkidAction, resize2fsAction, blockdevAction},
		},
		{
			desc: "Block volume expansion",
			req: &csi.NodeExpandVolumeRequest{
				CapacityRange:     stdCapacityRange,
				VolumePath:        blockVolumePath,
				VolumeId:          "test",
				StagingTargetPath: "test",
			},
		},
		{
			desc: "Volume capability access type mismatch",
			req: &csi.NodeExpandVolumeRequest{
				CapacityRange:     stdCapacityRange,
				VolumePath:        targetTest,
				VolumeId:          "test",
				StagingTargetPath: "test",
				VolumeCapability: &csi.VolumeCapability{
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
					AccessType: &csi.VolumeCapability_Block{
						Block: &csi.VolumeCapability_BlockVolume{},
					},
				},
			},
		},
	}

	d.getHostUtil().(*azureutils.FakeHostUtil).SetPathIsDeviceResult(blockVolumePath, true, nil)

	for _, test := range tests {
		if (test.skipOnDarwin && runtime.GOOS == "darwin") || (test.skipOnWindows && runtime.GOOS == "windows") {
			continue
		}
		if runtime.GOOS != "windows" {
			d.setNextCommandOutputScripts(test.outputScripts...)
		}

		_, err := d.NodeExpandVolume(context.Background(), test.req)
		if !testutil.AssertError(&test.expectedErr, err) {
			t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
		}
	}
	err = os.RemoveAll(targetTest)
	assert.NoError(t, err)
	err = os.RemoveAll(blockVolumePath)
	assert.NoError(t, err)
}

func TestGetBlockSizeBytes(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	if runtime.GOOS == "windows" {
		t.Skip("Skipping test on Windows")
	}
	d, _ := NewFakeDriver(cntl)
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
			t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
		}
	}
	//Setup
	_ = makeDir(testTarget)

	err = os.RemoveAll(testTarget)
	assert.NoError(t, err)
}

func TestEnsureBlockTargetFile(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	// sip this test because `util/mount` not supported
	// on darwin
	if runtime.GOOS == "darwin" {
		t.Skip("Skipping tests on darwin")
	}
	testTarget, err := testutil.GetWorkDirPath("test")
	assert.NoError(t, err)
	testPath, err := testutil.GetWorkDirPath(fmt.Sprintf("test%ctest", os.PathSeparator))
	assert.NoError(t, err)
	d, err := NewFakeDriver(cntl)
	assert.NoError(t, err)
	fakeMounter, err := mounter.NewFakeSafeMounter()
	assert.NoError(t, err)
	d.setMounter(fakeMounter)

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
				DefaultError: status.Error(codes.Internal, fmt.Sprintf("could not mount target \"%s\": mkdir %s: not a directory", testTarget, testTarget)),
				WindowsError: status.Error(codes.Internal, fmt.Sprintf("could not remove mount target %#v: remove %s: The system cannot find the path specified.", testPath, testPath)),
			},
		},
	}
	for _, test := range tests {
		err := d.ensureBlockTargetFile(test.req)
		if !testutil.AssertError(&test.expectedErr, err) {
			t.Errorf("desc: %s\n actualErr: (%v), expectedErr: (%v)", test.desc, err, test.expectedErr)
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
	err = makeDir("./azuredisk.go")
	var e *os.PathError
	if !errors.As(err, &e) {
		t.Errorf("Unexpected Error: %v", err)
	}

	// Remove the directory created
	err = os.RemoveAll(targetTest)
	assert.NoError(t, err)
}

func TestGetDevicePathWithLUN(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
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
	cntl := gomock.NewController(t)
	defer cntl.Finish()
	d, _ := NewFakeDriver(cntl)
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

func TestNodePublishVolumeIdempotentMount(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Ordinary filesystem bind mounts are not supported on Windows")
	}

	d, err := NewFakeDriver(gomock.NewController(t))
	require.NoError(t, err)
	d.(*fakeDriver).enableKataMount = false
	safeMounter, err := mounter.NewFakeSafeMounter()
	require.NoError(t, err)
	mounts := mount.NewFakeMounter(nil)
	safeMounter.Interface = mounts
	d.setMounter(safeMounter)
	source := t.TempDir()
	target := filepath.Join(t.TempDir(), "target")
	req := &csi.NodePublishVolumeRequest{
		VolumeId: "vol_1", StagingTargetPath: source, TargetPath: target, Readonly: true,
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: defaultLinuxFsType}},
		},
	}
	_, err = d.NodePublishVolume(context.Background(), req)
	require.NoError(t, err)
	target, err = filepath.EvalSymlinks(target)
	require.NoError(t, err)
	_, err = d.NodePublishVolume(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, mounts.GetLog(), 1, "retry must not mount the ordinary target twice")
	assert.Equal(t, mount.FakeActionMount, mounts.GetLog()[0].Action)
	assert.Equal(t, target, mounts.GetLog()[0].Target)
	require.Len(t, mounts.MountPoints, 1)
	assert.Equal(t, target, mounts.MountPoints[0].Path)
}

func TestNodePublishVolumeKataIdempotent(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Kata filesystem mounts are only supported on Linux")
	}

	cntl := gomock.NewController(t)
	defer cntl.Finish()
	stdVolCap := &csi.VolumeCapability_Mount{
		Mount: &csi.VolumeCapability_MountVolume{
			FsType: defaultLinuxFsType,
		},
	}
	d, _ := NewFakeDriver(cntl)
	d.(*fakeDriver).enableKataMount = true
	d.(*fakeDriver).isKataNode = true
	fakeMounter, err := mounter.NewFakeSafeMounter()
	assert.NoError(t, err)
	d.setMounter(fakeMounter)
	runtimeClassName := "kata"
	d.(*fakeDriver).kubeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "direct-volume-pod", Namespace: "default", UID: "test-pod-uid"},
			Spec:       corev1.PodSpec{RuntimeClassName: &runtimeClassName},
		},
		&nodev1.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name:        runtimeClassName,
				Annotations: map[string]string{kataAnnotationKey: kataAnnotationValue},
			},
			Handler: "kata",
		},
	)

	var calls int
	store := &kataTestDirectVolume{rootPath: t.TempDir()}
	d.(*fakeDriver).kataDirectVolume = &kataStubDirectVolume{
		findMountInfo: store.FindMountInfo,
		addMountInfo: func(target string, mountInfo directvolume.MountInfo) error {
			assert.Equal(t, targetTest, target)
			assert.Equal(t, directvolume.MountInfo{
				VolumeType: kataDirectVolumeType,
				Device:     "/dev/sdd",
				FsType:     defaultLinuxFsType,
				Metadata:   map[string]string{kataVolumeIDKey: "vol_1"},
				Options:    []string{"ro"},
			}, mountInfo)
			calls++
			return store.AddMountInfo(target, mountInfo)
		},
	}

	volumeCap := csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER}
	req := csi.NodePublishVolumeRequest{VolumeCapability: &csi.VolumeCapability{AccessMode: &volumeCap, AccessType: stdVolCap},
		VolumeId:          "vol_1",
		TargetPath:        targetTest,
		StagingTargetPath: sourceTest,
		PublishContext: map[string]string{
			consts.LUN: "/dev/disk/azure/scsi1/lun1",
		},
		VolumeContext: map[string]string{
			podNameField:      "direct-volume-pod",
			podNamespaceField: "default", podUIDField: "test-pod-uid",
		},
		Readonly: true}

	_, err = d.NodePublishVolume(context.Background(), &req)
	assert.NoError(t, err)
	_, err = d.NodePublishVolume(context.Background(), &req)
	assert.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.Len(t, d.(*fakeDriver).kubeClient.(*fake.Clientset).Actions(), 2, "retry must skip discovery")
}

func TestValidateBlockDeviceSize(t *testing.T) {
	cntl := gomock.NewController(t)
	defer cntl.Finish()

	d, _ := NewFakeDriver(cntl)
	fakeMounter, err := mounter.NewFakeSafeMounter()
	assert.NoError(t, err)
	d.setMounter(fakeMounter)

	notFoundErr := errors.New("exit status 1")
	blockdevAction := func() ([]byte, []byte, error) {
		return []byte(fmt.Sprintf("%d", volumehelper.GiBToBytes(20))), []byte{}, nil
	}
	blockdevSmallAction := func() ([]byte, []byte, error) {
		return []byte(fmt.Sprintf("%d", volumehelper.GiBToBytes(5))), []byte{}, nil
	}
	blockdevErrorAction := func() ([]byte, []byte, error) {
		return []byte{}, []byte{}, notFoundErr
	}

	tests := []struct {
		desc          string
		devicePath    string
		requestGiB    int64
		outputScript  testingexec.FakeAction
		expectedErr   error
		expectedBytes int64
		skipOnWindows bool
	}{
		{
			desc:          "Error getting block size",
			devicePath:    "/dev/sda",
			requestGiB:    10,
			outputScript:  blockdevErrorAction,
			expectedErr:   status.Error(codes.Internal, fmt.Sprintf("could not get size of block volume at path %s: %v", "/dev/sda", fmt.Errorf("error when getting size of block volume at path /dev/sda: output: , err: exit status 1"))),
			skipOnWindows: true,
		},
		{
			desc:          "Block size too small",
			devicePath:    "/dev/sdb",
			requestGiB:    10,
			outputScript:  blockdevSmallAction,
			expectedErr:   status.Error(codes.Internal, fmt.Sprintf("block volume at path %s size check failed: current %d GiB < requested %d GiB", "/dev/sdb", 5, 10)),
			skipOnWindows: true,
		},
		{
			desc:          "Successful validation",
			devicePath:    "/dev/sdc",
			requestGiB:    10,
			outputScript:  blockdevAction,
			expectedErr:   nil,
			expectedBytes: volumehelper.GiBToBytes(20),
			skipOnWindows: true,
		},
		{
			desc:          "Exact size match",
			devicePath:    "/dev/sdd",
			requestGiB:    20,
			outputScript:  blockdevAction,
			expectedErr:   nil,
			expectedBytes: volumehelper.GiBToBytes(20),
			skipOnWindows: true,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			if test.skipOnWindows && runtime.GOOS == "windows" {
				t.Skip("Skipping test on Windows")
			}

			d.setNextCommandOutputScripts(test.outputScript)

			actualBytes, err := d.validateBlockDeviceSize(test.devicePath, test.requestGiB)

			if test.expectedErr == nil {
				assert.NoError(t, err)
				assert.Equal(t, test.expectedBytes, actualBytes)
			} else {
				assert.EqualError(t, err, test.expectedErr.Error())
				assert.Equal(t, int64(0), actualBytes)
			}
		})
	}
}
