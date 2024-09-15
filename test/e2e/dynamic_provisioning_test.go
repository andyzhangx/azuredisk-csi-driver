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

package e2e

import (
	"fmt"
	"os"

	"sigs.k8s.io/azuredisk-csi-driver/test/e2e/driver"
	"sigs.k8s.io/azuredisk-csi-driver/test/e2e/testsuites"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	clientset "k8s.io/client-go/kubernetes"
	restclientset "k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/framework"
	admissionapi "k8s.io/pod-security-admission/api"
	consts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
)

var _ = ginkgo.Describe("Dynamic Provisioning", func() {
	t := dynamicProvisioningTestSuite{}

	ginkgo.Context("[single-az]", func() {
		t.defineTests(false)
	})

	ginkgo.Context("[multi-az]", func() {
		t.defineTests(true)
	})
})

type dynamicProvisioningTestSuite struct {
	allowedTopologyValues []string
}

func (t *dynamicProvisioningTestSuite) defineTests(isMultiZone bool) {
	f := framework.NewDefaultFramework("azuredisk")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	var (
		cs          clientset.Interface
		ns          *v1.Namespace
		snapshotrcs restclientset.Interface
		testDriver  driver.PVTestDriver
	)

	ginkgo.BeforeEach(func(ctx ginkgo.SpecContext) {
		checkPodsRestart := testCmd{
			command:  "bash",
			args:     []string{"test/utils/check_driver_pods_restart.sh"},
			startLog: "Check driver pods if restarts ...",
			endLog:   "Check successfully",
		}
		execTestCmd([]testCmd{checkPodsRestart})

		cs = f.ClientSet
		ns = f.Namespace

		var err error
		snapshotrcs, err = restClient(testsuites.SnapshotAPIGroup, testsuites.APIVersionv1)
		if err != nil {
			ginkgo.Fail(fmt.Sprintf("could not get rest clientset: %v", err))
		}

		// Populate allowedTopologyValues from node labels fior the first time
		if isMultiZone && len(t.allowedTopologyValues) == 0 {
			nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			framework.ExpectNoError(err)
			allowedTopologyValuesMap := make(map[string]bool)
			for _, node := range nodes.Items {
				if zone, ok := node.Labels[driver.TopologyKey]; ok {
					allowedTopologyValuesMap[zone] = true
				}
			}
			for k := range allowedTopologyValuesMap {
				t.allowedTopologyValues = append(t.allowedTopologyValues, k)
			}
		}
	})

	testDriver = driver.InitAzureDiskDriver()
	ginkgo.It("should create a volume on demand with mount options [kubernetes.io/azure-disk] [disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		pvcSize := "10Gi"
		if isMultiZone {
			pvcSize = "512Gi"
		}
		pods := []testsuites.PodDetails{
			{
				Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data && grep 'hello world' /mnt/test-1/data"),
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						ClaimSize: pvcSize,
						MountOptions: []string{
							"barrier=1",
							"acl",
						},
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
				IsWindows:    isWindowsCluster,
				WinServerVer: winServerVer,
			},
		}
		test := testsuites.DynamicallyProvisionedCmdVolumeTest{
			CSIDriver: testDriver,
			Pods:      pods,
			StorageClassParameters: map[string]string{
				"skuName": "Standard_LRS",
			},
		}

		if isMultiZone && !isUsingInTreeVolumePlugin && !isCapzTest {
			test.StorageClassParameters = map[string]string{
				"skuName":           "UltraSSD_LRS",
				"cachingmode":       "None",
				"logicalSectorSize": "512",
				"zoned":             "true",
			}
		}
		if isUsingInTreeVolumePlugin {
			// cover case: https://github.com/kubernetes/kubernetes/issues/103433
			test.StorageClassParameters["Kind"] = "managed"
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create a pod with volume mount subpath [disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()

		pods := []testsuites.PodDetails{
			{
				Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data && grep 'hello world' /mnt/test-1/data"),
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						ClaimSize: "10Gi",
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
				IsWindows:    isWindowsCluster,
				WinServerVer: winServerVer,
			},
		}

		scParameters := map[string]string{
			"skuName":                "Standard_LRS",
			"networkAccessPolicy":    "DenyAll",
			"PublicNetworkAccess":    "Enabled",
			"userAgent":              "azuredisk-e2e-test",
			"enableAsyncAttach":      "false",
			"attachDiskInitialDelay": "500",
		}
		test := testsuites.DynamicallyProvisionedVolumeSubpathTester{
			CSIDriver:              testDriver,
			Pods:                   pods,
			StorageClassParameters: scParameters,
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("Should create and attach a volume with basic perfProfile [enableBursting][disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		skipIfOnAzureStackCloud()
		skipIfUsingInTreeVolumePlugin()
		pods := []testsuites.PodDetails{
			{
				Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data && grep 'hello world' /mnt/test-1/data"),
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						FSType:    "ext4",
						ClaimSize: "1Ti",
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
				IsWindows:    isWindowsCluster,
				WinServerVer: winServerVer,
			},
		}
		test := testsuites.DynamicallyProvisionedCmdVolumeTest{
			CSIDriver: testDriver,
			Pods:      pods,
			StorageClassParameters: map[string]string{
				"skuName":     "Premium_LRS",
				"perfProfile": "Basic",
				// enableBursting can only be applied to Premium disk, disk size > 512GB, Ultra & shared disk is not supported.
				"enableBursting":        "true",
				"userAgent":             "azuredisk-e2e-test",
				"enableAsyncAttach":     "false",
				"enablePerformancePlus": "true",
			},
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should receive FailedMount event with invalid mount options [kubernetes.io/azure-disk] [disk.csi.azure.com]", func(ctx ginkgo.SpecContext) {
		skipIfTestingInWindowsCluster()

		pods := []testsuites.PodDetails{
			{
				Cmd: "echo 'hello world' > /mnt/test-1/data && grep 'hello world' /mnt/test-1/data",
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						ClaimSize: "10Gi",
						MountOptions: []string{
							"invalid",
							"mount",
							"options",
						},
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
			},
		}
		test := testsuites.DynamicallyProvisionedInvalidMountOptions{
			CSIDriver:              testDriver,
			Pods:                   pods,
			StorageClassParameters: map[string]string{"skuName": "StandardSSD_LRS"},
		}
		if !isUsingInTreeVolumePlugin && (location == "westus2" || location == "westeurope") {
			test.StorageClassParameters = map[string]string{"skuName": "StandardSSD_ZRS"}
		}
		if isAzureStackCloud {
			test.StorageClassParameters = map[string]string{"skuName": "Standard_LRS"}
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create a raw block volume on demand [kubernetes.io/azure-disk] [disk.csi.azure.com]", func(ctx ginkgo.SpecContext) {
		skipIfTestingInWindowsCluster()

		pods := []testsuites.PodDetails{
			{
				Cmd: "ls /dev | grep e2e-test",
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						ClaimSize:  "10Gi",
						VolumeMode: testsuites.Block,
						VolumeDevice: testsuites.VolumeDeviceDetails{
							NameGenerate: "test-volume-",
							DevicePath:   "/dev/e2e-test",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
			},
		}
		test := testsuites.DynamicallyProvisionedCmdVolumeTest{
			CSIDriver:              testDriver,
			Pods:                   pods,
			StorageClassParameters: map[string]string{"skuName": "Premium_LRS"},
		}
		if !isUsingInTreeVolumePlugin && supportsZRS {
			test.StorageClassParameters = map[string]string{"skuName": "StandardSSD_ZRS"}
		}

		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create a volume in separate resource group and bind it to a pod [kubernetes.io/azure-disk] [disk.csi.azure.com]", func(ctx ginkgo.SpecContext) {
		skipIfTestingInWindowsCluster()
		skipIfUsingInTreeVolumePlugin()

		pod := testsuites.PodDetails{
			Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data && grep 'hello world' /mnt/test-1/data"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					FSType:    "ext4",
					ClaimSize: "10Gi",
					VolumeMount: testsuites.VolumeMountDetails{
						NameGenerate:      "test-volume-",
						MountPathGenerate: "/mnt/test-",
					},
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
		}

		test := testsuites.DynamicallyProvisionedExternalRgVolumeTest{
			CSIDriver:              testDriver,
			Pod:                    pod,
			StorageClassParameters: map[string]string{"skuName": "Premium_LRS"},
			SeparateResourceGroups: false,
		}
		if !isUsingInTreeVolumePlugin && supportsZRS {
			test.StorageClassParameters = map[string]string{"skuName": "StandardSSD_ZRS"}
		}

		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create multiple volumes, each in separate resource groups and attach them to a single pod [kubernetes.io/azure-disk] [disk.csi.azure.com]", func(ctx ginkgo.SpecContext) {
		skipIfTestingInWindowsCluster()
		skipIfUsingInTreeVolumePlugin()

		pod := testsuites.PodDetails{
			Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data && echo 'hello world' > /mnt/test-2/data && grep 'hello world' /mnt/test-1/data && grep 'hello world' /mnt/test-2/data"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					FSType:    "ext4",
					ClaimSize: "10Gi",
					VolumeMount: testsuites.VolumeMountDetails{
						NameGenerate:      "test-volume-",
						MountPathGenerate: "/mnt/test-",
					},
					VolumeAccessMode: v1.ReadWriteOnce,
				},
				{
					FSType:    "ext4",
					ClaimSize: "10Gi",
					VolumeMount: testsuites.VolumeMountDetails{
						NameGenerate:      "test-volume-",
						MountPathGenerate: "/mnt/test-",
					},
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
		}

		test := testsuites.DynamicallyProvisionedExternalRgVolumeTest{
			CSIDriver:              testDriver,
			Pod:                    pod,
			StorageClassParameters: map[string]string{"skuName": "Premium_LRS"},
			SeparateResourceGroups: true,
		}
		if !isUsingInTreeVolumePlugin && supportsZRS {
			test.StorageClassParameters = map[string]string{"skuName": "StandardSSD_ZRS"}
		}

		test.Run(ctx, cs, ns)
	})

	// Track issue https://github.com/kubernetes/kubernetes/issues/70505
	ginkgo.It("should create a volume on demand and mount it as readOnly in a pod [kubernetes.io/azure-disk] [disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		pods := []testsuites.PodDetails{
			{
				Cmd: convertToPowershellorCmdCommandIfNecessary("touch /mnt/test-1/data"),
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						FSType:    "ext4",
						ClaimSize: "10Gi",
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
							ReadOnly:          true,
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
				IsWindows:    isWindowsCluster,
				WinServerVer: winServerVer,
			},
		}
		test := testsuites.DynamicallyProvisionedReadOnlyVolumeTest{
			CSIDriver:              testDriver,
			Pods:                   pods,
			StorageClassParameters: map[string]string{"skuName": "StandardSSD_LRS"},
		}
		if !isUsingInTreeVolumePlugin && (location == "westus2" || location == "westeurope") {
			test.StorageClassParameters = map[string]string{"skuName": "Premium_ZRS"}
		}
		if isAzureStackCloud {
			test.StorageClassParameters = map[string]string{"skuName": "Standard_LRS"}
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create multiple PV objects, bind to PVCs and attach all to different pods on the same node [kubernetes.io/azure-disk] [disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		pods := []testsuites.PodDetails{
			{
				Cmd: convertToPowershellorCmdCommandIfNecessary("while true; do echo $(date -u) >> /mnt/test-1/data; sleep 3600; done"),
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						FSType:    "ext3",
						ClaimSize: "10Gi",
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
				IsWindows:    isWindowsCluster,
				WinServerVer: winServerVer,
			},
			{
				Cmd: convertToPowershellorCmdCommandIfNecessary("while true; do echo $(date -u) >> /mnt/test-1/data; sleep 3600; done"),
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						FSType:    "ext4",
						ClaimSize: "10Gi",
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
				IsWindows:    isWindowsCluster,
				WinServerVer: winServerVer,
			},
			{
				Cmd: convertToPowershellorCmdCommandIfNecessary("while true; do echo $(date -u) >> /mnt/test-1/data; sleep 3600; done"),
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						FSType:    "xfs",
						ClaimSize: "10Gi",
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
				IsWindows:    isWindowsCluster,
				WinServerVer: winServerVer,
			},
		}
		test := testsuites.DynamicallyProvisionedCollocatedPodTest{
			CSIDriver:              testDriver,
			Pods:                   pods,
			ColocatePods:           true,
			StorageClassParameters: map[string]string{"skuName": "Premium_LRS"},
		}
		if !isUsingInTreeVolumePlugin && supportsZRS {
			test.StorageClassParameters = map[string]string{"skuName": "StandardSSD_ZRS"}
		}

		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create a deployment object, write and read to it, delete the pod and write and read to it again [kubernetes.io/azure-disk] [disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		pod := testsuites.PodDetails{
			Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' >> /mnt/test-1/data && while true; do sleep 3600; done"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					FSType:    "ext3",
					ClaimSize: "10Gi",
					VolumeMount: testsuites.VolumeMountDetails{
						NameGenerate:      "test-volume-",
						MountPathGenerate: "/mnt/test-",
					},
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			UseCMD:       false,
		}

		podCheckCmd := []string{"cat", "/mnt/test-1/data"}
		expectedString := "hello world\n"
		if isWindowsCluster {
			podCheckCmd = []string{"cmd", "/c", "type C:\\mnt\\test-1\\data.txt"}
			expectedString = "hello world\r\n"
		}
		test := testsuites.DynamicallyProvisionedDeletePodTest{
			CSIDriver: testDriver,
			Pod:       pod,
			PodCheck: &testsuites.PodExecCheck{
				Cmd:            podCheckCmd,
				ExpectedString: expectedString, // pod will be restarted so expect to see 2 instances of string
			},
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It(fmt.Sprintf("should delete PV with reclaimPolicy %q [kubernetes.io/azure-disk] [disk.csi.azure.com] [Windows]", v1.PersistentVolumeReclaimDelete), func(ctx ginkgo.SpecContext) {
		reclaimPolicy := v1.PersistentVolumeReclaimDelete
		volumes := t.normalizeVolumes([]testsuites.VolumeDetails{
			{
				FSType:           "ext4",
				ClaimSize:        "10Gi",
				ReclaimPolicy:    &reclaimPolicy,
				VolumeAccessMode: v1.ReadWriteOnce,
			},
		}, isMultiZone)
		test := testsuites.DynamicallyProvisionedReclaimPolicyTest{
			CSIDriver: testDriver,
			Volumes:   volumes,
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It(fmt.Sprintf("should retain PV with reclaimPolicy %q [disk.csi.azure.com]", v1.PersistentVolumeReclaimRetain), func(ctx ginkgo.SpecContext) {
		// This tests uses the CSI driver to delete the PV.
		// TODO: Go via the k8s interfaces and also make it more reliable for in-tree and then
		//       test can be enabled.
		skipIfUsingInTreeVolumePlugin()

		reclaimPolicy := v1.PersistentVolumeReclaimRetain
		volumes := t.normalizeVolumes([]testsuites.VolumeDetails{
			{
				FSType:           "ext4",
				ClaimSize:        "10Gi",
				ReclaimPolicy:    &reclaimPolicy,
				VolumeAccessMode: v1.ReadWriteOnce,
			},
		}, isMultiZone)
		test := testsuites.DynamicallyProvisionedReclaimPolicyTest{
			CSIDriver: testDriver,
			Volumes:   volumes,
			Azuredisk: azurediskDriver,
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should clone a volume from an existing volume and read from it [disk.csi.azure.com]", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()
		if isWindowsCluster && !isWindowsHPCDeployment {
			ginkgo.Skip("test case not supported by Windows clusters with non host process deployment drivers")
		}
		pod := testsuites.PodDetails{
			Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					FSType:    "xfs",
					ClaimSize: "10Gi",
					VolumeMount: testsuites.VolumeMountDetails{
						NameGenerate:      "test-volume-",
						MountPathGenerate: "/mnt/test-",
					},
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
		}
		podWithClonedVolume := testsuites.PodDetails{
			Cmd:          convertToPowershellorCmdCommandIfNecessary("grep 'hello world' /mnt/test-1/data"),
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
		}
		test := testsuites.DynamicallyProvisionedVolumeCloningTest{
			CSIDriver:           testDriver,
			Pod:                 pod,
			PodWithClonedVolume: podWithClonedVolume,
			StorageClassParameters: map[string]string{
				"skuName": "Standard_LRS",
				"fsType":  "xfs",
			},
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should clone a volume of larger size than the source volume and make sure the filesystem is appropriately adjusted [disk.csi.azure.com]", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()
		if isWindowsCluster && !isWindowsHPCDeployment {
			ginkgo.Skip("test case not supported by Windows clusters with non host process deployment drivers")
		}

		pod := testsuites.PodDetails{
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					FSType:    "xfs",
					ClaimSize: "10Gi",
					VolumeMount: testsuites.VolumeMountDetails{
						NameGenerate:      "test-volume-",
						MountPathGenerate: "/mnt/test-",
					},
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
		}
		clonedVolumeSize := "20Gi"

		podWithClonedVolume := testsuites.PodDetails{
			Cmd:          convertToPowershellorCmdCommandIfNecessary("df -h | grep /mnt/test- | awk '{print $2}' | grep -E '19|20'"),
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
		}

		test := testsuites.DynamicallyProvisionedVolumeCloningTest{
			CSIDriver:           testDriver,
			Pod:                 pod,
			PodWithClonedVolume: podWithClonedVolume,
			ClonedVolumeSize:    clonedVolumeSize,
			StorageClassParameters: map[string]string{
				"skuName": "Standard_LRS",
				"fsType":  "xfs",
			},
		}
		if !isUsingInTreeVolumePlugin && supportsZRS {
			test.StorageClassParameters = map[string]string{
				"skuName":             "StandardSSD_ZRS",
				"fsType":              "xfs",
				"networkAccessPolicy": "DenyAll",
				"PublicNetworkAccess": "Disabled",
			}
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create multiple PV objects, bind to PVCs and attach all to a single pod [kubernetes.io/azure-disk] [disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		pods := []testsuites.PodDetails{
			{
				Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data && echo 'hello world' > /mnt/test-2/data && echo 'hello world' > /mnt/test-3/data && grep 'hello world' /mnt/test-1/data && grep 'hello world' /mnt/test-2/data && grep 'hello world' /mnt/test-3/data"),
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						FSType:    "ext3",
						ClaimSize: "10Gi",
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
					{
						FSType:    "ext4",
						ClaimSize: "10Gi",
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
					{
						FSType:    "xfs",
						ClaimSize: "10Gi",
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
				IsWindows:    isWindowsCluster,
				WinServerVer: winServerVer,
			},
		}
		test := testsuites.DynamicallyProvisionedCmdVolumeTest{
			CSIDriver:              testDriver,
			Pods:                   pods,
			StorageClassParameters: map[string]string{"skuName": "StandardSSD_LRS"},
		}
		if isAzureStackCloud {
			test.StorageClassParameters = map[string]string{"skuName": "Standard_LRS"}
		}
		if !isUsingInTreeVolumePlugin && supportsZRS {
			test.StorageClassParameters = map[string]string{"skuName": "StandardSSD_ZRS"}
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create a raw block volume and a filesystem volume on demand and bind to the same pod [kubernetes.io/azure-disk] [disk.csi.azure.com]", func(ctx ginkgo.SpecContext) {
		skipIfTestingInWindowsCluster()

		pods := []testsuites.PodDetails{
			{
				Cmd: "dd if=/dev/zero of=/dev/xvda bs=1024k count=100 && echo 'hello world' > /mnt/test-1/data && grep 'hello world' /mnt/test-1/data",
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						FSType:    "ext4",
						ClaimSize: "10Gi",
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
					{
						FSType:       "ext4",
						MountOptions: []string{"rw"},
						ClaimSize:    "10Gi",
						VolumeMode:   testsuites.Block,
						VolumeDevice: testsuites.VolumeDeviceDetails{
							NameGenerate: "test-block-volume-",
							DevicePath:   "/dev/xvda",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
			},
		}
		test := testsuites.DynamicallyProvisionedCmdVolumeTest{
			CSIDriver:              testDriver,
			Pods:                   pods,
			StorageClassParameters: map[string]string{"skuName": "Premium_LRS"},
		}
		if !isUsingInTreeVolumePlugin && supportsZRS {
			test.StorageClassParameters = map[string]string{"skuName": "StandardSSD_ZRS"}
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create a pod, write and read to it, take a volume snapshot, and create another pod from the snapshot [disk.csi.azure.com]", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()
		skipIfTestingInWindowsCluster()

		pod := testsuites.PodDetails{
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			Cmd:          convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					FSType:    getFSType(isWindowsCluster),
					ClaimSize: "10Gi",
					VolumeMount: testsuites.VolumeMountDetails{
						NameGenerate:      "test-volume-",
						MountPathGenerate: "/mnt/test-",
					},
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
		}
		podWithSnapshot := testsuites.PodDetails{
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			Cmd:          convertToPowershellorCmdCommandIfNecessary("grep 'hello world' /mnt/test-1/data"),
		}
		test := testsuites.DynamicallyProvisionedVolumeSnapshotTest{
			CSIDriver:              testDriver,
			Pod:                    pod,
			ShouldOverwrite:        false,
			IsWindowsHPCDeployment: isWindowsHPCDeployment,
			PodWithSnapshot:        podWithSnapshot,
			StorageClassParameters: map[string]string{"skuName": "StandardSSD_LRS"},
			SnapshotStorageClassParameters: map[string]string{
				"incremental": "false", "dataAccessAuthMode": "AzureActiveDirectory",
			},
		}
		if isAzureStackCloud {
			test.StorageClassParameters = map[string]string{"skuName": "Standard_LRS"}
		}
		if !isUsingInTreeVolumePlugin && supportsZRS {
			test.StorageClassParameters = map[string]string{"skuName": "StandardSSD_ZRS"}
		}
		test.Run(ctx, cs, snapshotrcs, ns)
	})

	ginkgo.It("should create a pod, write to its pv, take a volume snapshot, overwrite data in original pv, create another pod from the snapshot, and read unaltered original data from original pv[disk.csi.azure.com]", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()
		skipIfTestingInWindowsCluster()

		pod := testsuites.PodDetails{
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			Cmd:          convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					FSType:    getFSType(isWindowsCluster),
					ClaimSize: "10Gi",
					VolumeMount: testsuites.VolumeMountDetails{
						NameGenerate:      "test-volume-",
						MountPathGenerate: "/mnt/test-",
					},
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
		}

		podOverwrite := testsuites.PodDetails{
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			Cmd:          convertToPowershellorCmdCommandIfNecessary("echo 'overwrite' > /mnt/test-1/data; sleep 3600"),
		}

		podWithSnapshot := testsuites.PodDetails{
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			Cmd:          convertToPowershellorCmdCommandIfNecessary("grep 'hello world' /mnt/test-1/data"),
		}

		test := testsuites.DynamicallyProvisionedVolumeSnapshotTest{
			CSIDriver:              testDriver,
			Pod:                    pod,
			ShouldOverwrite:        true,
			PodOverwrite:           podOverwrite,
			PodWithSnapshot:        podWithSnapshot,
			StorageClassParameters: map[string]string{"skuName": "StandardSSD_LRS"},
			SnapshotStorageClassParameters: map[string]string{
				"incremental": "true", "dataAccessAuthMode": "None",
			},
		}
		if isAzureStackCloud {
			test.StorageClassParameters = map[string]string{"skuName": "Standard_LRS"}
		}
		test.Run(ctx, cs, snapshotrcs, ns)
	})

	//nolint:dupl
	ginkgo.It("should create a pod with small storage size, take a volume snapshot cross region, and restore disk in another region [disk.csi.azure.com]", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()
		skipIfTestingInWindowsCluster()

		pod := testsuites.PodDetails{
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			Cmd:          convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					FSType:    getFSType(isWindowsCluster),
					ClaimSize: "10Gi",
					VolumeMount: testsuites.VolumeMountDetails{
						NameGenerate:      "test-volume-",
						MountPathGenerate: "/mnt/test-",
					},
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
		}
		podWithSnapshot := testsuites.PodDetails{
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			Cmd:          convertToPowershellorCmdCommandIfNecessary("grep 'hello world' /mnt/test-1/data"),
		}
		test := testsuites.DynamicallyProvisionedVolumeSnapshotCrossRegionTest{
			CSIDriver:              testDriver,
			Pod:                    pod,
			PodWithSnapshot:        podWithSnapshot,
			StorageClassParameters: map[string]string{"skuName": "StandardSSD_LRS"},
			SnapshotStorageClassParameters: map[string]string{
				"incremental": "true", "dataAccessAuthMode": "AzureActiveDirectory", "location": "westus2",
			},
		}
		if location == "westus2" {
			test.SnapshotStorageClassParameters["location"] = "westeurope"
		}
		if isAzureStackCloud {
			test.StorageClassParameters = map[string]string{"skuName": "Standard_LRS"}
		}
		test.Run(ctx, cs, snapshotrcs, ns)
	})

	//nolint:dupl
	ginkgo.It("should create a pod with large storage size, take a volume snapshot cross region, and restore disk in another region [disk.csi.azure.com]", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()
		skipIfTestingInWindowsCluster()

		pod := testsuites.PodDetails{
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			Cmd:          convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					FSType:    getFSType(isWindowsCluster),
					ClaimSize: "100Gi",
					VolumeMount: testsuites.VolumeMountDetails{
						NameGenerate:      "test-volume-",
						MountPathGenerate: "/mnt/test-",
					},
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
		}
		podWithSnapshot := testsuites.PodDetails{
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			Cmd:          convertToPowershellorCmdCommandIfNecessary("grep 'hello world' /mnt/test-1/data"),
		}
		test := testsuites.DynamicallyProvisionedVolumeSnapshotCrossRegionTest{
			CSIDriver:              testDriver,
			Pod:                    pod,
			PodWithSnapshot:        podWithSnapshot,
			StorageClassParameters: map[string]string{"skuName": "StandardSSD_LRS"},
			SnapshotStorageClassParameters: map[string]string{
				"incremental": "true", "dataAccessAuthMode": "AzureActiveDirectory", "location": "westus2",
			},
		}
		if location == "westus2" {
			test.SnapshotStorageClassParameters["location"] = "westeurope"
		}
		if isAzureStackCloud {
			test.StorageClassParameters = map[string]string{"skuName": "Standard_LRS"}
		}
		test.Run(ctx, cs, snapshotrcs, ns)
	})

	ginkgo.It("should create a pod with multiple volumes [kubernetes.io/azure-disk] [disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		volumes := []testsuites.VolumeDetails{}
		for i := 1; i <= 3; i++ {
			volume := testsuites.VolumeDetails{
				ClaimSize: "10Gi",
				VolumeMount: testsuites.VolumeMountDetails{
					NameGenerate:      "test-volume-",
					MountPathGenerate: "/mnt/test-",
				},
				VolumeAccessMode: v1.ReadWriteOnce,
			}
			volumes = append(volumes, volume)
		}

		pods := []testsuites.PodDetails{
			{
				Cmd:          convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data && grep 'hello world' /mnt/test-1/data"),
				Volumes:      t.normalizeVolumes(volumes, isMultiZone),
				IsWindows:    isWindowsCluster,
				WinServerVer: winServerVer,
			},
		}
		test := testsuites.DynamicallyProvisionedPodWithMultiplePVsTest{
			CSIDriver: testDriver,
			Pods:      pods,
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create a volume on demand and resize it [disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()
		volume := testsuites.VolumeDetails{
			ClaimSize: "10Gi",
			VolumeMount: testsuites.VolumeMountDetails{
				NameGenerate:      "test-volume-",
				MountPathGenerate: "/mnt/test-",
			},
			VolumeAccessMode: v1.ReadWriteOnce,
		}
		pod := testsuites.PodDetails{
			Cmd: convertToPowershellorCmdCommandIfNecessary("while true; do echo $(date -u) >> /mnt/test-1/data; sleep 3600; done"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					ClaimSize: volume.ClaimSize,
					MountOptions: []string{
						"barrier=1",
						"acl",
					},
					VolumeMount:      volume.VolumeMount,
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			UseCMD:       false,
		}

		test := testsuites.DynamicallyProvisionedResizeVolumeTest{
			CSIDriver:              testDriver,
			Volume:                 volume,
			Pod:                    pod,
			ResizeOffline:          true,
			StorageClassParameters: map[string]string{"skuName": "Standard_LRS"},
		}
		if !isUsingInTreeVolumePlugin && supportsZRS {
			test.StorageClassParameters = map[string]string{
				"skuName": "StandardSSD_ZRS",
				"fsType":  "btrfs",
			}
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create a volume on demand and dynamically resize it without detaching [disk.csi.azure.com] ", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()

		//Subscription must be registered for LiveResize
		volume := testsuites.VolumeDetails{
			ClaimSize: "10Gi",
			VolumeMount: testsuites.VolumeMountDetails{
				NameGenerate:      "test-volume-",
				MountPathGenerate: "/mnt/test-",
			},
			VolumeAccessMode: v1.ReadWriteOnce,
		}
		pod := testsuites.PodDetails{
			Cmd: convertToPowershellorCmdCommandIfNecessary("while true; do echo $(date -u) >> /mnt/test-1/data; sleep 3600; done"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					ClaimSize: volume.ClaimSize,
					MountOptions: []string{
						"barrier=1",
						"acl",
					},
					VolumeMount:      volume.VolumeMount,
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			UseCMD:       false,
		}

		test := testsuites.DynamicallyProvisionedResizeVolumeTest{
			CSIDriver:              testDriver,
			Volume:                 volume,
			Pod:                    pod,
			ResizeOffline:          false,
			StorageClassParameters: map[string]string{"skuName": "Standard_LRS"},
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create a block volume on demand and dynamically resize it without detaching [disk.csi.azure.com] ", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()

		//Subscription must be registered for LiveResize
		volume := testsuites.VolumeDetails{
			ClaimSize: "10Gi",
			VolumeMount: testsuites.VolumeMountDetails{
				NameGenerate:      "test-volume-",
				MountPathGenerate: "/mnt/test-",
			},
			VolumeAccessMode: v1.ReadWriteOnce,
			VolumeMode:       testsuites.Block,
		}
		pod := testsuites.PodDetails{
			Cmd: convertToPowershellorCmdCommandIfNecessary("while true; do echo $(date -u) >> /mnt/test-1/data; sleep 3600; done"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					ClaimSize: volume.ClaimSize,
					MountOptions: []string{
						"barrier=1",
						"acl",
					},
					VolumeMount:      volume.VolumeMount,
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			UseCMD:       false,
		}

		test := testsuites.DynamicallyProvisionedResizeVolumeTest{
			CSIDriver:              testDriver,
			Volume:                 volume,
			Pod:                    pod,
			ResizeOffline:          false,
			StorageClassParameters: map[string]string{"skuName": "Standard_LRS"},
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create a volume azuredisk with tag [disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()
		skipIfTestingInWindowsCluster()

		pods := []testsuites.PodDetails{
			{
				Cmd: convertToPowershellorCmdCommandIfNecessary("while true; do echo $(date -u) >> /mnt/test-1/data; sleep 3600; done"),
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						ClaimSize: "10Gi",
						MountOptions: []string{
							"barrier=1",
							"acl",
						},
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
				IsWindows:    isWindowsCluster,
				WinServerVer: winServerVer,
			},
		}
		tags := "disk=test"
		test := testsuites.DynamicallyProvisionedAzureDiskWithTag{
			CSIDriver:              testDriver,
			Pods:                   pods,
			StorageClassParameters: map[string]string{"skuName": "Standard_LRS", "tags": tags},
			Tags:                   tags,
		}
		if !isUsingInTreeVolumePlugin && supportsZRS {
			test.StorageClassParameters = map[string]string{"skuName": "StandardSSD_ZRS", "tags": tags}
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should detach disk after pod deleted [disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		pods := []testsuites.PodDetails{
			{
				Cmd: convertToPowershellorCmdCommandIfNecessary("while true; do echo $(date -u) >> /mnt/test-1/data; sleep 3600; done"),
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						ClaimSize: "10Gi",
						MountOptions: []string{
							"barrier=1",
							"acl",
						},
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
				IsWindows:    isWindowsCluster,
				WinServerVer: winServerVer,
			},
		}
		test := testsuites.DynamicallyProvisionedAzureDiskDetach{
			CSIDriver:              testDriver,
			Pods:                   pods,
			StorageClassParameters: map[string]string{"skuName": "Standard_LRS"},
		}
		if !isUsingInTreeVolumePlugin && supportsZRS {
			test.StorageClassParameters = map[string]string{"skuName": "StandardSSD_ZRS"}
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should create a statefulset object, write and read to it, delete the pod and write and read to it again [kubernetes.io/azure-disk] [disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		pod := testsuites.PodDetails{
			Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' >> /mnt/test-1/data && while true; do sleep 3600; done"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					FSType:    "ext3",
					ClaimSize: "10Gi",
					VolumeMount: testsuites.VolumeMountDetails{
						NameGenerate:      "pvc",
						MountPathGenerate: "/mnt/test-",
					},
					VolumeAccessMode: v1.ReadWriteOnce,
				},
			}, isMultiZone),
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			UseCMD:       false,
		}

		podCheckCmd := []string{"cat", "/mnt/test-1/data"}
		expectedString := "hello world\n"
		if isWindowsCluster {
			podCheckCmd = []string{"cmd", "/c", "type C:\\mnt\\test-1\\data.txt"}
			expectedString = "hello world\r\n"
		}
		test := testsuites.DynamicallyProvisionedStatefulSetTest{
			CSIDriver: testDriver,
			Pod:       pod,
			PodCheck: &testsuites.PodExecCheck{
				Cmd:            podCheckCmd,
				ExpectedString: expectedString, // pod will be restarted so expect to see 2 instances of string
			},
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("Should test pod failover with cordoning a node", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()
		skipIfTestingInWindowsCluster()
		if isMultiZone {
			ginkgo.Skip("test case does not apply to multi az case")
		}

		volume := testsuites.VolumeDetails{
			ClaimSize: "10Gi",
			VolumeMount: testsuites.VolumeMountDetails{
				NameGenerate:      "test-volume-",
				MountPathGenerate: "/mnt/test-",
			},
			VolumeAccessMode: v1.ReadWriteOnce,
		}
		pod := testsuites.PodDetails{
			Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' >> /mnt/test-1/data && while true; do sleep 3600; done"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					ClaimSize: volume.ClaimSize,
					MountOptions: []string{
						"barrier=1",
						"acl",
					},
					VolumeMount:      volume.VolumeMount,
					VolumeAccessMode: volume.VolumeAccessMode,
				},
			}, false),
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			UseCMD:       false,
		}
		podCheckCmd := []string{"cat", "/mnt/test-1/data"}
		expectedString := "hello world\n"
		if isWindowsCluster {
			podCheckCmd = []string{"cmd", "/c", "type C:\\mnt\\test-1\\data.txt"}
			expectedString = "hello world\r\n"
		}

		storageClassParameters := map[string]string{"skuName": "StandardSSD_LRS"}

		test := testsuites.PodFailover{
			CSIDriver: testDriver,
			Pod:       pod,
			Volume:    volume,
			PodCheck: &testsuites.PodExecCheck{
				Cmd:            podCheckCmd,
				ExpectedString: expectedString, // pod will be restarted so expect to see 2 instances of string
			},
			StorageClassParameters: storageClassParameters,
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("Should test pod failover with cordoning a node using ZRS", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()
		skipIfNotZRSSupported()
		skipIfTestingInWindowsCluster()

		volume := testsuites.VolumeDetails{
			ClaimSize: "10Gi",
			VolumeMount: testsuites.VolumeMountDetails{
				NameGenerate:      "test-volume-",
				MountPathGenerate: "/mnt/test-",
			},
			VolumeAccessMode: v1.ReadWriteOnce,
		}
		pod := testsuites.PodDetails{
			Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' >> /mnt/test-1/data && while true; do sleep 3600; done"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					ClaimSize: volume.ClaimSize,
					MountOptions: []string{
						"barrier=1",
						"acl",
					},
					VolumeMount:      volume.VolumeMount,
					VolumeAccessMode: volume.VolumeAccessMode,
				},
			}, false),
			IsWindows:    isWindowsCluster,
			WinServerVer: winServerVer,
			UseCMD:       false,
		}
		podCheckCmd := []string{"cat", "/mnt/test-1/data"}
		expectedString := "hello world\n"
		if isWindowsCluster {
			podCheckCmd = []string{"cmd", "/c", "type C:\\mnt\\test-1\\data.txt"}
			expectedString = "hello world\r\n"
		}

		storageClassParameters := map[string]string{"skuName": "StandardSSD_ZRS"}

		test := testsuites.PodFailover{
			CSIDriver: testDriver,
			Pod:       pod,
			Volume:    volume,
			PodCheck: &testsuites.PodExecCheck{
				Cmd:            podCheckCmd,
				ExpectedString: expectedString, // pod will be restarted so expect to see 2 instances of string
			},
			StorageClassParameters: storageClassParameters,
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should succeed when attaching a shared block volume to multiple pods [disk.csi.azure.com][shared disk]", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()
		skipIfOnAzureStackCloud()
		skipIfTestingInWindowsCluster()
		if isMultiZone {
			skipIfNotZRSSupported()
			if isCapzTest {
				ginkgo.Skip("skip shared disk multi zone test on capz cluster")
			}
		}

		pod := testsuites.PodDetails{
			Cmd: convertToPowershellorCmdCommandIfNecessary("while true; do sleep 5; done"),
			Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
				{
					ClaimSize: "10Gi",
					VolumeMount: testsuites.VolumeMountDetails{
						NameGenerate:      "test-shared-volume-",
						MountPathGenerate: "/dev/shared-",
					},
					VolumeMode:       testsuites.Block,
					VolumeAccessMode: v1.ReadWriteMany,
				},
			}, isMultiZone),
			UseCMD:          false,
			IsWindows:       isWindowsCluster,
			WinServerVer:    winServerVer,
			UseAntiAffinity: isMultiZone,
			ReplicaCount:    2,
		}

		storageClassParameters := map[string]string{
			"skuname":     "StandardSSD_LRS",
			"maxshares":   "2",
			"cachingmode": "None",
		}
		if supportsZRS {
			storageClassParameters["skuname"] = "StandardSSD_ZRS"
		}

		podCheck := &testsuites.PodExecCheck{
			ExpectedString: "VOLUME ATTACHED",
		}
		if !isWindowsCluster {
			podCheck.Cmd = []string{
				"sh",
				"-c",
				"(stat /dev/shared-1 > /dev/null) && echo \"VOLUME ATTACHED\"",
			}
		} else {
			podCheck.Cmd = []string{
				"powershell",
				"-NoLogo",
				"-Command",
				"if (Test-Path c:\\dev\\shared-1) { \"VOLUME ATTACHED\" | Out-Host }",
			}
		}

		test := testsuites.DynamicallyProvisionedSharedDiskTester{
			CSIDriver:              testDriver,
			Pod:                    pod,
			PodCheck:               podCheck,
			StorageClassParameters: storageClassParameters,
		}
		test.Run(ctx, cs, ns)
	})

	ginkgo.It("should succeed with advanced perfProfile [disk.csi.azure.com] [Windows]", func(ctx ginkgo.SpecContext) {
		skipIfUsingInTreeVolumePlugin()
		skipIfOnAzureStackCloud()
		skipIfTestingInWindowsCluster()
		pods := []testsuites.PodDetails{
			{
				Cmd: convertToPowershellorCmdCommandIfNecessary("echo 'hello world' > /mnt/test-1/data && grep 'hello world' /mnt/test-1/data"),
				Volumes: t.normalizeVolumes([]testsuites.VolumeDetails{
					{
						ClaimSize: "10Gi",
						MountOptions: []string{
							"barrier=1",
							"acl",
						},
						VolumeMount: testsuites.VolumeMountDetails{
							NameGenerate:      "test-volume-",
							MountPathGenerate: "/mnt/test-",
						},
						VolumeAccessMode: v1.ReadWriteOnce,
					},
				}, isMultiZone),
				IsWindows:    isWindowsCluster,
				WinServerVer: winServerVer,
			},
		}
		test := testsuites.DynamicallyProvisionedCmdVolumeTest{
			CSIDriver: testDriver,
			Pods:      pods,
			StorageClassParameters: map[string]string{
				"skuname":                             "StandardSSD_LRS",
				"perfProfile":                         "advanced",
				"device-setting/queue/max_sectors_kb": "211",
				"device-setting/queue/scheduler":      "none",
				"device-setting/device/queue_depth":   "17",
				"device-setting/queue/nr_requests":    "44",
				"device-setting/queue/read_ahead_kb":  "256",
				"device-setting/queue/wbt_lat_usec":   "0",
				"device-setting/queue/rotational":     "0",
			},
		}

		test.Run(ctx, cs, ns)
	})
}

// Normalize volumes by adding allowed topology values and WaitForFirstConsumer binding mode if we are testing in a multi-az cluster
func (t *dynamicProvisioningTestSuite) normalizeVolumes(volumes []testsuites.VolumeDetails, isMultiZone bool) []testsuites.VolumeDetails {
	for i := range volumes {
		volumes[i] = t.normalizeVolume(volumes[i], isMultiZone)
	}
	return volumes
}

func (t *dynamicProvisioningTestSuite) normalizeVolume(volume testsuites.VolumeDetails, isMultiZone bool) testsuites.VolumeDetails {
	driverName := os.Getenv(driver.AzureDriverNameVar)
	switch driverName {
	case "kubernetes.io/azure-disk":
		volumeBindingMode := storagev1.VolumeBindingWaitForFirstConsumer
		volume.VolumeBindingMode = &volumeBindingMode
	case "", consts.DefaultDriverName:
		if !isMultiZone {
			return volume
		}
		volume.AllowedTopologyValues = t.allowedTopologyValues
		volumeBindingMode := storagev1.VolumeBindingWaitForFirstConsumer
		volume.VolumeBindingMode = &volumeBindingMode
	}

	return volume
}

func restClient(group string, version string) (restclientset.Interface, error) {
	config, err := framework.LoadConfig()
	if err != nil {
		ginkgo.Fail(fmt.Sprintf("could not load config: %v", err))
	}
	gv := schema.GroupVersion{Group: group, Version: version}
	config.GroupVersion = &gv
	config.APIPath = "/apis"
	config.NegotiatedSerializer = serializer.WithoutConversionCodecFactory{CodecFactory: serializer.NewCodecFactory(runtime.NewScheme())}
	return restclientset.RESTClientFor(config)
}
