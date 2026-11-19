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

package testsuites

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapshotclientset "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	apps "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	restclientset "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/deployment"
	e2eevents "k8s.io/kubernetes/test/e2e/framework/events"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epv "k8s.io/kubernetes/test/e2e/framework/pv"
	testutil "k8s.io/kubernetes/test/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/azuredisk-csi-driver/pkg/azuredisk"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
	"sigs.k8s.io/azuredisk-csi-driver/test/e2e/driver"
)

const (
	// Some pods can take much longer to get ready due to volume attach/detach latency.
	slowPodStartTimeout = 15 * time.Minute
	// Description that will printed during tests
	failedConditionDescription = "Error status code"

	poll                 = 2 * time.Second
	pollLongTimeout      = 5 * time.Minute
	pollTimeout          = 10 * time.Minute
	pollForStringTimeout = 1 * time.Minute

	testLabelKey   = "test-label-key"
	testLabelValue = "test-label-value"
	HostNameLabel  = "kubernetes.io/hostname"
)

var (
	TestLabel = map[string]string{
		testLabelKey: testLabelValue,
	}

	TestPodAntiAffinity = v1.Affinity{
		PodAntiAffinity: &v1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{MatchLabels: TestLabel},
					TopologyKey:   HostNameLabel,
				}},
		},
	}
)

type TestStorageClass struct {
	client       clientset.Interface
	storageClass *storagev1.StorageClass
	namespace    *v1.Namespace
}

func NewTestStorageClass(c clientset.Interface, ns *v1.Namespace, sc *storagev1.StorageClass) *TestStorageClass {
	return &TestStorageClass{
		client:       c,
		storageClass: sc,
		namespace:    ns,
	}
}

func (t *TestStorageClass) Create(ctx context.Context) storagev1.StorageClass {
	var err error

	ginkgo.By("creating a StorageClass " + t.storageClass.Name)
	t.storageClass, err = t.client.StorageV1().StorageClasses().Create(ctx, t.storageClass, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	return *t.storageClass
}

func (t *TestStorageClass) Cleanup(ctx context.Context) {
	framework.Logf("deleting StorageClass %s", t.storageClass.Name)
	err := t.client.StorageV1().StorageClasses().Delete(ctx, t.storageClass.Name, metav1.DeleteOptions{})
	framework.ExpectNoError(err)
}

type TestVolumeSnapshotClass struct {
	client              restclientset.Interface
	volumeSnapshotClass *snapshotv1.VolumeSnapshotClass
	namespace           *v1.Namespace
}

func NewTestVolumeSnapshotClass(c restclientset.Interface, ns *v1.Namespace, vsc *snapshotv1.VolumeSnapshotClass) *TestVolumeSnapshotClass {
	return &TestVolumeSnapshotClass{
		client:              c,
		volumeSnapshotClass: vsc,
		namespace:           ns,
	}
}

func (t *TestVolumeSnapshotClass) Create(ctx context.Context) {
	ginkgo.By("creating a VolumeSnapshotClass")
	var err error
	t.volumeSnapshotClass, err = snapshotclientset.New(t.client).SnapshotV1().VolumeSnapshotClasses().Create(ctx, t.volumeSnapshotClass, metav1.CreateOptions{})
	framework.ExpectNoError(err)
}

func (t *TestVolumeSnapshotClass) CreateSnapshot(ctx context.Context, pvc *v1.PersistentVolumeClaim) *snapshotv1.VolumeSnapshot {
	ginkgo.By("creating a VolumeSnapshot for " + pvc.Name)
	snapshot := &snapshotv1.VolumeSnapshot{
		TypeMeta: metav1.TypeMeta{
			Kind:       VolumeSnapshotKind,
			APIVersion: SnapshotAPIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "volume-snapshot-",
			Namespace:    t.namespace.Name,
		},
		Spec: snapshotv1.VolumeSnapshotSpec{
			VolumeSnapshotClassName: &t.volumeSnapshotClass.Name,
			Source: snapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvc.Name,
			},
		},
	}
	snapshot, err := snapshotclientset.New(t.client).SnapshotV1().VolumeSnapshots(t.namespace.Name).Create(ctx, snapshot, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	return snapshot
}

func (t *TestVolumeSnapshotClass) ReadyToUse(ctx context.Context, snapshot *snapshotv1.VolumeSnapshot) {
	ginkgo.By("waiting for VolumeSnapshot to be ready to use - " + snapshot.Name)
	err := wait.Poll(15*time.Second, 30*time.Minute, func() (bool, error) {
		vs, err := snapshotclientset.New(t.client).SnapshotV1().VolumeSnapshots(t.namespace.Name).Get(ctx, snapshot.Name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("did not see ReadyToUse: %v", err)
		}
		return *vs.Status.ReadyToUse, nil
	})
	framework.ExpectNoError(err)
}

func (t *TestVolumeSnapshotClass) DeleteSnapshot(ctx context.Context, vs *snapshotv1.VolumeSnapshot) {
	ginkgo.By("deleting a VolumeSnapshot " + vs.Name)
	err := snapshotclientset.New(t.client).SnapshotV1().VolumeSnapshots(t.namespace.Name).Delete(ctx, vs.Name, metav1.DeleteOptions{})
	framework.ExpectNoError(err)
}

func (t *TestVolumeSnapshotClass) Cleanup() {
	// skip deleting volume snapshot storage class otherwise snapshot e2e test will fail, details:
	// https://github.com/kubernetes-sigs/azuredisk-csi-driver/pull/260#issuecomment-583296932
	framework.Logf("skip deleting VolumeSnapshotClass %s", t.volumeSnapshotClass.Name)
	//err := snapshotclientset.New(t.client).SnapshotV1().VolumeSnapshotClasses().Delete(t.volumeSnapshotClass.Name, nil)
	//framework.ExpectNoError(err)
}

type TestPreProvisionedPersistentVolume struct {
	client                    clientset.Interface
	persistentVolume          *v1.PersistentVolume
	requestedPersistentVolume *v1.PersistentVolume
}

func NewTestPreProvisionedPersistentVolume(c clientset.Interface, pv *v1.PersistentVolume) *TestPreProvisionedPersistentVolume {
	return &TestPreProvisionedPersistentVolume{
		client:                    c,
		requestedPersistentVolume: pv,
	}
}

func (pv *TestPreProvisionedPersistentVolume) Create(ctx context.Context) v1.PersistentVolume {
	var err error
	ginkgo.By("creating a PV")
	pv.persistentVolume, err = pv.client.CoreV1().PersistentVolumes().Create(ctx, pv.requestedPersistentVolume, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	return *pv.persistentVolume
}

type TestPersistentVolumeClaim struct {
	client                         clientset.Interface
	claimSize                      string
	volumeMode                     v1.PersistentVolumeMode
	accessMode                     v1.PersistentVolumeAccessMode
	storageClass                   *storagev1.StorageClass
	namespace                      *v1.Namespace
	persistentVolume               *v1.PersistentVolume
	persistentVolumeClaim          *v1.PersistentVolumeClaim
	requestedPersistentVolumeClaim *v1.PersistentVolumeClaim
	dataSource                     *v1.TypedLocalObjectReference
}

func NewTestPersistentVolumeClaim(c clientset.Interface, ns *v1.Namespace, claimSize string, volumeMode VolumeMode, accessMode v1.PersistentVolumeAccessMode, sc *storagev1.StorageClass) *TestPersistentVolumeClaim {
	mode := v1.PersistentVolumeFilesystem
	if volumeMode == Block {
		mode = v1.PersistentVolumeBlock
	}
	return &TestPersistentVolumeClaim{
		client:       c,
		claimSize:    claimSize,
		volumeMode:   mode,
		accessMode:   accessMode,
		namespace:    ns,
		storageClass: sc,
	}
}

func NewTestPersistentVolumeClaimWithDataSource(c clientset.Interface, ns *v1.Namespace, claimSize string, volumeMode VolumeMode, accessMode v1.PersistentVolumeAccessMode, sc *storagev1.StorageClass, dataSource *v1.TypedLocalObjectReference) *TestPersistentVolumeClaim {
	mode := v1.PersistentVolumeFilesystem
	if volumeMode == Block {
		mode = v1.PersistentVolumeBlock
	}
	return &TestPersistentVolumeClaim{
		client:       c,
		claimSize:    claimSize,
		volumeMode:   mode,
		accessMode:   accessMode,
		namespace:    ns,
		storageClass: sc,
		dataSource:   dataSource,
	}
}

func (t *TestPersistentVolumeClaim) Create(ctx context.Context) {
	var err error

	ginkgo.By("creating a PVC")
	storageClassName := ""
	if t.storageClass != nil {
		storageClassName = t.storageClass.Name
	}
	t.requestedPersistentVolumeClaim = generatePVC(t.namespace.Name, storageClassName, "", t.claimSize, t.volumeMode, t.accessMode, t.dataSource)
	t.persistentVolumeClaim, err = t.client.CoreV1().PersistentVolumeClaims(t.namespace.Name).Create(ctx, t.requestedPersistentVolumeClaim, metav1.CreateOptions{})
	framework.ExpectNoError(err)
}

func (t *TestPersistentVolumeClaim) ValidateProvisionedPersistentVolume(ctx context.Context) {
	var err error

	// Make sure the persistentVolumeClaim.Spec.VolumeName isn't empty
	t.persistentVolumeClaim, err = t.client.CoreV1().PersistentVolumeClaims(t.namespace.Name).Get(ctx, t.persistentVolumeClaim.Name, metav1.GetOptions{})
	framework.ExpectNoError(err)
	// Get the bound PersistentVolume
	ginkgo.By("validating provisioned PV")
	t.persistentVolume, err = t.client.CoreV1().PersistentVolumes().Get(ctx, t.persistentVolumeClaim.Spec.VolumeName, metav1.GetOptions{})
	framework.ExpectNoError(err)

	// Check sizes
	expectedCapacity := t.requestedPersistentVolumeClaim.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	claimCapacity := t.persistentVolumeClaim.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	gomega.Expect(claimCapacity.Value()).To(gomega.Equal(expectedCapacity.Value()), "claimCapacity is not equal to requestedCapacity")

	pvCapacity := t.persistentVolume.Spec.Capacity[v1.ResourceName(v1.ResourceStorage)]
	gomega.Expect(pvCapacity.Value()).To(gomega.Equal(expectedCapacity.Value()), "pvCapacity is not equal to requestedCapacity")

	// Check PV properties
	ginkgo.By("checking the PV")
	expectedAccessModes := t.requestedPersistentVolumeClaim.Spec.AccessModes
	gomega.Expect(t.persistentVolume.Spec.AccessModes).To(gomega.Equal(expectedAccessModes))
	gomega.Expect(t.persistentVolume.Spec.ClaimRef.Name).To(gomega.Equal(t.persistentVolumeClaim.ObjectMeta.Name))
	gomega.Expect(t.persistentVolume.Spec.ClaimRef.Namespace).To(gomega.Equal(t.persistentVolumeClaim.ObjectMeta.Namespace))
	// If storageClass is nil, PV was pre-provisioned with these values already set
	if t.storageClass != nil {
		gomega.Expect(t.persistentVolume.Spec.PersistentVolumeReclaimPolicy).To(gomega.Equal(*t.storageClass.ReclaimPolicy))
		gomega.Expect(t.persistentVolume.Spec.MountOptions).To(gomega.Equal(t.storageClass.MountOptions))
		if *t.storageClass.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
			gomega.Expect(t.persistentVolume.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions[0].Values).
				To(gomega.HaveLen(1))
		}
	}
}

func (t *TestPersistentVolumeClaim) WaitForBound(ctx context.Context) v1.PersistentVolumeClaim {
	var err error

	ginkgo.By(fmt.Sprintf("waiting for PVC to be in phase %q", v1.ClaimBound))
	err = e2epv.WaitForPersistentVolumeClaimPhase(ctx, v1.ClaimBound, t.client, t.namespace.Name, t.persistentVolumeClaim.Name, framework.Poll, framework.ClaimProvisionTimeout)
	framework.ExpectNoError(err)

	ginkgo.By("checking the PVC")
	// Get new copy of the claim
	t.persistentVolumeClaim, err = t.client.CoreV1().PersistentVolumeClaims(t.namespace.Name).Get(ctx, t.persistentVolumeClaim.Name, metav1.GetOptions{})
	framework.ExpectNoError(err)

	return *t.persistentVolumeClaim
}

func generatePVC(namespace, storageClassName, name, claimSize string, volumeMode v1.PersistentVolumeMode, accessMode v1.PersistentVolumeAccessMode, dataSource *v1.TypedLocalObjectReference) *v1.PersistentVolumeClaim {
	if accessMode != v1.ReadWriteOnce && accessMode != v1.ReadOnlyMany && accessMode != v1.ReadWriteMany {
		accessMode = v1.ReadWriteOnce
	}

	pvcMeta := metav1.ObjectMeta{
		Namespace: namespace,
	}
	if name == "" {
		pvcMeta.GenerateName = "pvc-"
	} else {
		pvcMeta.Name = name
	}

	return &v1.PersistentVolumeClaim{
		ObjectMeta: pvcMeta,
		Spec: v1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClassName,
			AccessModes: []v1.PersistentVolumeAccessMode{
				accessMode,
			},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(claimSize),
				},
			},
			VolumeMode: &volumeMode,
			DataSource: dataSource,
		},
	}
}

func (t *TestPersistentVolumeClaim) Cleanup(ctx context.Context) {
	// Since PV is created after pod creation when the volume binding mode is WaitForFirstConsumer,
	// we need to populate fields such as PVC and PV info in TestPersistentVolumeClaim, and valid it
	if t.storageClass != nil && *t.storageClass.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
		var err error
		t.persistentVolumeClaim, err = t.client.CoreV1().PersistentVolumeClaims(t.namespace.Name).Get(ctx, t.persistentVolumeClaim.Name, metav1.GetOptions{})
		framework.ExpectNoError(err)
		t.ValidateProvisionedPersistentVolume(ctx)
	}
	framework.Logf("deleting PVC %q/%q", t.namespace.Name, t.persistentVolumeClaim.Name)
	err := e2epv.DeletePersistentVolumeClaim(ctx, t.client, t.persistentVolumeClaim.Name, t.namespace.Name)
	framework.ExpectNoError(err)
	// Wait for the PV to get deleted if reclaim policy is Delete. (If it's
	// Retain, there's no use waiting because the PV won't be auto-deleted and
	// it's expected for the caller to do it.) Technically, the first few delete
	// attempts may fail, as the volume is still attached to a node because
	// kubelet is slowly cleaning up the previous pod, however it should succeed
	// in a couple of minutes.
	if t.persistentVolume.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimDelete {
		ginkgo.By(fmt.Sprintf("waiting for claim's PV %q to be deleted", t.persistentVolume.Name))
		err := e2epv.WaitForPersistentVolumeDeleted(ctx, t.client, t.persistentVolume.Name, 5*time.Second, 20*time.Minute)
		framework.ExpectNoError(err)
	}
	// Wait for the PVC to be deleted
	err = waitForPersistentVolumeClaimDeleted(ctx, t.client, t.namespace.Name, t.persistentVolumeClaim.Name, 5*time.Second, 5*time.Minute)
	framework.ExpectNoError(err)
}

func (t *TestPersistentVolumeClaim) ReclaimPolicy() v1.PersistentVolumeReclaimPolicy {
	return t.persistentVolume.Spec.PersistentVolumeReclaimPolicy
}

func (t *TestPersistentVolumeClaim) WaitForPersistentVolumePhase(ctx context.Context, phase v1.PersistentVolumePhase) {
	err := e2epv.WaitForPersistentVolumePhase(ctx, phase, t.client, t.persistentVolume.Name, 5*time.Second, 10*time.Minute)
	framework.ExpectNoError(err)
}

func (t *TestPersistentVolumeClaim) DeleteBoundPersistentVolume(ctx context.Context) {
	ginkgo.By(fmt.Sprintf("deleting PV %q", t.persistentVolume.Name))
	err := e2epv.DeletePersistentVolume(ctx, t.client, t.persistentVolume.Name)
	framework.ExpectNoError(err)
	ginkgo.By(fmt.Sprintf("waiting for claim's PV %q to be deleted", t.persistentVolume.Name))
	err = e2epv.WaitForPersistentVolumeDeleted(ctx, t.client, t.persistentVolume.Name, 5*time.Second, 20*time.Minute)
	framework.ExpectNoError(err)
}

func (t *TestPersistentVolumeClaim) DeleteBackingVolume(ctx context.Context, driver azuredisk.CSIDriver) {
	volumeID := t.persistentVolume.Spec.CSI.VolumeHandle
	ginkgo.By(fmt.Sprintf("deleting azuredisk volume %q", volumeID))
	req := &csi.DeleteVolumeRequest{
		VolumeId: volumeID,
	}
	_, err := driver.DeleteVolume(ctx, req)
	if err != nil {
		ginkgo.Fail(fmt.Sprintf("could not delete volume %q: %v", volumeID, err))
	}
}

type TestDeployment struct {
	client     clientset.Interface
	deployment *apps.Deployment
	namespace  *v1.Namespace
	podNames   []string
}

func NewTestDeployment(c clientset.Interface, ns *v1.Namespace, replicaCount int32, command string, pvc *v1.PersistentVolumeClaim, volumeName, mountPath string, readOnly, isWindows, useCMD, useAntiAffinity bool, winServerVer string) *TestDeployment {
	generateName := "azuredisk-volume-tester-"
	selectorValue := fmt.Sprintf("%s%d", generateName, rand.Int())

	volumeMounts := make([]v1.VolumeMount, 0)
	volumeDevices := make([]v1.VolumeDevice, 0)

	if pvc.Spec.VolumeMode == nil || *pvc.Spec.VolumeMode == v1.PersistentVolumeFilesystem {
		volumeMounts = []v1.VolumeMount{
			{
				Name:      volumeName,
				MountPath: mountPath,
				ReadOnly:  readOnly,
			},
		}
	} else {
		volumeDevices = []v1.VolumeDevice{
			{
				Name:       volumeName,
				DevicePath: mountPath,
			},
		}
	}

	testDeployment := &TestDeployment{
		client:    c,
		namespace: ns,
		deployment: &apps.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: generateName,
			},
			Spec: apps.DeploymentSpec{
				Replicas: &replicaCount,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": selectorValue},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": selectorValue},
					},
					Spec: v1.PodSpec{
						NodeSelector: map[string]string{"kubernetes.io/os": "linux"},
						Containers: []v1.Container{
							{
								Name:          "volume-tester",
								Image:         imageutils.GetE2EImage(imageutils.BusyBox),
								Command:       []string{"/bin/sh"},
								Args:          []string{"-c", command},
								VolumeMounts:  volumeMounts,
								VolumeDevices: volumeDevices,
							},
						},
						RestartPolicy: v1.RestartPolicyAlways,
						Volumes: []v1.Volume{
							{
								Name: volumeName,
								VolumeSource: v1.VolumeSource{
									PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
										ClaimName: pvc.Name,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if useAntiAffinity {
		affinity := &v1.Affinity{
			PodAntiAffinity: &v1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": selectorValue},
						},
						TopologyKey: driver.TopologyKey,
					},
				},
			},
		}
		testDeployment.deployment.Spec.Template.Spec.Affinity = affinity
	}

	if isWindows {
		testDeployment.deployment.Spec.Template.Spec.NodeSelector = map[string]string{
			"kubernetes.io/os": "windows",
		}
		testDeployment.deployment.Spec.Template.Spec.Containers[0].Image = "mcr.microsoft.com/windows/servercore:" + getWinImageTag(winServerVer)
		if useCMD {
			testDeployment.deployment.Spec.Template.Spec.Containers[0].Command = []string{"cmd"}
			testDeployment.deployment.Spec.Template.Spec.Containers[0].Args = []string{"/c", command}
		} else {
			testDeployment.deployment.Spec.Template.Spec.Containers[0].Command = []string{"powershell.exe"}
			testDeployment.deployment.Spec.Template.Spec.Containers[0].Args = []string{"-Command", command}
		}
	}

	return testDeployment
}

func (t *TestDeployment) Create(ctx context.Context) {
	var err error
	t.deployment, err = t.client.AppsV1().Deployments(t.namespace.Name).Create(ctx, t.deployment, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	err = testutil.WaitForDeploymentComplete(t.client, t.deployment, framework.Logf, poll, pollLongTimeout)
	framework.ExpectNoError(err)
	pods, err := deployment.GetPodsForDeployment(ctx, t.client, t.deployment)
	framework.ExpectNoError(err)
	for _, pod := range pods.Items {
		t.podNames = append(t.podNames, pod.Name)
	}
}

func (t *TestDeployment) WaitForPodReady(ctx context.Context) {
	pods, err := deployment.GetPodsForDeployment(ctx, t.client, t.deployment)
	framework.ExpectNoError(err)
	t.podNames = []string{}
	for _, pod := range pods.Items {
		t.podNames = append(t.podNames, pod.Name)
	}
	ch := make(chan error, len(t.podNames))
	defer close(ch)
	for _, pod := range pods.Items {
		go func(client clientset.Interface, pod v1.Pod) {
			err = e2epod.WaitForPodRunningInNamespace(ctx, client, &pod)
			ch <- err
		}(t.client, pod)
	}
	// Wait on all goroutines to report on pod ready
	var wg sync.WaitGroup
	for range t.podNames {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := <-ch
			framework.ExpectNoError(err)
		}()
	}
	wg.Wait()
}

func (t *TestDeployment) PollForStringInPodsExec(command []string, expectedString string) {
	pollForStringInPodsExec(t.namespace.Name, t.podNames, command, expectedString)
}

func (t *TestDeployment) DeletePodAndWait(ctx context.Context) {
	ch := make(chan error, len(t.podNames))
	for _, podName := range t.podNames {
		framework.Logf("Deleting pod %q in namespace %q", podName, t.namespace.Name)
		go func(client clientset.Interface, ns, podName string) {
			err := client.CoreV1().Pods(ns).Delete(ctx, podName, metav1.DeleteOptions{})
			ch <- err
		}(t.client, t.namespace.Name, podName)
	}
	// Wait on all goroutines to report on pod delete
	for _, podName := range t.podNames {
		err := <-ch
		if err != nil {
			if !apierrs.IsNotFound(err) {
				framework.ExpectNoError(fmt.Errorf("pod %q Delete API error: %v", podName, err))
			}
		}
	}

	for _, podName := range t.podNames {
		framework.Logf("Waiting for pod %q in namespace %q to be fully deleted", podName, t.namespace.Name)
		go func(client clientset.Interface, ns, podName string) {
			err := e2epod.WaitForPodNotFoundInNamespace(ctx, client, podName, ns, e2epod.DefaultPodDeletionTimeout)
			ch <- err
		}(t.client, t.namespace.Name, podName)
	}
	// Wait on all goroutines to report on pod terminating
	for _, podName := range t.podNames {
		err := <-ch
		if err != nil {
			framework.ExpectNoError(fmt.Errorf("pod %q error waiting for delete: %v", podName, err))
		}
	}
}

func (t *TestDeployment) Cleanup(ctx context.Context) {
	framework.Logf("deleting Deployment %q/%q", t.namespace.Name, t.deployment.Name)
	body, err := t.Logs(ctx)
	if err != nil {
		framework.Logf("Error getting logs for %s: %v", t.deployment.Name, err)
	} else {
		for i, logs := range body {
			framework.Logf("Pod %s has the following logs: %s", t.podNames[i], string(logs))
		}
	}

	err = t.client.AppsV1().Deployments(t.namespace.Name).Delete(ctx, t.deployment.Name, metav1.DeleteOptions{})
	framework.ExpectNoError(err)
}

func (t *TestDeployment) Logs(ctx context.Context) (logs [][]byte, err error) {
	for _, name := range t.podNames {
		log, err := podLogs(ctx, t.client, name, t.namespace.Name)
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	return
}

type TestStatefulset struct {
	client      clientset.Interface
	statefulset *apps.StatefulSet
	namespace   *v1.Namespace
	podName     string
}

func NewTestStatefulset(c clientset.Interface, ns *v1.Namespace, command string, pvc *v1.PersistentVolumeClaim, volumeName, mountPath string, readOnly, isWindows, useCMD bool, winServerVer string) *TestStatefulset {
	generateName := "azuredisk-volume-tester-"
	selectorValue := fmt.Sprintf("%s%d", generateName, rand.Int())
	replicas := int32(1)
	var volumeClaimTest []v1.PersistentVolumeClaim
	volumeClaimTest = append(volumeClaimTest, *pvc)
	testStatefulset := &TestStatefulset{
		client:    c,
		namespace: ns,
		statefulset: &apps.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: generateName,
			},
			Spec: apps.StatefulSetSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"app": selectorValue},
				},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": selectorValue},
					},
					Spec: v1.PodSpec{
						NodeSelector: map[string]string{"kubernetes.io/os": "linux"},
						Containers: []v1.Container{
							{
								Name:    "volume-tester",
								Image:   imageutils.GetE2EImage(imageutils.BusyBox),
								Command: []string{"/bin/sh"},
								Args:    []string{"-c", command},
								VolumeMounts: []v1.VolumeMount{
									{
										Name:      volumeName,
										MountPath: mountPath,
										ReadOnly:  readOnly,
									},
								},
							},
						},
						RestartPolicy: v1.RestartPolicyAlways,
					},
				},
				VolumeClaimTemplates: volumeClaimTest,
			},
		},
	}

	if isWindows {
		testStatefulset.statefulset.Spec.Template.Spec.NodeSelector = map[string]string{
			"kubernetes.io/os": "windows",
		}
		testStatefulset.statefulset.Spec.Template.Spec.Containers[0].Image = "mcr.microsoft.com/windows/servercore:" + getWinImageTag(winServerVer)
		if useCMD {
			testStatefulset.statefulset.Spec.Template.Spec.Containers[0].Command = []string{"cmd"}
			testStatefulset.statefulset.Spec.Template.Spec.Containers[0].Args = []string{"/c", command}
		} else {
			testStatefulset.statefulset.Spec.Template.Spec.Containers[0].Command = []string{"powershell.exe"}
			testStatefulset.statefulset.Spec.Template.Spec.Containers[0].Args = []string{"-Command", command}
		}
	}

	return testStatefulset
}

func (t *TestStatefulset) Create(ctx context.Context) {
	var err error
	t.statefulset, err = t.client.AppsV1().StatefulSets(t.namespace.Name).Create(ctx, t.statefulset, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	err = waitForStatefulSetComplete(ctx, t.client, t.namespace, t.statefulset)
	framework.ExpectNoError(err)
	selector, err := metav1.LabelSelectorAsSelector(t.statefulset.Spec.Selector)
	framework.ExpectNoError(err)
	options := metav1.ListOptions{LabelSelector: selector.String()}
	statefulSetPods, err := t.client.CoreV1().Pods(t.namespace.Name).List(ctx, options)
	framework.ExpectNoError(err)
	// always get first pod as there should only be one
	t.podName = statefulSetPods.Items[0].Name
}

func (t *TestStatefulset) WaitForPodReady(ctx context.Context) {
	selector, err := metav1.LabelSelectorAsSelector(t.statefulset.Spec.Selector)
	framework.ExpectNoError(err)
	options := metav1.ListOptions{LabelSelector: selector.String()}
	statefulSetPods, err := t.client.CoreV1().Pods(t.namespace.Name).List(ctx, options)
	framework.ExpectNoError(err)
	// always get first pod as there should only be one
	pod := statefulSetPods.Items[0]
	t.podName = pod.Name
	err = e2epod.WaitForPodRunningInNamespace(ctx, t.client, &pod)
	framework.ExpectNoError(err)
}

func (t *TestStatefulset) PollForStringInPodsExec(command []string, expectedString string) {
	pollForStringInPodsExec(t.namespace.Name, []string{t.podName}, command, expectedString)
}

func (t *TestStatefulset) DeletePodAndWait(ctx context.Context) {
	framework.Logf("Deleting pod %q in namespace %q", t.podName, t.namespace.Name)
	err := t.client.CoreV1().Pods(t.namespace.Name).Delete(ctx, t.podName, metav1.DeleteOptions{})
	if err != nil {
		if !apierrs.IsNotFound(err) {
			framework.ExpectNoError(fmt.Errorf("pod %q Delete API error: %v", t.podName, err))
		}
		return
	}
	//sleep ensure waitForPodready will not be passed before old pod is deleted.
	time.Sleep(60 * time.Second)
}

func (t *TestStatefulset) Cleanup(ctx context.Context) {
	framework.Logf("deleting StatefulSet %q/%q", t.namespace.Name, t.statefulset.Name)
	body, err := t.Logs(ctx)
	if err != nil {
		framework.Logf("Error getting logs for pod %s: %v", t.podName, err)
	} else {
		framework.Logf("Pod %s has the following logs: %s", t.podName, body)
	}
	err = t.client.AppsV1().StatefulSets(t.namespace.Name).Delete(ctx, t.statefulset.Name, metav1.DeleteOptions{})
	framework.ExpectNoError(err)
}

func (t *TestStatefulset) Logs(ctx context.Context) ([]byte, error) {
	return podLogs(ctx, t.client, t.podName, t.namespace.Name)
}
func waitForStatefulSetComplete(ctx context.Context, cs clientset.Interface, ns *v1.Namespace, ss *apps.StatefulSet) error {
	err := wait.PollImmediate(poll, pollTimeout, func() (bool, error) {
		var err error
		statefulSet, err := cs.AppsV1().StatefulSets(ns.Name).Get(ctx, ss.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		klog.Infof("%d/%d replicas in the StatefulSet are ready", statefulSet.Status.ReadyReplicas, *statefulSet.Spec.Replicas)
		if statefulSet.Status.ReadyReplicas == *statefulSet.Spec.Replicas {
			return true, nil
		}
		return false, nil
	})

	return err
}

type TestJob struct {
	client    clientset.Interface
	job       *batchv1.Job
	namespace *v1.Namespace
}

func NewTestJob(c clientset.Interface, ns *v1.Namespace, ttlSecondsAfterFinished *int32, command string, pvc *v1.PersistentVolumeClaim, volumeName, mountPath string, readOnly, isWindows, useCMD, useAntiAffinity bool, winServerVer string) *TestJob {
	generateName := "azuredisk-volume-tester-"
	selectorValue := fmt.Sprintf("%s%d", generateName, rand.Int())

	volumeMounts := make([]v1.VolumeMount, 0)
	volumeDevices := make([]v1.VolumeDevice, 0)

	if pvc.Spec.VolumeMode == nil || *pvc.Spec.VolumeMode == v1.PersistentVolumeFilesystem {
		volumeMounts = []v1.VolumeMount{
			{
				Name:      volumeName,
				MountPath: mountPath,
				ReadOnly:  readOnly,
			},
		}
	} else {
		volumeDevices = []v1.VolumeDevice{
			{
				Name:       volumeName,
				DevicePath: mountPath,
			},
		}
	}

	testJob := &TestJob{
		client:    c,
		namespace: ns,
		job: &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: generateName,
			},
			Spec: batchv1.JobSpec{
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": selectorValue},
					},
					Spec: v1.PodSpec{
						NodeSelector: map[string]string{"kubernetes.io/os": "linux"},
						Containers: []v1.Container{
							{
								Name:          "volume-tester",
								Image:         imageutils.GetE2EImage(imageutils.BusyBox),
								Command:       []string{"/bin/sh"},
								Args:          []string{"-c", command},
								VolumeMounts:  volumeMounts,
								VolumeDevices: volumeDevices,
							},
						},
						RestartPolicy: v1.RestartPolicyNever,
						Volumes: []v1.Volume{
							{
								Name: volumeName,
								VolumeSource: v1.VolumeSource{
									PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
										ClaimName: pvc.Name,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if ttlSecondsAfterFinished != nil {
		testJob.job.Spec.TTLSecondsAfterFinished = ttlSecondsAfterFinished
	}

	if useAntiAffinity {
		affinity := &v1.Affinity{
			PodAntiAffinity: &v1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": selectorValue},
						},
						TopologyKey: driver.TopologyKey,
					},
				},
			},
		}
		testJob.job.Spec.Template.Spec.Affinity = affinity
	}

	if isWindows {
		testJob.job.Spec.Template.Spec.NodeSelector = map[string]string{
			"kubernetes.io/os": "windows",
		}
		testJob.job.Spec.Template.Spec.Containers[0].Image = "mcr.microsoft.com/windows/servercore:" + getWinImageTag(winServerVer)
		if useCMD {
			testJob.job.Spec.Template.Spec.Containers[0].Command = []string{"cmd"}
			testJob.job.Spec.Template.Spec.Containers[0].Args = []string{"/c", command}
		} else {
			testJob.job.Spec.Template.Spec.Containers[0].Command = []string{"powershell.exe"}
			testJob.job.Spec.Template.Spec.Containers[0].Args = []string{"-Command", command}
		}
	}

	return testJob
}

func (t *TestJob) Create(ctx context.Context) {
	var err error
	t.job, err = t.client.BatchV1().Jobs(t.namespace.Name).Create(ctx, t.job, metav1.CreateOptions{})
	framework.ExpectNoError(err)
}

func (t *TestJob) WaitForAttachBatchCheck(ctx context.Context) error {
	selector, err := metav1.LabelSelectorAsSelector(t.job.Spec.Selector)
	framework.ExpectNoError(err)
	jobPods, err := e2epod.WaitForPodsWithLabel(ctx, t.client, t.namespace.Name, selector)
	framework.ExpectNoError(err)

	err = waitForPodEvent(ctx, t.client, &jobPods.Items[0], "The maximum number of data disks allowed to be attached to a VM of this size is", 10*time.Minute)
	if err == nil {
		return fmt.Errorf("pod %q hit MaximumDataDisksExceeded issue during attaching", jobPods.Items[0].Name)
	}
	return nil
}

// waitForPodEvent waits for a pod event with the given message to be emitted.
func waitForPodEvent(ctx context.Context, c clientset.Interface, pod *v1.Pod, meg string, timeout time.Duration) error {
	conditionDesc := fmt.Sprintf("failed with pod event: %s", meg)
	return e2epod.WaitForPodCondition(ctx, c, pod.Namespace, pod.Name, conditionDesc, timeout, func(pod *v1.Pod) (bool, error) {
		switch pod.Status.Phase {
		case v1.PodRunning, v1.PodFailed, v1.PodSucceeded:
			return true, errors.New("pod running, failed or succeeded")
		case v1.PodPending:
			podEvents, err := c.CoreV1().Events(pod.Namespace).List(ctx, metav1.ListOptions{
				FieldSelector: fields.OneTermEqualSelector("involvedObject.name", pod.Name).String(),
			})
			if err != nil {
				return true, fmt.Errorf("failed to list events for pod %s: %w", pod.Name, err)
			}

			for _, event := range podEvents.Items {
				if strings.Contains(event.Message, meg) {
					return true, nil
				}
			}
		}
		return false, nil
	})
}

func (t *TestJob) Cleanup(ctx context.Context) {
	err := t.client.BatchV1().Jobs(t.namespace.Name).Delete(ctx, t.job.Name, metav1.DeleteOptions{})
	if !apierrs.IsNotFound(err) {
		framework.Logf("deleting Job %q/%q", t.namespace.Name, t.job.Name)
		framework.ExpectNoError(err)
	}
}

type TestPod struct {
	client    clientset.Interface
	pod       *v1.Pod
	namespace *v1.Namespace
}

func NewTestPod(c clientset.Interface, ns *v1.Namespace, command string, isWindows bool, winServerVer string) *TestPod {
	testPod := &TestPod{
		client:    c,
		namespace: ns,
		pod: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "azuredisk-volume-tester-",
			},
			Spec: v1.PodSpec{
				NodeSelector: map[string]string{"kubernetes.io/os": "linux"},
				Containers: []v1.Container{
					{
						Name:    "volume-tester",
						Image:   imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{"/bin/sh"},
						Args:    []string{"-c", command},
					},
				},
				RestartPolicy:                v1.RestartPolicyNever,
				Volumes:                      make([]v1.Volume, 0),
				AutomountServiceAccountToken: ptr.To(false),
			},
		},
	}
	if isWindows {
		testPod.pod.Spec.NodeSelector = map[string]string{
			"kubernetes.io/os": "windows",
		}
		testPod.pod.Spec.Containers[0].Image = "mcr.microsoft.com/windows/servercore:" + getWinImageTag(winServerVer)
		testPod.pod.Spec.Containers[0].Command = []string{"powershell.exe"}
		testPod.pod.Spec.Containers[0].Args = []string{"-Command", command}
	}

	return testPod
}

func (t *TestPod) Create(ctx context.Context) {
	var err error

	t.pod, err = t.client.CoreV1().Pods(t.namespace.Name).Create(ctx, t.pod, metav1.CreateOptions{})
	framework.ExpectNoError(err)
}

func (t *TestPod) WaitForSuccess(ctx context.Context) {
	err := e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, t.client, t.pod.Name, t.namespace.Name, 15*time.Minute)
	framework.ExpectNoError(err)
}

func (t *TestPod) WaitForRunning(ctx context.Context) {
	err := e2epod.WaitForPodRunningInNamespace(ctx, t.client, t.pod)
	framework.ExpectNoError(err)
}

func (t *TestPod) WaitForRunningLong(ctx context.Context) {
	err := e2epod.WaitForPodRunningInNamespaceSlow(ctx, t.client, t.pod.Name, t.namespace.Name)
	framework.ExpectNoError(err)
}

func (t *TestPod) WaitForFailedMountError(ctx context.Context) {
	err := e2eevents.WaitTimeoutForEvent(
		ctx,
		t.client,
		t.namespace.Name,
		fields.Set{"reason": events.FailedMountVolume}.AsSelector().String(),
		"MountVolume.MountDevice failed for volume",
		pollLongTimeout)
	framework.ExpectNoError(err)
}

// Ideally this would be in "k8s.io/kubernetes/test/e2e/framework"
// Similar to framework.WaitForPodSuccessInNamespaceSlow
var podFailedCondition = func(pod *v1.Pod) (bool, error) {
	switch pod.Status.Phase {
	case v1.PodFailed:
		ginkgo.By("Saw pod failure")
		return true, nil
	case v1.PodSucceeded:
		return true, fmt.Errorf("pod %q succeeded with reason: %q, message: %q", pod.Name, pod.Status.Reason, pod.Status.Message)
	default:
		return false, nil
	}
}

func (t *TestPod) WaitForFailure(ctx context.Context) {
	err := e2epod.WaitForPodCondition(ctx, t.client, t.namespace.Name, t.pod.Name, failedConditionDescription, slowPodStartTimeout, podFailedCondition)
	framework.ExpectNoError(err)
}

func (t *TestPod) SetupVolume(pvc *v1.PersistentVolumeClaim, name, mountPath string, readOnly bool) {
	volumeMount := v1.VolumeMount{
		Name:      name,
		MountPath: mountPath,
		ReadOnly:  readOnly,
	}
	t.pod.Spec.Containers[0].VolumeMounts = append(t.pod.Spec.Containers[0].VolumeMounts, volumeMount)

	volume := v1.Volume{
		Name: name,
		VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvc.Name,
			},
		},
	}
	t.pod.Spec.Volumes = append(t.pod.Spec.Volumes, volume)
}

func (t *TestPod) SetupInlineVolume(name, mountPath, diskURI string, readOnly bool) {
	volumeMount := v1.VolumeMount{
		Name:      name,
		MountPath: mountPath,
		ReadOnly:  readOnly,
	}
	t.pod.Spec.Containers[0].VolumeMounts = append(t.pod.Spec.Containers[0].VolumeMounts, volumeMount)

	kind := v1.AzureDataDiskKind("Managed")
	_, _, diskName, _ := azureutils.GetInfoFromURI(diskURI) //nolint
	volume := v1.Volume{
		Name: name,
		VolumeSource: v1.VolumeSource{
			AzureDisk: &v1.AzureDiskVolumeSource{
				DiskName:    diskName,
				DataDiskURI: diskURI,
				ReadOnly:    &readOnly,
				Kind:        &kind,
			},
		},
	}
	t.pod.Spec.Volumes = append(t.pod.Spec.Volumes, volume)
}

func (t *TestPod) SetupRawBlockVolume(pvc *v1.PersistentVolumeClaim, name, devicePath string) {
	volumeDevice := v1.VolumeDevice{
		Name:       name,
		DevicePath: devicePath,
	}
	t.pod.Spec.Containers[0].VolumeDevices = make([]v1.VolumeDevice, 0)
	t.pod.Spec.Containers[0].VolumeDevices = append(t.pod.Spec.Containers[0].VolumeDevices, volumeDevice)

	volume := v1.Volume{
		Name: name,
		VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvc.Name,
			},
		},
	}
	t.pod.Spec.Volumes = append(t.pod.Spec.Volumes, volume)
}

func (t *TestPod) SetupVolumeMountWithSubpath(pvc *v1.PersistentVolumeClaim, name, mountPath string, subpath string, readOnly bool) {
	volumeMount := v1.VolumeMount{
		Name:      name,
		MountPath: mountPath,
		SubPath:   subpath,
		ReadOnly:  readOnly,
	}

	t.pod.Spec.Containers[0].VolumeMounts = append(t.pod.Spec.Containers[0].VolumeMounts, volumeMount)

	volume := v1.Volume{
		Name: name,
		VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvc.Name,
			},
		},
	}

	t.pod.Spec.Volumes = append(t.pod.Spec.Volumes, volume)
}

func (t *TestPod) SetNodeSelector(nodeSelector map[string]string) {
	t.pod.Spec.NodeSelector = nodeSelector
}

func (t *TestPod) ListNodes(ctx context.Context) []string {
	var err error
	var nodes *v1.NodeList
	var nodeNames []string
	nodes, err = t.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err)
	for _, item := range nodes.Items {
		nodeNames = append(nodeNames, item.ObjectMeta.Name)
	}
	return nodeNames
}

func (t *TestPod) SetNodeUnschedulable(ctx context.Context, nodeName string, unschedulable bool) {
	var err error
	var node *v1.Node
	node, err = t.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	framework.ExpectNoError(err)
	node.Spec.Unschedulable = unschedulable
	_, err = t.client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	framework.ExpectNoError(err)
}

func (t *TestPod) Cleanup(ctx context.Context) {
	cleanupPodOrFail(ctx, t.client, t.pod.Name, t.namespace.Name)
}

func (t *TestPod) GetZoneForVolume(ctx context.Context, index int) string {
	pvcSource := t.pod.Spec.Volumes[index].VolumeSource.PersistentVolumeClaim
	if pvcSource == nil {
		return ""
	}

	pvc, err := t.client.CoreV1().PersistentVolumeClaims(t.namespace.Name).Get(ctx, pvcSource.ClaimName, metav1.GetOptions{})
	framework.ExpectNoError(err)

	pv, err := t.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	framework.ExpectNoError(err)

	zone := ""
	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, ex := range term.MatchExpressions {
			if ex.Key == "topology.disk.csi.azure.com/zone" && ex.Operator == v1.NodeSelectorOpIn {
				zone = ex.Values[0]
			}
		}
	}

	return zone
}

func (t *TestPod) Logs(ctx context.Context) ([]byte, error) {
	return podLogs(ctx, t.client, t.pod.Name, t.namespace.Name)
}

func cleanupPodOrFail(ctx context.Context, client clientset.Interface, name, namespace string) {
	framework.Logf("deleting Pod %q/%q", namespace, name)
	body, err := podLogs(ctx, client, name, namespace)
	if err != nil {
		framework.Logf("Error getting logs for pod %s: %v", name, err)
	} else {
		framework.Logf("Pod %s has the following logs: %s", name, body)
	}
	e2epod.DeletePodOrFail(ctx, client, namespace, name)
}

func podLogs(ctx context.Context, client clientset.Interface, name, namespace string) ([]byte, error) {
	return client.CoreV1().Pods(namespace).GetLogs(name, &v1.PodLogOptions{}).Do(ctx).Raw()
}

// waitForPersistentVolumeClaimDeleted waits for a PersistentVolumeClaim to be removed from the system until timeout occurs, whichever comes first.
func waitForPersistentVolumeClaimDeleted(ctx context.Context, c clientset.Interface, ns string, pvcName string, Poll, timeout time.Duration) error {
	framework.Logf("Waiting up to %v for PersistentVolumeClaim %s to be removed", timeout, pvcName)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(Poll) {
		_, err := c.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			if apierrs.IsNotFound(err) {
				framework.Logf("Claim %q in namespace %q doesn't exist in the system", pvcName, ns)
				return nil
			}
			framework.Logf("Failed to get claim %q in namespace %q, retrying in %v. Error: %v", pvcName, ns, Poll, err)
		}
	}
	return fmt.Errorf("PersistentVolumeClaim %s is not removed from the system within %v", pvcName, timeout)
}

func ListNodeNames(ctx context.Context, c clientset.Interface) []string {
	var nodeNames []string
	nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err)
	for _, item := range nodes.Items {
		nodeNames = append(nodeNames, item.ObjectMeta.Name)
	}
	return nodeNames
}

func getWinImageTag(winServerVer string) string {
	switch winServerVer {
	case "windows-2022":
		return "ltsc2022"
	case "windows-2025":
		return "ltsc2025"
	default:
		return "ltsc2019"
	}
}

func pollForStringWorker(namespace string, pod string, command []string, expectedString string, ch chan<- error) {
	args := append([]string{"exec", pod, "--"}, command...)
	err := wait.PollImmediate(poll, pollForStringTimeout, func() (bool, error) {
		stdout, err := e2ekubectl.RunKubectl(namespace, args...)
		if err != nil {
			framework.Logf("Error waiting for output %q in pod %q: %v.", expectedString, pod, err)
			return false, nil
		}
		if !strings.Contains(stdout, expectedString) {
			framework.Logf("The stdout did not contain output %q in pod %q, found: %q.", expectedString, pod, stdout)
			return false, nil
		}
		return true, nil
	})
	ch <- err
}

// Execute the command for all pods in the namespace, looking for expectedString in stdout
func pollForStringInPodsExec(namespace string, pods []string, command []string, expectedString string) {
	ch := make(chan error, len(pods))
	for _, pod := range pods {
		go pollForStringWorker(namespace, pod, command, expectedString, ch)
	}
	errs := make([]error, 0, len(pods))
	for range pods {
		errs = append(errs, <-ch)
	}
	framework.ExpectNoError(utilerrors.NewAggregate(errs), "Failed to find %q in at least one pod's output.", expectedString)
}

func (t *TestPod) SetAffinity(affinity *v1.Affinity) {
	t.pod.Spec.Affinity = affinity
}

func (t *TestPod) SetLabel(labels map[string]string) {
	t.pod.ObjectMeta.Labels = labels
}
