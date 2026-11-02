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

package testsuites

import (
	"context"

	"sigs.k8s.io/azuredisk-csi-driver/test/e2e/driver"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

// DynamicallyProvisionedCollocatedPodTest will provision required StorageClass(es), PVC(s) and Pod(s)
// Waiting for the PV provisioner to create a new PV
// Testing if multiple Pod(s) can write simultaneously
type DynamicallyProvisionedCollocatedPodTest struct {
	CSIDriver              driver.DynamicPVTestDriver
	Pods                   []PodDetails
	ColocatePods           bool
	StorageClassParameters map[string]string
}

func (t *DynamicallyProvisionedCollocatedPodTest) Run(ctx context.Context, client clientset.Interface, namespace *v1.Namespace) {
	nodeName := ""
	for _, pod := range t.Pods {
		tpod, cleanup := pod.SetupWithDynamicVolumes(ctx, client, namespace, t.CSIDriver, t.StorageClassParameters)
		if t.ColocatePods && nodeName != "" {
			// Keep scheduling enabled so WaitForFirstConsumer volumes can bind.
			tpod.SetAffinity(&v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchFields: []v1.NodeSelectorRequirement{
									{Key: "metadata.name", Operator: v1.NodeSelectorOpIn, Values: []string{nodeName}},
								},
							},
						},
					},
				},
			})
		}
		// defer must be called here for resources not get removed before using them
		for i := range cleanup {
			defer cleanup[i](ctx)
		}

		ginkgo.By("deploying the pod")
		tpod.Create(ctx)
		defer tpod.Cleanup(ctx)

		ginkgo.By("checking that the pod is running")
		tpod.WaitForRunningLong(ctx)
		runningPod, err := client.CoreV1().Pods(namespace.Name).Get(ctx, tpod.pod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "getting scheduled pod %s", tpod.pod.Name)
		gomega.Expect(runningPod.Spec.NodeName).NotTo(gomega.BeEmpty(), "running pod must have an assigned node")
		if t.ColocatePods && nodeName != "" {
			gomega.Expect(runningPod.Spec.NodeName).To(gomega.Equal(nodeName), "all colocated pods must run on the first pod's node")
		}
		if nodeName == "" {
			nodeName = runningPod.Spec.NodeName
		}
	}
}
