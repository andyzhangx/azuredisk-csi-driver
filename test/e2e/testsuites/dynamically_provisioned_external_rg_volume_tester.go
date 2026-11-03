/*
Copyright 2022 The Kubernetes Authors.

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
	"fmt"
	"strconv"
	"strings"

	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
	"sigs.k8s.io/azuredisk-csi-driver/test/e2e/driver"
	"sigs.k8s.io/azuredisk-csi-driver/test/utils/azure"
	"sigs.k8s.io/azuredisk-csi-driver/test/utils/credentials"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/uuid"

	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

// DynamicallyProvisionedExternalRgVolumeTest will provision required StorageClass(es), PVC(s) and Pod(s) in an external resource group
// Waiting for the PV provisioner to create a new PV
// Testing if the Pod(s) Cmd is run with a 0 exit code
type DynamicallyProvisionedExternalRgVolumeTest struct {
	CSIDriver              driver.DynamicPVTestDriver
	Pod                    PodDetails
	StorageClassParameters map[string]string
	SeparateResourceGroups bool
}

func (t *DynamicallyProvisionedExternalRgVolumeTest) Run(ctx context.Context, client clientset.Interface, namespace *v1.Namespace) {
	tpod := NewTestPod(client, namespace, t.Pod.Cmd, false, "")
	ginkgo.By("Checking Prow test resource group")
	creds, err := credentials.CreateAzureCredentialFile()
	framework.ExpectNoError(err, fmt.Sprintf("Error getting creds for AzurePublicCloud %v", err))
	defer func() {
		err := credentials.DeleteAzureCredentialFile()
		framework.ExpectNoError(err)
	}()

	ginkgo.By("Prow test resource group: " + creds.ResourceGroup)
	azureClient, err := azure.GetAzureClient(creds.Cloud, creds.SubscriptionID, creds.AADClientID, creds.TenantID, creds.AADClientSecret, creds.AADFederatedTokenFile)
	framework.ExpectNoError(err)
	var externalRG string
	var externalRGList []string
	type externalClaim struct {
		pvc           *TestPersistentVolumeClaim
		resourceGroup string
	}
	var claims []externalClaim
	defer func() {
		for _, rgName := range externalRGList {
			// Only delete resource group the test created
			if strings.HasPrefix(rgName, credentials.ResourceGroupPrefix) {
				framework.Logf("Deleting resource group %s", rgName)
				err := azureClient.DeleteResourceGroup(ctx, rgName)
				framework.ExpectNoError(err)
			}
		}
	}()

	for i, volume := range t.Pod.Volumes {
		storageClassParams := map[string]string{}

		for k, v := range t.StorageClassParameters {
			storageClassParams[k] = v
		}

		if t.SeparateResourceGroups || externalRG == "" {
			//create external resource group
			externalRG = credentials.ResourceGroupPrefix + string(uuid.NewUUID())
			ginkgo.By("Creating external resource group: " + externalRG)
			_, err = azureClient.EnsureResourceGroup(ctx, externalRG, creds.Location, nil)
			framework.ExpectNoError(err)
			externalRGList = append(externalRGList, externalRG)
		}
		storageClassParams["resourceGroup"] = externalRG

		ginkgo.By("creating volume in external rg " + externalRG)
		tpvc, pvcCleanup := volume.SetupDynamicPersistentVolumeClaim(ctx, client, namespace, t.CSIDriver, storageClassParams)
		for i := range pvcCleanup {
			defer pvcCleanup[i](ctx)
		}
		claims = append(claims, externalClaim{pvc: tpvc, resourceGroup: externalRG})
		tpod.SetupVolume(tpvc.persistentVolumeClaim, volume.VolumeMount.NameGenerate+strconv.Itoa(i+1), volume.VolumeMount.MountPathGenerate+strconv.Itoa(i+1), volume.VolumeMount.ReadOnly)
	}

	ginkgo.By("deploying the pod")
	tpod.Create(ctx)
	defer tpod.Cleanup(ctx)
	ginkgo.By("checking that the pod's command exits with no error")
	tpod.WaitForSuccess(ctx)

	ginkgo.By("verifying each disk was provisioned in its requested external resource group")
	for _, claim := range claims {
		claim.pvc.WaitForBound(ctx)
		claim.pvc.ValidateProvisionedPersistentVolume(ctx)
		pv := claim.pvc.persistentVolume
		gomega.Expect(pv.Spec.CSI).NotTo(gomega.BeNil(), "external resource group test requires a CSI PV")
		diskURI := pv.Spec.CSI.VolumeHandle
		subscriptionID, resourceGroup, _, err := azureutils.GetInfoFromURI(diskURI)
		framework.ExpectNoError(err, "parsing disk URI for PV %s", pv.Name)
		gomega.Expect(strings.EqualFold(subscriptionID, creds.SubscriptionID)).To(gomega.BeTrue(),
			"disk %s must be in subscription %s", diskURI, creds.SubscriptionID)
		gomega.Expect(strings.EqualFold(resourceGroup, claim.resourceGroup)).To(gomega.BeTrue(),
			"disk %s must be in external resource group %s", diskURI, claim.resourceGroup)
	}
}
