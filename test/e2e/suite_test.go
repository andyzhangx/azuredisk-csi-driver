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
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	"github.com/onsi/gomega"
	"github.com/pborman/uuid"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/config"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azuredisk"
	"sigs.k8s.io/azuredisk-csi-driver/test/e2e/driver"
	"sigs.k8s.io/azuredisk-csi-driver/test/utils/azure"
	"sigs.k8s.io/azuredisk-csi-driver/test/utils/credentials"
	"sigs.k8s.io/azuredisk-csi-driver/test/utils/testutil"
)

const (
	kubeconfigEnvVar = "KUBECONFIG"
	reportDirEnv     = "ARTIFACTS"
	defaultReportDir = "/workspace/_artifacts"
)

var azurediskDriver *azuredisk.Driver

// handleFlags sets up all flags and parses the command line.
func handleFlags() {
	config.CopyFlags(config.Flags, flag.CommandLine)
	framework.RegisterCommonFlags(flag.CommandLine)
	framework.RegisterClusterFlags(flag.CommandLine)
	flag.Parse()
}

var _ = ginkgo.BeforeSuite(func() {
	// k8s.io/kubernetes/test/e2e/framework requires env KUBECONFIG to be set
	// it does not fall back to defaults
	if os.Getenv(kubeconfigEnvVar) == "" {
		kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
		os.Setenv(kubeconfigEnvVar, kubeconfig)
	}
	handleFlags()
	framework.AfterReadingAllFlags(&framework.TestContext)

	// Default storage driver configuration is CSI. Freshly built
	// CSI driver is installed for that case.
	if os.Getenv(driver.AzureDriverNameVar) == "" && testutil.IsRunningInProw() {
		creds, err := credentials.CreateAzureCredentialFile(false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		azureClient, err := azure.GetAzureClient(creds.Cloud, creds.SubscriptionID, creds.AADClientID, creds.TenantID, creds.AADClientSecret)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		_, err = azureClient.EnsureResourceGroup(context.Background(), creds.ResourceGroup, creds.Location, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Need to login to ACR using SP credential if we are running in Prow so we can push test images.
		// If running locally, user should run 'docker login' before running E2E tests

		registry := os.Getenv("REGISTRY")
		gomega.Expect(registry).NotTo(gomega.Equal(""))

		log.Println("Attempting docker login with Azure service principal")
		cmd := exec.Command("docker", "login", fmt.Sprintf("--username=%s", creds.AADClientID), fmt.Sprintf("--password=%s", creds.AADClientSecret), registry)
		err = cmd.Run()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		log.Println("docker login is successful")

		// Install Azure Disk CSI Driver on cluster from project root
		err = os.Chdir("../..")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := os.Chdir("test/e2e")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		projectRoot, err := os.Getwd()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(strings.HasSuffix(projectRoot, "azuredisk-csi-driver")).To(gomega.Equal(true))

		log.Println("Installing Azure Disk CSI Driver...")
		cmd = exec.Command("make", "e2e-bootstrap")
		cmd.Dir = projectRoot
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err = cmd.Run()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		log.Println("Azure Disk CSI Driver installed")

		nodeid := os.Getenv("nodeid")
		azurediskDriver = azuredisk.NewDriver(nodeid)
		go func() {
			os.Setenv("AZURE_CREDENTIAL_FILE", credentials.TempAzureCredentialFilePath)
			azurediskDriver.Run(fmt.Sprintf("unix:///tmp/csi-%s.sock", uuid.NewUUID().String()))
		}()
	}
})

var _ = ginkgo.AfterSuite(func() {
	if os.Getenv(driver.AzureDriverNameVar) == "" && testutil.IsRunningInProw() {
		err := os.Chdir("../..")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			err := os.Chdir("test/e2e")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		projectRoot, err := os.Getwd()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(strings.HasSuffix(projectRoot, "azuredisk-csi-driver")).To(gomega.Equal(true))

		log.Println("===================azuredisk log===================")
		cmdSh := exec.Command("sh", "test/utils/azuredisk_log.sh")
		cmdSh.Dir = projectRoot
		cmdSh.Stdout = os.Stdout
		cmdSh.Stderr = os.Stderr
		err = cmdSh.Run()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		log.Println("===================================================")

		log.Println("Uninstalling Azure Disk CSI Driver...")
		cmd := exec.Command("make", "e2e-teardown")
		cmd.Dir = projectRoot
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err = cmd.Run()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		log.Println("Azure Disk CSI Driver uninstalled")

		err = credentials.DeleteAzureCredentialFile()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}
})

func TestE2E(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	reportDir := os.Getenv(reportDirEnv)
	if reportDir == "" {
		reportDir = defaultReportDir
	}
	r := []ginkgo.Reporter{reporters.NewJUnitReporter(path.Join(reportDir, "junit_01.xml"))}
	ginkgo.RunSpecsWithDefaultAndCustomReporters(t, "AzureDisk CSI Driver End-to-End Tests", r)
}
