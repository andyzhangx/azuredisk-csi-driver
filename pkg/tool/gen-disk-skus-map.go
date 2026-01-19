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

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"k8s.io/klog/v2"

	"sigs.k8s.io/azuredisk-csi-driver/pkg/optimization"
)

var (
	MaxValueOfMaxSharesCapability        = "maxvalueofmaxshares"
	MaxBurstIopsCapability               = "maxburstiops"
	MaxIOpsCapability                    = "maxiops"
	MaxBandwidthMBpsCapability           = "maxbandwidthmbps"
	MaxBurstBandwidthMBpsCapability      = "maxburstbandwidthmbps"
	MaxSizeGiBCapability                 = "maxsizegib"
	UncachedDiskIOPSCapability           = "uncacheddiskiops"
	UncachedDiskBytesPerSecondCapability = "uncacheddiskbytespersecond"
	MaxDataDiskCountCapability           = "maxdatadiskcount"
	VCPUsCapability                      = "vcpus"
	SkusFullfilePath                     = "skus-full.json"
)

func init() {
	klog.InitFlags(nil)
}

// exit is a separate function to handle program termination
var exit = func(code int) {
	os.Exit(code)
}

func main() {

	boilerPlate := `/*
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
	
package optimization
	
import (
)`
	var sb strings.Builder
	diskMapStart := "	DiskSkuMap = map[string]map[string]DiskSkuInfo{"
	diskMapEnd := "	}"
	accStart := `	"%s": map[string]DiskSkuInfo {`
	accEnd := "		},"
	diskSku := `		"%s": DiskSkuInfo{StorageAccountType: "%s", StorageTier: "%s", DiskSize: "%s", MaxAllowedShares: %s, MaxBurstIops: %s, MaxIops: %s, MaxBwMbps: %s, MaxBurstBwMbps: %s, MaxSizeGiB: %s},`

	nodeMapStart := "	NodeInfoMap = map[string]NodeInfo{"
	nodeInfo := `	"%s": NodeInfo{SkuName: "%s", MaxDataDiskCount: %s, VCpus: %s, MaxBurstIops: %s, MaxIops: %s, MaxBwMbps: %s, MaxBurstBwMbps: %s},`
	nodeMapEnd := "	}"

	defer os.Remove(SkusFullfilePath)
	skusFilePath := "pkg/azuredisk/azure_skus_map.go"

	skuMap := map[string]bool{}

	var resources []*armcompute.ResourceSKU

	if err := getAllSkus(); err != nil {
		klog.Errorf("Could not get skus. Error: %v", err)
	}

	diskSkusFullJSON, err := os.Open(SkusFullfilePath)
	if err != nil {
		klog.Errorf("Could not read file. Error: %v, FilePath: %s", err, SkusFullfilePath)
		return
	}
	defer diskSkusFullJSON.Close()

	byteValue, _ := io.ReadAll(diskSkusFullJSON)

	err = json.Unmarshal([]byte(byteValue), &resources)
	if err != nil {
		klog.Errorf("Could not parse json file file. Error: %v, FilePath: %s", err, SkusFullfilePath)
		return
	}

	diskSkuInfoMap := map[string]map[string]optimization.DiskSkuInfo{}
	vmSkuInfoMap := map[string]optimization.NodeInfo{}

	for _, sku := range resources {
		resType := strings.ToLower(*sku.ResourceType)
		skuKey := ""
		if resType == "disks" {
			skuKey = fmt.Sprintf("%s-%s-%s", *sku.Name, *sku.Tier, *sku.Size)
		} else if resType == "virtualmachines" {
			skuKey = *sku.Name
		} else {
			continue
		}
		// If we already added the sku, skip
		skuKeyLower := strings.ToLower(skuKey)
		if _, ok := skuMap[skuKeyLower]; ok {
			continue
		}
		skuMap[skuKeyLower] = true
		if resType == "disks" {
			account := strings.ToLower(*sku.Name)
			diskSize := strings.ToLower(*sku.Size)
			if _, ok := diskSkuInfoMap[account]; !ok {
				diskSkuInfoMap[account] = map[string]optimization.DiskSkuInfo{}
			}
			diskSkuInfoMap[account][diskSize], err = getDiskCapabilities(sku)
			if err != nil {
				klog.Errorf("populateSkuMap: Failed to get disk capabilities for disk %s %s %s. Error: %v", *sku.Name, *sku.Size, *sku.Tier, err)
				exit(1)
			}
		} else if resType == "virtualmachines" {
			nodeInfo := optimization.NodeInfo{}
			nodeInfo.SkuName = *sku.Name
			err = populateNodeCapabilities(sku, &nodeInfo)
			if err != nil {
				klog.Errorf("populateSkuMap: Failed to populate node capabilities. Error: %v", err)
				exit(1)
			}
			vmSkuInfoMap[strings.ToLower(*sku.Name)] = nodeInfo
		}
	}

	// Write the boiler plate stuff
	appendWithErrCheck(&sb, boilerPlate)
	appendWithErrCheck(&sb, "\n")
	appendWithErrCheck(&sb, "\n")
	appendWithErrCheck(&sb, "var (")
	appendWithErrCheck(&sb, "\n")

	// Write the disk map
	appendWithErrCheck(&sb, diskMapStart)
	for account, sizes := range diskSkuInfoMap {
		appendWithErrCheck(&sb, "\n")
		appendWithErrCheck(&sb, fmt.Sprintf(accStart, account))

		for size, sku := range sizes {
			//diskSku := `"%s": DiskSkuInfo{storageAccountType: "%s", storageTier: "%s", diskSize: "%s",
			//maxAllowedShares: %s, maxBurstIops: %s, maxIops: %s, maxBwMbps: %s, maxBurstBwMbps: %s, maxSizeGiB: %s},`
			appendWithErrCheck(&sb, "\n")
			appendWithErrCheck(&sb, fmt.Sprintf(diskSku, size, sku.StorageAccountType, sku.StorageTier, sku.DiskSize, strconv.Itoa(sku.MaxAllowedShares),
				strconv.Itoa(sku.MaxBurstIops), strconv.Itoa(sku.MaxIops), strconv.Itoa(sku.MaxBwMbps), strconv.Itoa(sku.MaxBurstBwMbps), strconv.Itoa(sku.MaxSizeGiB)))
		}
		appendWithErrCheck(&sb, "\n")
		appendWithErrCheck(&sb, accEnd)
	}

	appendWithErrCheck(&sb, "\n")
	appendWithErrCheck(&sb, diskMapEnd)
	appendWithErrCheck(&sb, "\n")
	appendWithErrCheck(&sb, "\n")

	// Write the VM Sku map
	appendWithErrCheck(&sb, nodeMapStart)

	//nodeInfo := `	"%s": NodeInfo{skuName: "%s", maxDataDiskCount: %s, vcpus: %s, maxBurstIops: %s, maxIops: %s, maxBwMbps: %s, maxBurstBwMbps: %s},`
	for vm, sku := range vmSkuInfoMap {
		appendWithErrCheck(&sb, "\n")
		appendWithErrCheck(&sb, fmt.Sprintf(nodeInfo, vm, sku.SkuName, strconv.Itoa(sku.MaxDataDiskCount), strconv.Itoa(sku.VCpus),
			strconv.Itoa(sku.MaxBurstIops), strconv.Itoa(sku.MaxIops), formatInt(sku.MaxBwMbps), formatInt(sku.MaxBurstBwMbps)))
	}
	appendWithErrCheck(&sb, "\n")
	appendWithErrCheck(&sb, nodeMapEnd)
	appendWithErrCheck(&sb, "\n")
	appendWithErrCheck(&sb, ")")
	appendWithErrCheck(&sb, "\n")

	err = os.WriteFile(skusFilePath, []byte(sb.String()), 0644)
	if err != nil {
		klog.Errorf("Could write file. Error: %v, FilePath: %s", err, skusFilePath)
	}
	klog.Info("Wrote to file ", skusFilePath)
}

func formatInt(value int) string {
	if value <= 0 {
		return "0"
	}

	return strconv.Itoa(value)
}
func getAllSkus() (err error) {
	if os.Getenv("TEST_SCENARIO") == "true" {
		if _, err := os.Stat("skus-sample.json"); err == nil {
			SkusFullfilePath = "skus-sample.json"
			return nil
		}
	}
	klog.V(2).Infof("Getting skus and writing to %s", SkusFullfilePath)
	cmd := exec.Command("az", "vm", "list-skus")
	outfile, err := os.Create(SkusFullfilePath)
	var errorBuf bytes.Buffer
	if err != nil {
		klog.Errorf("Could not create file. Error: %v File: %s", err, SkusFullfilePath)
		return err
	}
	defer outfile.Close()

	cmd.Stdout = outfile
	cmd.Stderr = &errorBuf

	err = cmd.Start()
	if err != nil {
		return err
	}
	err = cmd.Wait()
	if err != nil {
		klog.Errorf("Could not get skus. ExitCode: %v Error: %s", err, errorBuf.String())
	}
	return err
}

// populateNodeCapabilities populates node capabilities from SkuInfo
func populateNodeCapabilities(sku *armcompute.ResourceSKU, nodeInfo *optimization.NodeInfo) (err error) {
	if sku.Capabilities != nil {
		for _, capability := range sku.Capabilities {
			err = nil
			if capability.Name != nil {
				switch strings.ToLower(*capability.Name) {
				case UncachedDiskIOPSCapability:
					nodeInfo.MaxIops, err = strconv.Atoi(*capability.Value)
				case UncachedDiskBytesPerSecondCapability:
					bw, err := strconv.Atoi(*capability.Value)
					nodeInfo.MaxBwMbps = bw / (1024 * 1024)
					if err != nil {
						return fmt.Errorf("PopulateNodeCapabilities: Failed to parse node capability %s. Error: %v", *capability.Name, err)
					}
				case MaxDataDiskCountCapability:
					nodeInfo.MaxDataDiskCount, err = strconv.Atoi(*capability.Value)
				case VCPUsCapability:
					nodeInfo.VCpus, err = strconv.Atoi(*capability.Value)
				default:
					continue
				}
			}

			if err != nil {
				return fmt.Errorf("PopulateNodeCapabilities: Failed to parse node capability %s. Error: %v", *capability.Name, err)
			}
		}
	}

	// If node doesn't support burst capabilities.
	// Set the burst limits as regular limits
	if nodeInfo.MaxBurstIops < nodeInfo.MaxIops {
		nodeInfo.MaxBurstIops = nodeInfo.MaxIops
	}

	if nodeInfo.MaxBurstBwMbps < nodeInfo.MaxBwMbps {
		nodeInfo.MaxBurstBwMbps = nodeInfo.MaxBwMbps
	}

	return nil
}

// getDiskCapabilities gets disk capabilities from SkuInfo
func getDiskCapabilities(sku *armcompute.ResourceSKU) (diskSku optimization.DiskSkuInfo, err error) {
	diskSku = optimization.DiskSkuInfo{}
	diskSku.StorageAccountType = *sku.Name
	diskSku.StorageTier = *sku.Tier
	diskSku.DiskSize = *sku.Size

	if sku.Capabilities != nil {
		for _, capability := range sku.Capabilities {
			err = nil
			if capability.Name != nil {
				switch strings.ToLower(*capability.Name) {
				case MaxValueOfMaxSharesCapability:
					diskSku.MaxAllowedShares, err = strconv.Atoi(*capability.Value)
				case MaxBurstIopsCapability:
					diskSku.MaxBurstIops, err = strconv.Atoi(*capability.Value)
				case MaxIOpsCapability:
					diskSku.MaxIops, err = strconv.Atoi(*capability.Value)
				case MaxBandwidthMBpsCapability:
					diskSku.MaxBwMbps, err = strconv.Atoi(*capability.Value)
				case MaxBurstBandwidthMBpsCapability:
					diskSku.MaxBurstBwMbps, err = strconv.Atoi(*capability.Value)
				case MaxSizeGiBCapability:
					diskSku.MaxSizeGiB, err = strconv.Atoi(*capability.Value)
				default:
					continue
				}
			}

			if err != nil {
				return diskSku, fmt.Errorf("GetDiskCapabilities: Failed to parse disk capability %s. Error: %v", *capability.Name, err)
			}
		}
	}

	// If disk doesn't support burst capabilities.
	// Set the burst limits as regular limits
	if diskSku.MaxBurstIops < diskSku.MaxIops {
		diskSku.MaxBurstIops = diskSku.MaxIops
	}

	if diskSku.MaxBurstBwMbps < diskSku.MaxBwMbps {
		diskSku.MaxBurstBwMbps = diskSku.MaxBwMbps
	}

	return diskSku, nil
}

func appendWithErrCheck(sb *strings.Builder, strToAppend string) {
	if _, err := sb.WriteString(strToAppend); err != nil {
		panic(err)
	}
}
