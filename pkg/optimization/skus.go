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

package optimization

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/apimachinery/pkg/types"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
)

// NodeInfo stores VM/Node specific static information
// VM information is present in sku.json in below format
// {
// 	"resourceType": "virtualmachines",
// 	"name": "Standard_E16-4ds_v4",
// 	"tier": "Standard",
// 	"size": "E16-4ds_v4",
// 	"capabilities": [
// 	 {
// 	  "name": "vCPUs",
// 	  "value": "16"
// 	 },
// 	 {
// 	  "name": "MemoryGB",
// 	  "value": "128"
// 	 },
// 	 {
// 	  "name": "MaxDataDiskCount",
// 	  "value": "32"
// 	 },
// 	 {
// 	  "name": "UncachedDiskIOPS",
// 	  "value": "25600"
// 	 },
// 	 {
// 	  "name": "UncachedDiskBytesPerSecond",
// 	  "value": "402653184"
// 	 }
// 	]
// }
type NodeInfo struct {
	SkuName          string
	Zone             string
	Region           string
	MaxDataDiskCount int
	VCpus            int
	MaxBurstIops     int
	MaxIops          int
	MaxBwMbps        int
	MaxBurstBwMbps   int
}

// DiskSkuInfo stores disk sku information
// disk sku information is present in sku.json in below format
// {
// 	"resourceType": "disks",
// 	"name": "Premium_LRS",
// 	"tier": "Premium",
// 	"size": "P4",
// 	"capabilities": [
// 	 {
// 	  "name": "MaxSizeGiB",
// 	  "value": "32"
// 	 {
// 	  "name": "MaxIOps",
// 	  "value": "120"
// 	 },
// 	 {
// 	  "name": "MaxBandwidthMBps",
// 	  "value": "25"
// 	 },
// 	 {
// 	  "name": "MaxValueOfMaxShares",
// 	  "value": "1"
// 	 },
// 	 {
// 	  "name": "MaxBurstIops",
// 	  "value": "3500"
// 	 },
// 	 {
// 	  "name": "MaxBurstBandwidthMBps",
// 	  "value": "170"
// 	 }
// 	]
//}
type DiskSkuInfo struct {
	StorageAccountType string
	StorageTier        string
	DiskSize           string
	MaxAllowedShares   int
	MaxBurstIops       int
	MaxIops            int
	MaxBwMbps          int
	MaxBurstBwMbps     int
	MaxSizeGiB         int
}

// NewNodeInfo populates Node and Sku related information in memory
func NewNodeInfo(cloud cloudprovider.Interface, nodeID string) (*NodeInfo, error) {
	klog.V(2).Infof("NewNodeInfo: Starting to populate node and disk sku information.")

	instances, ok := cloud.Instances()
	if !ok {
		return nil, status.Error(codes.Internal, "NewNodeInfo: Failed to get instances from Azure cloud provider")
	}

	instanceType, err := instances.InstanceType(context.TODO(), types.NodeName(nodeID))
	if err != nil {
		return nil, fmt.Errorf("NewNodeInfo: Failed to get instance type from Azure cloud provider, nodeName: %v, error: %w", nodeID, err)
	}

	zones, ok := cloud.Zones()
	if !ok {
		return nil, status.Error(codes.Internal, "NewNodeInfo: Failed to get zones from Azure cloud provider.")
	}
	zone, err := zones.GetZone(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("NewNodeInfo: Failed to get zone from Azure cloud provider, nodeName: %v, error: %w", nodeID, err)
	}

	nodeInfo := &NodeInfo{}
	nodeInfo.SkuName = instanceType
	nodeInfo.Zone = zone.FailureDomain
	nodeInfo.Region = zone.Region

	nodeSkuNameLower := strings.ToLower(nodeInfo.SkuName)

	vmSku, ok := NodeInfoMap[nodeSkuNameLower]
	if !ok {
		return nil, fmt.Errorf("NewNodeInfo: Could not find SKU %s in the sku map", nodeSkuNameLower)
	}

	nodeInfo.MaxBurstBwMbps = vmSku.MaxBurstBwMbps
	nodeInfo.MaxBurstIops = vmSku.MaxBurstIops
	nodeInfo.MaxBwMbps = vmSku.MaxBwMbps
	nodeInfo.MaxIops = vmSku.MaxIops
	nodeInfo.MaxDataDiskCount = vmSku.MaxDataDiskCount
	nodeInfo.VCpus = vmSku.VCpus

	return nodeInfo, nil
}

func GetDiskSkuInfoMap() map[string]map[string]DiskSkuInfo {
	return DiskSkuMap
}

// GetRandomIOLatencyInSec gets the estimated random IP latency for a small write for a disk size
// These latencies are manually calculated and stored
// ToDo: Make this estimation dynamic
func (sku *DiskSkuInfo) GetRandomIOLatencyInSec() float64 {
	if sku.MaxSizeGiB <= 4096 {
		return 0.0022
	}

	if sku.MaxSizeGiB <= 8192 {
		return 0.0028
	}

	if sku.MaxSizeGiB <= 16384 {
		return 0.0034
	}

	return 0.004
}

// GetSequentialOLatencyInSec gets the estimated sequential IO latency for a disk size
// These latencies are manually calculated and stored
// ToDo: Make this estimation dynamic
func (sku *DiskSkuInfo) GetSequentialOLatencyInSec() float64 {
	if sku.MaxSizeGiB <= 4096 {
		return 0.0033
	}

	if sku.MaxSizeGiB <= 8192 {
		return 0.0041
	}

	if sku.MaxSizeGiB <= 16384 {
		return 0.0046
	}

	return 0.0052
}
