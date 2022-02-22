//go:build linux
// +build linux

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
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"k8s.io/klog/v2"
)

type DeviceHelper struct{}

const (
	blockDeviceRootPath = "/sys/block"
)

func (deviceHelper *DeviceHelper) DiskSupportsPerfOptimization(diskPerfProfile, diskAccountType string) bool {
	return isPerfTuningEnabled(diskPerfProfile) && accountSupportsPerfOptimization(diskAccountType)
}

// OptimizeDiskPerformance optimizes device performance by setting tuning block device settings
func (deviceHelper *DeviceHelper) OptimizeDiskPerformance(nodeInfo *NodeInfo, devicePath,
	perfProfile, accountType, diskSizeGibStr, diskIopsStr, diskBwMbpsStr string) (err error) {
	klog.V(2).Infof("OptimizeDiskPerformance: Tuning settings for %s", devicePath)

	if nodeInfo == nil {
		return fmt.Errorf("OptimizeDiskPerformance: Node info is not provided. Error: invalid parameter")
	}

	queueDepth, nrRequests, scheduler, maxSectorsKb, readAheadKb, err := getOptimalDeviceSettings(nodeInfo, DiskSkuMap, perfProfile, accountType, diskSizeGibStr, diskIopsStr, diskBwMbpsStr)
	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Failed to get optimal settings for profile %s accountType %s device %s Error: %w", perfProfile, accountType, devicePath, err)
	}

	deviceName, err := getDeviceName(devicePath)
	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Could not get deviceName for %s. Error: %w", devicePath, err)
	}

	klog.V(2).Infof("OptimizeDiskPerformance: Tuning settings for devicePath %s, deviceName %s, profile %s queueDepth %s nrRequests %s scheduler %s maxSectorsKb %s readAheadKb %s",
		devicePath,
		deviceName,
		perfProfile,
		queueDepth,
		nrRequests,
		scheduler,
		maxSectorsKb,
		readAheadKb)

	err = echoToFile(maxSectorsKb, filepath.Join(blockDeviceRootPath, deviceName, "queue/max_sectors_kb"))
	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Could not set max_sectors_kb for device %s. Error: %w", deviceName, err)
	}

	err = echoToFile(scheduler, filepath.Join(blockDeviceRootPath, deviceName, "queue/scheduler"))
	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Could not set queue/scheduler for device %s. Error: %w", deviceName, err)
	}

	err = echoToFile("1", filepath.Join(blockDeviceRootPath, deviceName, "queue/iosched/fifo_batch"))
	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Could not set queue/iosched/fifo_batch for device %s. Error: %w", deviceName, err)
	}

	err = echoToFile("1", filepath.Join(blockDeviceRootPath, deviceName, "queue/iosched/writes_starved"))
	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Could not set queue/iosched/writes_starved for device %s. Error: %w", deviceName, err)
	}

	err = echoToFile(queueDepth, filepath.Join(blockDeviceRootPath, deviceName, "device/queue_depth"))
	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Could not set queue/queue_depth for device %s. Error: %w", deviceName, err)
	}

	err = echoToFile(nrRequests, filepath.Join(blockDeviceRootPath, deviceName, "queue/nr_requests"))
	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Could not set queue/nr_requests for device %s. Error: %w", deviceName, err)
	}

	err = echoToFile(readAheadKb, filepath.Join(blockDeviceRootPath, deviceName, "queue/read_ahead_kb"))
	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Could not set queue/read_ahead_kb for device %s. Error: %w", deviceName, err)
	}

	err = echoToFile("0", filepath.Join(blockDeviceRootPath, deviceName, "queue/wbt_lat_usec"))
	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Could not set queue/wbt_lat_usec for device %s. Error: %w", deviceName, err)
	}

	err = echoToFile("0", filepath.Join(blockDeviceRootPath, deviceName, "queue/rotational"))
	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Could not set queue/rotational for device %s. Error: %w", deviceName, err)
	}

	return err
}

// getDeviceName gets the device name from the device lunpath
// Device lun path is of the format /dev/disk/azure/scsi1/lun0
func getDeviceName(lunPath string) (deviceName string, err error) {
	devicePath, err := filepath.EvalSymlinks(lunPath)
	if err != nil {
		return "", fmt.Errorf("path %s is not a symlink. Error: %w", lunPath, err)
	}

	return filepath.Base(devicePath), nil
}

// echoToFile echos setting value to the file
func echoToFile(content, filePath string) (err error) {
	outfile, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer outfile.Close()

	cmd := exec.Command("echo", content)
	cmd.Stdout = outfile
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Wait()
}

func getOptimalDeviceSettings(nodeInfo *NodeInfo, diskSkus map[string]map[string]DiskSkuInfo, perfProfile, accountType, diskSizeGibStr, diskIopsStr, diskBwMbpsStr string) (queueDepth, nrRequests, scheduler, maxSectorsKb, readAheadKb string, err error) {
	klog.V(12).Infof("Calculating perf optimizations for rofile %s accountType %s diskSize", perfProfile, accountType, diskSizeGibStr)
	iopsHeadRoom := .25
	maxHwSectorsKb := 512.0

	// TODO: Get matching disk SKU
	// In future get the disk SKU using disk size ex. P10, P30 etc
	diskSku, err := getMatchingDiskSku(diskSkus, accountType, diskSizeGibStr, diskIopsStr, diskBwMbpsStr)

	if err != nil || diskSku == nil {
		return queueDepth, nrRequests, scheduler, maxSectorsKb, readAheadKb, fmt.Errorf("could not find sku for account %s size %s. Error: sku not found", accountType, diskSizeGibStr)
	}

	diskIopsFloat := float64(diskSku.MaxBurstIops)
	maxBurstIops := math.Min(diskIopsFloat, float64(nodeInfo.MaxBurstIops))

	// Some VM SKUs don't have IOPS published
	// for such VMs set the properties based on disk IOPs limits
	if maxBurstIops <= 0 {
		maxBurstIops = diskIopsFloat
	}

	diskBwFloat := float64(diskSku.MaxBurstBwMbps)
	// unless otherwise specified, BW Units are in kB (not KB) which is 1000 bytes
	maxBurstBw := math.Min(diskBwFloat, float64(nodeInfo.MaxBurstBwMbps)) * 1000

	// Some VM SKUs don't have BW published
	// for such VMs set the properties based on disk BW limits
	if maxBurstBw <= 0 {
		maxBurstBw = diskBwFloat
	}

	// Adjusted burst IOs possible
	iopsSeqIo := maxBurstIops * (1 - iopsHeadRoom)

	// Request size needed to get max Burst BW, capped at maxHwSectorsKb
	rsMinSeqIo := math.Ceil(math.Min(maxHwSectorsKb, (maxBurstBw / iopsSeqIo)))

	// TODO: dynamically calculate this
	latencyPerIoToGetMaxBwInSec := diskSku.GetSequentialOLatencyInSec()
	latencyForSmallRandOperationInSec := diskSku.GetRandomIOLatencyInSec()

	// queue_depth needed to drive maxBurstBw with IO size rsMinSeqIo and each IO taking latencyPerIoToGetMaxBwInSec
	qdMaxSeqBw := math.Ceil(maxBurstBw * latencyPerIoToGetMaxBwInSec / rsMinSeqIo)

	// queue_depth needed for random IOs to hit IOPS reserved for them
	qdMaxRandomIops := math.Ceil(maxBurstIops * iopsHeadRoom * latencyForSmallRandOperationInSec)

	maxSectorsKb = fmt.Sprintf("%g", rsMinSeqIo)
	scheduler = "mq-deadline"

	qdTotal := fmt.Sprintf("%g", math.Ceil(qdMaxRandomIops+qdMaxSeqBw))
	queueDepth = qdTotal
	nrRequests = qdTotal

	readAheadKb = fmt.Sprintf("%g", math.Ceil(qdMaxSeqBw*rsMinSeqIo))

	klog.V(2).Infof("Returning perf attributes for perfProfile %s accountType %s queueDepth %s nrRequests %s scheduler %s maxSectorsKb %s readAheadKb %s",
		perfProfile,
		accountType,
		queueDepth,
		nrRequests,
		scheduler,
		maxSectorsKb,
		readAheadKb)
	return queueDepth, nrRequests, scheduler, maxSectorsKb, readAheadKb, err
}

// getMatchingDiskSku gets the smallest SKU which matches the size, io and bw requirement
// TODO: Query the disk size (e.g. P10, P30 etc) and use that to find the sku
func getMatchingDiskSku(diskSkus map[string]map[string]DiskSkuInfo, accountType, diskSizeGibStr, diskIopsStr, diskBwMbpsStr string) (matchingSku *DiskSkuInfo, err error) {
	accountTypeLower := strings.ToLower(accountType)
	skus, ok := diskSkus[accountTypeLower]

	if !ok || skus == nil || len(diskSkus[accountTypeLower]) <= 0 {
		return nil, fmt.Errorf("could not find sku for account %s. Error: sku not found", accountType)
	}

	diskSizeGb, err := strconv.Atoi(diskSizeGibStr)
	if err != nil {
		return nil, fmt.Errorf("could not parse disk size %s. Error: incorrect sku size", diskSizeGibStr)
	}

	// Treating these as non required field, as they come as part of the provisioned size
	// If these are explicitly set then that will be used to get the best possible match
	diskIops, err := strconv.Atoi(diskIopsStr)
	if err != nil {
		diskIops = 0
	}
	diskBwMbps, err := strconv.Atoi(diskBwMbpsStr)
	if err != nil {
		diskBwMbps = 0
	}

	for _, sku := range diskSkus[accountTypeLower] {
		// Use the smallest sku size which can fulfil Size, IOs and BW requirements
		if meetsRequest(&sku, diskSizeGb, diskIops, diskBwMbps) {
			if matchingSku == nil || sku.MaxSizeGiB < matchingSku.MaxSizeGiB {
				tempSku := sku
				matchingSku = &tempSku
			}
		}
	}

	return matchingSku, nil
}

// meetsRequest checks to see if given SKU meets\has enough size, iops and bw limits
func meetsRequest(sku *DiskSkuInfo, diskSizeGb, diskIops, diskBwMbps int) bool {
	if sku == nil {
		return false
	}

	if sku.MaxSizeGiB >= diskSizeGb && sku.MaxBwMbps >= diskBwMbps && sku.MaxIops >= diskIops {
		return true
	}

	return false
}
