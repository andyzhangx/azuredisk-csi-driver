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

	consts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
)

type DeviceHelper struct {
	blockDeviceRootPath string
}

func NewDeviceHelper() *DeviceHelper {
	return &DeviceHelper{blockDeviceRootPath: consts.BlockDeviceRootPathLinux}
}

func (deviceHelper *DeviceHelper) DiskSupportsPerfOptimization(diskPerfProfile, diskAccountType string) bool {
	return isPerfTuningEnabled(diskPerfProfile) && accountSupportsPerfOptimization(diskAccountType)
}

// OptimizeDiskPerformance optimizes device performance by setting tuning block device settings
func (deviceHelper *DeviceHelper) OptimizeDiskPerformance(nodeInfo *NodeInfo, devicePath,
	perfProfile, accountType, diskSizeGibStr, diskIopsStr, diskBwMbpsStr string, deviceSettingsFromCtx map[string]string) (err error) {

	if nodeInfo == nil {
		return fmt.Errorf("OptimizeDiskPerformance: Node info is not provided. Error: invalid parameter")
	}

	deviceName, err := getDeviceName(devicePath)
	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Could not get deviceName for %s. Error: %v", devicePath, err)
	}

	var deviceSettings map[string]string
	deviceRoot := filepath.Join(deviceHelper.blockDeviceRootPath, deviceName)
	switch strings.ToLower(perfProfile) {
	case consts.PerfProfileBasic:
		deviceSettings, err = getDeviceSettingsForBasicProfile(nodeInfo,
			deviceRoot, perfProfile, accountType, diskSizeGibStr, diskIopsStr, diskBwMbpsStr)
	case consts.PerfProfileAdvanced:
		deviceSettings, err = getDeviceSettingsForAdvancedProfile(deviceRoot, deviceSettingsFromCtx)
	default:
		return fmt.Errorf("OptimizeDiskPerformance: Invalid perfProfile %s", perfProfile)
	}

	if err != nil {
		return fmt.Errorf("OptimizeDiskPerformance: Failed to optimize disk for deviceName %s perfProfile %s. Error: %v",
			deviceName, perfProfile, err)
	}

	klog.V(2).Infof("OptimizeDiskPerformance: Tuning settings for deviceRoot %s perfProfile %s accountType %s deviceSettings %v",
		deviceRoot,
		perfProfile,
		accountType,
		deviceSettings)
	return applyDeviceSettings(deviceRoot, deviceSettings)
}

func getDeviceSettingsForBasicProfile(nodeInfo *NodeInfo,
	deviceRoot, perfProfile, accountType, diskSizeGibStr, diskIopsStr, diskBwMbpsStr string) (deviceSettings map[string]string, err error) {
	klog.V(2).Infof("getDeviceSettingsForBasicProfile: Getting settings for deviceRoot %s",
		deviceRoot)
	queueDepth, nrRequests, scheduler, maxSectorsKb, _, err := getOptimalDeviceSettings(nodeInfo, DiskSkuMap, perfProfile, accountType, diskSizeGibStr, diskIopsStr, diskBwMbpsStr)
	if err != nil {
		return nil, fmt.Errorf("getDeviceSettingsForBasicProfile: Failed to get optimal settings for profile %s accountType %s. Error: %v", perfProfile, accountType, err)
	}

	deviceSettings = make(map[string]string)
	deviceSettings[filepath.Join(deviceRoot, "queue/max_sectors_kb")] = maxSectorsKb
	deviceSettings[filepath.Join(deviceRoot, "queue/scheduler")] = scheduler
	deviceSettings[filepath.Join(deviceRoot, "device/queue_depth")] = queueDepth
	deviceSettings[filepath.Join(deviceRoot, "queue/nr_requests")] = nrRequests
	deviceSettings[filepath.Join(deviceRoot, "queue/read_ahead_kb")] = "8"

	return deviceSettings, nil
}

func getDeviceSettingsForAdvancedProfile(deviceRoot string, deviceSettingsFromCtx map[string]string) (deviceSettings map[string]string, err error) {
	klog.V(2).Infof("getDeviceSettingsForAdvancedProfile: Getting settings for deviceRoot %s deviceSettingsFromCtx %v",
		deviceRoot,
		deviceSettingsFromCtx)
	deviceSettings = make(map[string]string)
	for setting, value := range deviceSettingsFromCtx {
		deviceSettings[filepath.Join(deviceRoot, setting)] = value
	}

	return deviceSettings, nil
}

func applyDeviceSettings(deviceRoot string, deviceSettings map[string]string) (err error) {
	if err = AreDeviceSettingsValid(deviceRoot, deviceSettings); err != nil {
		return err
	}

	for setting, value := range deviceSettings {
		err = echoToFile(value, setting)
		if err != nil {
			return fmt.Errorf("applyDeviceSettings: Could not set %s with value %s. Error: %v",
				setting,
				value,
				err)
		}
	}

	return nil
}

// getDeviceName gets the device name from the device lunpath
// Device lun path is of the format /dev/disk/azure/scsi1/lun0
func getDeviceName(lunPath string) (deviceName string, err error) {
	devicePath, err := filepath.EvalSymlinks(lunPath)
	if err != nil {
		return "", fmt.Errorf("path %s is not a symlink. Error: %v", lunPath, err)
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
	klog.V(12).Infof("Calculating perf optimizations for rofile %s accountType %s diskSize %s", perfProfile, accountType, diskSizeGibStr)
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
	qdMaxRandomIops := math.Ceil(maxBurstIops * latencyForSmallRandOperationInSec)

	maxSectorsKb = fmt.Sprintf("%g", rsMinSeqIo)
	scheduler = "mq-deadline"

	qdTotal := fmt.Sprintf("%g", math.Ceil(math.Max(((qdMaxRandomIops*iopsHeadRoom)+qdMaxSeqBw), qdMaxRandomIops)))
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
