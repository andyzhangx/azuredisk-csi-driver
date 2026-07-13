//go:build linux
// +build linux

/*
Copyright 2017 The Kubernetes Authors.

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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	mount "k8s.io/mount-utils"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
)

const sysClassBlockPath = "/sys/class/block/"

// exclude those used by azure as resource and OS root in /dev/disk/azure, /dev/disk/azure/scsi0
// "/dev/disk/azure/scsi0" dir is populated in Standard_DC4s/DC2s on Ubuntu 18.04
func listAzureDiskPath(io azureutils.IOHandler) []string {
	var azureDiskList []string
	azureResourcePaths := []string{"/dev/disk/azure/", "/dev/disk/azure/scsi0/"}
	for _, azureDiskPath := range azureResourcePaths {
		if dirs, err := io.ReadDir(azureDiskPath); err == nil {
			for _, f := range dirs {
				name := f.Name()
				diskPath := filepath.Join(azureDiskPath, name)
				if link, linkErr := io.Readlink(diskPath); linkErr == nil {
					sd := link[(strings.LastIndex(link, "/") + 1):]
					azureDiskList = append(azureDiskList, sd)
				}
			}
		}
	}
	klog.V(12).Infof("Azure sys disks paths: %v", azureDiskList)
	return azureDiskList
}

// getDiskLinkByDevName get disk link by device name from devLinkPath, e.g. /dev/disk/azure/, /dev/disk/by-id/
func getDiskLinkByDevName(io azureutils.IOHandler, devLinkPath, devName string) (string, error) {
	dirs, err := io.ReadDir(devLinkPath)
	klog.V(12).Infof("azureDisk - begin to find %s from %s", devName, devLinkPath)
	if err == nil {
		for _, f := range dirs {
			diskPath := devLinkPath + f.Name()
			klog.V(12).Infof("azureDisk - begin to Readlink: %s", diskPath)
			link, linkErr := io.Readlink(diskPath)
			if linkErr != nil {
				klog.Warningf("azureDisk - read link (%s) error: %v", diskPath, linkErr)
				continue
			}
			if strings.HasSuffix(link, devName) {
				return diskPath, nil
			}
		}
		return "", fmt.Errorf("device name(%s) is not found under %s", devName, devLinkPath)
	}
	return "", fmt.Errorf("read %s error: %v", devLinkPath, err)
}

func scsiHostRescan(io azureutils.IOHandler, _ *mount.SafeFormatAndMount) {
	scsiPath := "/sys/class/scsi_host/"
	if dirs, err := io.ReadDir(scsiPath); err == nil {
		for _, f := range dirs {
			name := scsiPath + f.Name() + "/scan"
			data := []byte("- - -")
			if err = io.WriteFile(name, data, 0666); err != nil {
				klog.Warningf("failed to rescan scsi host %s", name)
			}
		}
	} else {
		klog.Warningf("failed to read %s, err %v", scsiPath, err)
	}
}

func findDiskByLun(lun int, io azureutils.IOHandler, _ *mount.SafeFormatAndMount) (string, error) {
	azureDisks := listAzureDiskPath(io)
	device, err := findDiskByLunWithConstraint(lun, io, azureDisks)
	if err == nil && device != "" {
		return device, nil
	}

	devPaths := []string{
		fmt.Sprintf("/dev/disk/azure/scsi1/lun%d", lun),
		fmt.Sprintf("/dev/disk/azure/data/by-lun/%d", lun),
	}
	klog.Warningf("failed to find disk by lun %d, err %v, fall back to search in following device path: %s", lun, err, devPaths)
	for _, devPath := range devPaths {
		if _, err := os.Stat(devPath); err == nil {
			if device, err := io.Readlink(devPath); err == nil {
				klog.V(2).Infof("found device path %s linked to %s by lun %d", devPath, device, lun)
				return devPath, nil
			}
		}
	}
	return "", fmt.Errorf("failed to find disk by lun %d", lun)
}

func formatAndMount(source, target, fstype string, options []string, m *mount.SafeFormatAndMount) error {
	if newOptions, exists := azureutils.RemoveOptionIfExists(options, "directmount"); exists {
		klog.V(2).Infof("formatAndMount - skip format for %s, old options: %v, new options: %v", target, options, newOptions)
		return m.Mount(source, target, fstype, newOptions)
	}

	// Data-loss guard for kubernetes/kubernetes#140376: if blkid returns
	// empty but the raw device carries an ext/xfs/btrfs signature (e.g.
	// primary superblock damaged, backup SB still present), FormatAndMount
	// would silently reformat the disk and lose the customer's data. Peek
	// at a small set of well-known offsets first; if we see a signature,
	// downgrade to a plain Mount so the driver never issues mkfs on data.
	if newOptions, skip := azureutils.RemoveOptionIfExists(options, "skipFilesystemSignatureCheck"); skip {
		klog.V(2).Infof("formatAndMount - skipFilesystemSignatureCheck opt-out set on %s", source)
		options = newOptions
	} else if sig, err := peekFilesystemSignature(source); err != nil {
		// Unreadable device: be conservative and let m.FormatAndMount
		// surface its own error rather than pre-empting it. This
		// matches the pre-existing behaviour: the caller retries and
		// upstream sees the same error message it always did.
		klog.Warningf("formatAndMount - signature peek failed for %s: %v (proceeding with FormatAndMount)", source, err)
	} else if sig.Type != "" {
		klog.Warningf("formatAndMount - detected existing filesystem %s on %s; downgrading to Mount to avoid reformatting an already-formatted disk (see kubernetes/kubernetes#140376). Set mountOption 'skipFilesystemSignatureCheck' on the StorageClass/PV to override.", sig, source)
		return m.Mount(source, target, fstype, options)
	}

	return m.FormatAndMount(source, target, fstype, options)
}

// finds a device mounted to "current" node
func findDiskByLunWithConstraint(lun int, io azureutils.IOHandler, azureDisks []string) (string, error) {
	var err error
	sysPath := "/sys/bus/scsi/devices"
	if dirs, err := io.ReadDir(sysPath); err == nil {
		for _, f := range dirs {
			name := f.Name()
			// look for path like /sys/bus/scsi/devices/3:0:0:1
			arr := strings.Split(name, ":")
			if len(arr) < 4 {
				continue
			}
			if len(azureDisks) == 0 {
				klog.V(4).Infof("/dev/disk/azure is not populated, now try to parse %v directly", name)
				target, err := strconv.Atoi(arr[0])
				if err != nil {
					klog.Errorf("failed to parse target from %v (%v), err %v", arr[0], name, err)
					continue
				}
				// as observed, targets 0-3 are used by OS disks. Skip them
				if target <= 3 {
					continue
				}
			}

			// extract LUN from the path.
			// LUN is the last index of the array, i.e. 1 in /sys/bus/scsi/devices/3:0:0:1
			l, err := strconv.Atoi(arr[3])
			if err != nil {
				// unknown path format, continue to read the next one
				klog.V(4).Infof("azure disk - failed to parse lun from %v (%v), err %v", arr[3], name, err)
				continue
			}
			if lun == l {
				// find the matching LUN
				// read vendor and model to ensure it is a VHD disk
				vendorPath := filepath.Join(sysPath, name, "vendor")
				vendorBytes, err := io.ReadFile(vendorPath)
				if err != nil {
					klog.Errorf("failed to read device vendor, err: %v", err)
					continue
				}
				vendor := strings.TrimSpace(string(vendorBytes))
				if strings.ToUpper(vendor) != "MSFT" {
					klog.V(4).Infof("vendor doesn't match VHD, got %s", vendor)
					continue
				}

				modelPath := filepath.Join(sysPath, name, "model")
				modelBytes, err := io.ReadFile(modelPath)
				if err != nil {
					klog.Errorf("failed to read device model, err: %v", err)
					continue
				}
				model := strings.TrimSpace(string(modelBytes))
				if strings.ToUpper(model) != "VIRTUAL DISK" {
					klog.V(4).Infof("model doesn't match VHD, got %s", model)
					continue
				}

				// find a disk, validate name
				dir := filepath.Join(sysPath, name, "block")
				if dev, err := io.ReadDir(dir); err == nil {
					found := false
					devName := dev[0].Name()
					for _, diskName := range azureDisks {
						klog.V(12).Infof("azureDisk - validating disk %q with sys disk %q", devName, diskName)
						if devName == diskName {
							found = true
							break
						}
					}
					if !found {
						devLinkPaths := []string{"/dev/disk/azure/scsi1/", "/dev/disk/by-id/"}
						for _, devLinkPath := range devLinkPaths {
							diskPath, err := getDiskLinkByDevName(io, devLinkPath, devName)
							if err == nil {
								klog.V(4).Infof("azureDisk - found %s by %s under %s", diskPath, devName, devLinkPath)
								return diskPath, nil
							}
							klog.Warningf("azureDisk - getDiskLinkByDevName by %s under %s failed, error: %v", devName, devLinkPath, err)
						}
						return "/dev/" + devName, nil
					}
				}
			}
		}
	}
	return "", err
}

func preparePublishPath(_ string, _ *mount.SafeFormatAndMount) error {
	return nil
}

func CleanupMountPoint(path string, m *mount.SafeFormatAndMount, extensiveCheck bool) error {
	return mount.CleanupMountPoint(path, m, extensiveCheck)
}

func getDevicePathWithMountPath(mountPath string, m *mount.SafeFormatAndMount) (string, error) {
	args := []string{"-o", "source", "--noheadings", "--mountpoint", mountPath}
	output, err := m.Exec.Command("findmnt", args...).Output()
	if err != nil {
		return "", fmt.Errorf("could not determine device path(%s), error: %v", mountPath, err)
	}

	devicePath := strings.TrimSpace(string(output))
	if len(devicePath) == 0 {
		return "", fmt.Errorf("could not get valid device for mount path: %q", mountPath)
	}

	return devicePath, nil
}

func getBlockSizeBytes(devicePath string, m *mount.SafeFormatAndMount) (int64, error) {
	output, err := m.Exec.Command("blockdev", "--getsize64", devicePath).Output()
	if err != nil {
		return -1, fmt.Errorf("error when getting size of block volume at path %s: output: %s, err: %v", devicePath, string(output), err)
	}
	strOut := strings.TrimSpace(string(output))
	gotSizeBytes, err := strconv.ParseInt(strOut, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("failed to parse size %s into int a size", strOut)
	}
	return gotSizeBytes, nil
}

func resizeVolume(devicePath, volumePath string, m *mount.SafeFormatAndMount) error {
	_, err := mount.NewResizeFs(m.Exec).Resize(devicePath, volumePath)
	return err
}

// needResizeVolume check whether device needs resize
func needResizeVolume(devicePath, volumePath string, m *mount.SafeFormatAndMount) (bool, error) {
	return mount.NewResizeFs(m.Exec).NeedResize(devicePath, volumePath)
}

// rescanVolume rescan device for detecting device size expansion
// devicePath e.g. `/dev/sdc`
func rescanVolume(io azureutils.IOHandler, devicePath string) error {
	klog.V(6).Infof("rescanVolume - begin to rescan %s", devicePath)
	deviceName := filepath.Base(devicePath)
	rescanPath := filepath.Join(sysClassBlockPath, deviceName, "device/rescan")
	return io.WriteFile(rescanPath, []byte("1"), 0666)
}

// rescanAllVolumes rescan all sd* devices under /sys/class/block/sd* starting from sdc
func rescanAllVolumes(io azureutils.IOHandler) error {
	dirs, err := io.ReadDir(sysClassBlockPath)
	if err != nil {
		return err
	}
	for _, device := range dirs {
		deviceName := device.Name()
		if strings.HasPrefix(deviceName, "sd") && deviceName >= "sdc" {
			path := filepath.Join(sysClassBlockPath, deviceName)
			if err := rescanVolume(io, path); err != nil {
				klog.Warningf("rescanVolume - rescan %s failed with %v", path, err)
			}
		}
	}
	return nil
}

func (d *Driver) GetVolumeStats(_ context.Context, m *mount.SafeFormatAndMount, _, target string, hostutil hostUtil) ([]*csi.VolumeUsage, error) {
	var volUsages []*csi.VolumeUsage
	_, err := os.Stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "path %s does not exist", target)
		}
		return volUsages, status.Errorf(codes.Internal, "failed to stat file %s: %v", target, err)
	}

	isBlock, err := hostutil.PathIsDevice(target)
	if err != nil {
		return volUsages, status.Errorf(codes.NotFound, "failed to determine whether %s is block device: %v", target, err)
	}
	if isBlock {
		bcap, err := getBlockSizeBytes(target, m)
		if err != nil {
			return volUsages, status.Errorf(codes.Internal, "failed to get block capacity on path %s: %v", target, err)
		}
		return []*csi.VolumeUsage{
			{
				Unit:  csi.VolumeUsage_BYTES,
				Total: bcap,
			},
		}, nil
	}

	volumeMetrics, err := volume.NewMetricsStatFS(target).GetMetrics()
	if err != nil {
		return volUsages, err
	}

	available, ok := volumeMetrics.Available.AsInt64()
	if !ok {
		return volUsages, status.Errorf(codes.Internal, "failed to transform volume available size(%v)", volumeMetrics.Available)
	}
	capacity, ok := volumeMetrics.Capacity.AsInt64()
	if !ok {
		return volUsages, status.Errorf(codes.Internal, "failed to transform volume capacity size(%v)", volumeMetrics.Capacity)
	}
	used, ok := volumeMetrics.Used.AsInt64()
	if !ok {
		return volUsages, status.Errorf(codes.Internal, "failed to transform volume used size(%v)", volumeMetrics.Used)
	}

	inodesFree, ok := volumeMetrics.InodesFree.AsInt64()
	if !ok {
		return volUsages, status.Errorf(codes.Internal, "failed to transform disk inodes free(%v)", volumeMetrics.InodesFree)
	}
	inodes, ok := volumeMetrics.Inodes.AsInt64()
	if !ok {
		return volUsages, status.Errorf(codes.Internal, "failed to transform disk inodes(%v)", volumeMetrics.Inodes)
	}
	inodesUsed, ok := volumeMetrics.InodesUsed.AsInt64()
	if !ok {
		return volUsages, status.Errorf(codes.Internal, "failed to transform disk inodes used(%v)", volumeMetrics.InodesUsed)
	}

	return []*csi.VolumeUsage{
		{
			Unit:      csi.VolumeUsage_BYTES,
			Available: available,
			Total:     capacity,
			Used:      used,
		},
		{
			Unit:      csi.VolumeUsage_INODES,
			Available: inodesFree,
			Total:     inodes,
			Used:      inodesUsed,
		},
	}, nil
}
