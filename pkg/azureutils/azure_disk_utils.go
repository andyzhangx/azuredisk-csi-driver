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

package azureutils

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/mount-utils"
	"k8s.io/utils/ptr"
	consts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/filewatcher"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/optimization"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/util"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/configloader"
	azure "sigs.k8s.io/cloud-provider-azure/pkg/provider"
	azureconfig "sigs.k8s.io/cloud-provider-azure/pkg/provider/config"
)

const (
	azurePublicCloud                          = "AZUREPUBLICCLOUD"
	azureStackCloud                           = "AZURESTACKCLOUD"
	azurePublicCloudDefaultStorageAccountType = armcompute.DiskStorageAccountTypesStandardSSDLRS
	azureStackCloudDefaultStorageAccountType  = armcompute.DiskStorageAccountTypesStandardLRS
	defaultAzureDataDiskCachingMode           = v1.AzureDataDiskCachingReadOnly
	// default IOPS Caps & Throughput Cap (MBps) per https://docs.microsoft.com/en-us/azure/virtual-machines/linux/disks-ultra-ssd
	// see https://docs.microsoft.com/en-us/rest/api/compute/disks/createorupdate#uri-parameters
	diskNameMinLength         = 1
	diskNameMaxLength         = 80
	diskNameGenerateMaxLength = 76 // maxLength = 80 - (4 for ".vhd") = 76
	MaxPathLengthWindows      = 260
)

var (
	// see https://docs.microsoft.com/en-us/rest/api/compute/disks/createorupdate#create-a-managed-disk-by-copying-a-snapshot.
	diskSnapshotPath      = "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/snapshots/%s"
	diskSnapshotPathRE    = regexp.MustCompile(`(?i).*/subscriptions/(?:.*)/resourceGroups/(?:.*)/providers/Microsoft.Compute/snapshots/(.+)`)
	lunPathRE             = regexp.MustCompile(`/dev(?:.*)/disk/azure/scsi(?:.*)/lun(.+)`)
	supportedCachingModes = sets.NewString(
		string(api.AzureDataDiskCachingNone),
		string(api.AzureDataDiskCachingReadOnly),
		string(api.AzureDataDiskCachingReadWrite),
	)

	// volumeCaps represents how the volume could be accessed.
	volumeCaps = []*csi.VolumeCapability_AccessMode{
		{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY},
		{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER},
		{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER},
		{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY},
		{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER},
		{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
	}
)

type ManagedDiskParameters struct {
	AccountType             string
	CachingMode             v1.AzureDataDiskCachingMode
	DeviceSettings          map[string]string
	DiskAccessID            string
	DiskEncryptionSetID     string
	DiskEncryptionType      string
	DiskIOPSReadWrite       string
	DiskMBPSReadWrite       string
	DiskName                string
	EnableBursting          *bool
	PerformancePlus         *bool
	FsType                  string
	Location                string
	LogicalSectorSize       int
	MaxShares               int
	NetworkAccessPolicy     string
	PublicNetworkAccess     string
	PerfProfile             string
	SubscriptionID          string
	ResourceGroup           string
	Tags                    map[string]string
	UserAgent               string
	VolumeContext           map[string]string
	WriteAcceleratorEnabled string
	Zoned                   string
}

func GetCachingMode(attributes map[string]string) (armcompute.CachingTypes, error) {
	var (
		cachingMode v1.AzureDataDiskCachingMode
		err         error
	)

	for k, v := range attributes {
		if strings.EqualFold(k, consts.CachingModeField) {
			cachingMode = v1.AzureDataDiskCachingMode(v)
			break
		}
	}

	cachingMode, err = NormalizeCachingMode(cachingMode)
	return armcompute.CachingTypes(cachingMode), err
}

// GetAttachDiskInitialDelay gttachDiskInitialDelay from attributes
// return -1 if not found
func GetAttachDiskInitialDelay(attributes map[string]string) int {
	for k, v := range attributes {
		switch strings.ToLower(k) {
		case consts.AttachDiskInitialDelayField:
			if v, err := strconv.Atoi(v); err == nil {
				return v
			}
		}
	}
	return -1
}

// GetCloudProviderFromClient get Azure Cloud Provider
func GetCloudProviderFromClient(ctx context.Context, kubeClient clientset.Interface, secretName, secretNamespace, userAgent string,
	allowEmptyCloudConfig bool, enableTrafficMgr bool, enableMinimumRetryAfter bool, trafficMgrPort int64) (*azure.Cloud, error) {
	var config *azureconfig.Config
	var fromSecret bool
	var err error
	az := &azure.Cloud{}
	if kubeClient != nil {
		klog.V(2).Infof("reading cloud config from secret %s/%s", secretNamespace, secretName)
		config, err = configloader.Load[azureconfig.Config](ctx, &configloader.K8sSecretLoaderConfig{
			K8sSecretConfig: configloader.K8sSecretConfig{
				SecretName:      secretName,
				SecretNamespace: secretNamespace,
				CloudConfigKey:  "cloud-config",
			},
			KubeClient: kubeClient,
		}, nil)
		if err != nil {
			klog.V(2).Infof("InitializeCloudFromSecret: failed to get cloud config from secret %s/%s: %v", secretNamespace, secretName, err)
		}
		if err == nil && config != nil {
			fromSecret = true
		}
		az.KubeClient = kubeClient
	}

	if config == nil {
		klog.V(2).Infof("could not read cloud config from secret %s/%s", secretNamespace, secretName)
		credFile, ok := os.LookupEnv(consts.DefaultAzureCredentialFileEnv)
		if ok && strings.TrimSpace(credFile) != "" {
			klog.V(2).Infof("%s env var set as %v", consts.DefaultAzureCredentialFileEnv, credFile)
		} else {
			if util.IsWindowsOS() {
				credFile = consts.DefaultCredFilePathWindows
			} else {
				credFile = consts.DefaultCredFilePathLinux
			}
			klog.V(2).Infof("use default %s env var: %v", consts.DefaultAzureCredentialFileEnv, credFile)
		}
		config, err = configloader.Load[azureconfig.Config](ctx, nil, &configloader.FileLoaderConfig{FilePath: credFile})
		if err != nil {
			klog.Warningf("load azure config from file(%s) failed with %v", credFile, err)
		}
	}

	if config == nil {
		if allowEmptyCloudConfig {
			klog.V(2).Infof("no cloud config provided, error: %v, driver will run without cloud config", err)
		} else {
			return nil, fmt.Errorf("no cloud config provided, error: %v", err)
		}
	} else {
		// Location may be either upper case with spaces (e.g. "East US") or lower case without spaces (e.g. "eastus")
		// Kubernetes does not allow whitespaces in label values, e.g. for topology keys
		// ensure Kubernetes compatible format for Location by enforcing lowercase-no-space format
		config.Location = strings.ToLower(strings.ReplaceAll(config.Location, " ", ""))

		// disable disk related rate limit
		/* todo: reconfigure rate limit
		config.DiskRateLimit = &ratelimit.CloudProviderRateLimitConfig{
			CloudProviderRateLimit: false,
		}
		config.SnapshotRateLimit = &ratelimit.CloudProviderRateLimitConfig{
			CloudProviderRateLimit: false,
		}
		*/
		config.UserAgent = userAgent
		if enableTrafficMgr && trafficMgrPort > 0 {
			trafficMgrAddr := fmt.Sprintf("http://localhost:%d/", trafficMgrPort)
			klog.V(2).Infof("set ResourceManagerEndpoint as %s", trafficMgrAddr)
			config.ResourceManagerEndpoint = trafficMgrAddr
		}
		// these environment variables are injected by workload identity webhook
		if tenantID := os.Getenv("AZURE_TENANT_ID"); tenantID != "" {
			config.TenantID = tenantID
		}
		if clientID := os.Getenv("AZURE_CLIENT_ID"); clientID != "" {
			config.AADClientID = clientID
		}
		if federatedTokenFile := os.Getenv("AZURE_FEDERATED_TOKEN_FILE"); federatedTokenFile != "" {
			config.AADFederatedTokenFile = federatedTokenFile
			config.UseFederatedWorkloadIdentityExtension = true
		}
		if len(config.AADClientCertPath) > 0 {
			// Watch the certificate for changes; if the certificate changes, the pod will be restarted
			err = filewatcher.WatchFileForChanges(config.AADClientCertPath)
			klog.Warningf("Failed to watch certificate file for changes: %v", err)
		}
		if enableMinimumRetryAfter {
			config.EnableMinimumRetryAfter = enableMinimumRetryAfter
		}
		if err = az.InitializeCloudFromConfig(ctx, config, fromSecret, false); err != nil {
			klog.Warningf("InitializeCloudFromConfig failed with error: %v", err)
		}
	}

	// reassign kubeClient
	if kubeClient != nil && az.KubeClient == nil {
		az.KubeClient = kubeClient
	}
	return az, nil
}

func GetKubeClient(kubeconfig string) (clientset.Interface, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}

	return clientset.NewForConfig(config)
}

// GetDiskLUN : deviceInfo could be a LUN number or a device path, e.g. /dev/disk/azure/scsi1/lun2
func GetDiskLUN(deviceInfo string) (int32, error) {
	var diskLUN string
	if len(deviceInfo) <= 2 {
		diskLUN = deviceInfo
	} else {
		// extract the LUN num from a device path
		matches := lunPathRE.FindStringSubmatch(deviceInfo)
		if len(matches) == 2 {
			diskLUN = matches[1]
		} else {
			return -1, fmt.Errorf("cannot parse deviceInfo: %s", deviceInfo)
		}
	}

	lun, err := strconv.Atoi(diskLUN)
	if err != nil {
		return -1, err
	}
	return int32(lun), nil
}

// Disk name must begin with a letter or number, end with a letter, number or underscore,
// and may contain only letters, numbers, underscores, periods, or hyphens.
// See https://docs.microsoft.com/en-us/rest/api/compute/disks/createorupdate#uri-parameters
//
// Snapshot name must begin with a letter or number, end with a letter, number or underscore,
// and may contain only letters, numbers, underscores, periods, or hyphens.
// See https://docs.microsoft.com/en-us/rest/api/compute/snapshots/createorupdate#uri-parameters
//
// Since the naming rule of disk is same with snapshot's, here we use the same function to handle disks and snapshots.
func CreateValidDiskName(volumeName string) string {
	diskName := volumeName
	if len(diskName) > diskNameMaxLength {
		diskName = diskName[0:diskNameMaxLength]
		klog.Warningf("since the maximum volume name length is %d, so it is truncated as (%q)", diskNameMaxLength, diskName)
	}
	if !checkDiskName(diskName) || len(diskName) < diskNameMinLength {
		// todo: get cluster name
		diskName = GenerateVolumeName("pvc-disk", string(uuid.NewUUID()), diskNameGenerateMaxLength)
		klog.Warningf("the requested volume name (%q) is invalid, so it is regenerated as (%q)", volumeName, diskName)
	}

	return diskName
}

func GetFStype(attributes map[string]string) string {
	for k, v := range attributes {
		switch strings.ToLower(k) {
		case consts.FsTypeField:
			return strings.ToLower(v)
		}
	}
	return ""
}

func GetMaxShares(attributes map[string]string) (int, error) {
	for k, v := range attributes {
		switch strings.ToLower(k) {
		case consts.MaxSharesField:
			maxShares, err := strconv.Atoi(v)
			if err != nil {
				return 0, fmt.Errorf("parse %s failed with error: %v", v, err)
			}
			if maxShares < 1 {
				return 0, fmt.Errorf("parse %s returned with invalid value: %d", v, maxShares)
			}
			return maxShares, nil
		}
	}
	return 1, nil // disk is not shared
}

// GetInfoFromURI get subscriptionID, resourceGroup, diskName from diskURI
// examples:
// diskURI: /subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.Compute/disks/{disk-name}
// snapshotURI: /subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.Compute/snapshots/{snapshot-name}
func GetInfoFromURI(diskURI string) (string, string, string, error) {
	parts := strings.Split(diskURI, "/")
	if len(parts) != 9 {
		return "", "", "", fmt.Errorf("invalid URI: %s", diskURI)
	}
	return parts[2], parts[4], parts[8], nil
}

func GetValidCreationData(subscriptionID, resourceGroup, sourceResourceID, sourceType string) (armcompute.CreationData, error) {
	if sourceResourceID == "" {
		return armcompute.CreationData{
			CreateOption: to.Ptr(armcompute.DiskCreateOptionEmpty),
		}, nil
	}

	switch sourceType {
	case consts.SourceSnapshot:
		if match := diskSnapshotPathRE.FindString(sourceResourceID); match == "" {
			sourceResourceID = fmt.Sprintf(diskSnapshotPath, subscriptionID, resourceGroup, sourceResourceID)
		}

	case consts.SourceVolume:
		if match := consts.ManagedDiskPathRE.FindString(sourceResourceID); match == "" {
			sourceResourceID = fmt.Sprintf(consts.ManagedDiskPath, subscriptionID, resourceGroup, sourceResourceID)
		}
	default:
		return armcompute.CreationData{
			CreateOption: to.Ptr(armcompute.DiskCreateOptionEmpty),
		}, nil
	}

	splits := strings.Split(sourceResourceID, "/")
	if len(splits) > 9 {
		if sourceType == consts.SourceSnapshot {
			return armcompute.CreationData{}, fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", sourceResourceID, diskSnapshotPathRE)
		}

		return armcompute.CreationData{}, fmt.Errorf("sourceResourceID(%s) is invalid, correct format: %s", sourceResourceID, consts.ManagedDiskPathRE)
	}
	return armcompute.CreationData{
		CreateOption:     to.Ptr(armcompute.DiskCreateOptionCopy),
		SourceResourceID: &sourceResourceID,
	}, nil
}

func IsCorruptedDir(dir string) bool {
	_, pathErr := mount.PathExists(dir)
	return pathErr != nil && mount.IsCorruptedMnt(pathErr)
}

// IsARMResourceID check whether resourceID is an ARM ResourceID
func IsARMResourceID(resourceID string) bool {
	id := strings.ToLower(resourceID)
	return strings.Contains(id, "/subscriptions/")
}

// IsAzureStackCloud decides whether the driver is running on Azure Stack Cloud.
func IsAzureStackCloud(cloud string, disableAzureStackCloud bool) bool {
	return !disableAzureStackCloud && strings.EqualFold(cloud, azureStackCloud)
}

// IsValidAvailabilityZone returns true if the zone is in format of <region>-<zone-id>.
func IsValidAvailabilityZone(zone, region string) bool {
	if region == "" {
		index := strings.Index(zone, "-")
		return index > 0 && index < len(zone)-1
	}
	return strings.HasPrefix(zone, fmt.Sprintf("%s-", region))
}

// GetRegionFromAvailabilityZone returns region from availability zone if it's in format of <region>-<zone-id>
func GetRegionFromAvailabilityZone(zone string) string {
	parts := strings.Split(zone, "-")
	if len(parts) == 2 {
		return parts[0]
	}
	return ""
}

// IsValidVolumeCapabilities checks whether the volume capabilities are valid
func IsValidVolumeCapabilities(volCaps []*csi.VolumeCapability, maxShares int) error {
	if ok := IsValidAccessModes(volCaps); !ok {
		return fmt.Errorf("invalid access mode: %v", volCaps)
	}
	for _, c := range volCaps {
		blockVolume := c.GetBlock()
		mountVolume := c.GetMount()
		accessMode := c.GetAccessMode().GetMode()

		if blockVolume == nil && mountVolume == nil {
			return fmt.Errorf("blockVolume and mountVolume are both nil")
		}

		if blockVolume != nil && mountVolume != nil {
			return fmt.Errorf("blockVolume and mountVolume are both not nil")
		}
		if mountVolume != nil && (accessMode == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER ||
			accessMode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY ||
			accessMode == csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER) {
			return fmt.Errorf("mountVolume is not supported for access mode: %s", accessMode.String())
		}
		if maxShares < 2 && (accessMode == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER ||
			accessMode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY ||
			accessMode == csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER) {
			return fmt.Errorf("access mode: %s is not supported for non-shared disk", accessMode.String())
		}
	}
	return nil
}

func IsValidAccessModes(volCaps []*csi.VolumeCapability) bool {
	hasSupport := func(capability *csi.VolumeCapability) bool {
		for _, c := range volumeCaps {
			if c.GetMode() == capability.AccessMode.GetMode() {
				return true
			}
		}
		return false
	}

	foundAll := true
	for _, c := range volCaps {
		if !hasSupport(c) {
			foundAll = false
		}
	}
	return foundAll
}

func NormalizeCachingMode(cachingMode v1.AzureDataDiskCachingMode) (v1.AzureDataDiskCachingMode, error) {
	if cachingMode == "" {
		return defaultAzureDataDiskCachingMode, nil
	}

	if !supportedCachingModes.Has(string(cachingMode)) {
		return "", fmt.Errorf("azureDisk - %s is not supported cachingmode. Supported values are %s", cachingMode, supportedCachingModes.List())
	}

	return cachingMode, nil
}

func NormalizeNetworkAccessPolicy(networkAccessPolicy string) (armcompute.NetworkAccessPolicy, error) {
	if networkAccessPolicy == "" {
		return armcompute.NetworkAccessPolicy(networkAccessPolicy), nil
	}
	policy := armcompute.NetworkAccessPolicy(networkAccessPolicy)
	for _, s := range armcompute.PossibleNetworkAccessPolicyValues() {
		if policy == s {
			return policy, nil
		}
	}
	return "", fmt.Errorf("azureDisk - %s is not supported NetworkAccessPolicy. Supported values are %s", networkAccessPolicy, armcompute.PossibleNetworkAccessPolicyValues())
}

func NormalizePublicNetworkAccess(publicNetworkAccess string) (armcompute.PublicNetworkAccess, error) {
	if publicNetworkAccess == "" {
		return armcompute.PublicNetworkAccess(publicNetworkAccess), nil
	}
	access := armcompute.PublicNetworkAccess(publicNetworkAccess)
	for _, s := range armcompute.PossiblePublicNetworkAccessValues() {
		if access == s {
			return access, nil
		}
	}
	return "", fmt.Errorf("azureDisk - %s is not supported PublicNetworkAccess. Supported values are %s", publicNetworkAccess, armcompute.PossiblePublicNetworkAccessValues())
}

func NormalizeStorageAccountType(storageAccountType, cloud string, disableAzureStackCloud bool) (armcompute.DiskStorageAccountTypes, error) {
	if storageAccountType == "" {
		if IsAzureStackCloud(cloud, disableAzureStackCloud) {
			return azureStackCloudDefaultStorageAccountType, nil
		}
		return azurePublicCloudDefaultStorageAccountType, nil
	}

	sku := armcompute.DiskStorageAccountTypes(storageAccountType)
	supportedSkuNames := armcompute.PossibleDiskStorageAccountTypesValues()
	if IsAzureStackCloud(cloud, disableAzureStackCloud) {
		supportedSkuNames = []armcompute.DiskStorageAccountTypes{armcompute.DiskStorageAccountTypesStandardLRS, armcompute.DiskStorageAccountTypesPremiumLRS}
	}
	for _, s := range supportedSkuNames {
		if sku == s {
			return sku, nil
		}
	}

	return "", fmt.Errorf("azureDisk - %s is not supported sku/storageaccounttype. Supported values are %s", storageAccountType, supportedSkuNames)
}

func ValidateDiskEncryptionType(encryptionType string) error {
	if encryptionType == "" {
		return nil
	}
	supportedTypes := armcompute.PossibleEncryptionTypeValues()
	for _, s := range supportedTypes {
		if encryptionType == string(s) {
			return nil
		}
	}
	return fmt.Errorf("DiskEncryptionType(%s) is not supported", encryptionType)
}

func ValidateDataAccessAuthMode(dataAccessAuthMode string) error {
	if dataAccessAuthMode == "" {
		return nil
	}
	supportedModes := armcompute.PossibleDataAccessAuthModeValues()
	for _, s := range supportedModes {
		if dataAccessAuthMode == string(s) {
			return nil
		}
	}
	return fmt.Errorf("dataAccessAuthMode(%s) is not supported", dataAccessAuthMode)
}

func ParseDiskParametersForKey(parameters map[string]string, key string) (string, bool) {
	for k, v := range parameters {
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return "", false
}

func ParseDiskParameters(parameters map[string]string) (ManagedDiskParameters, error) {
	var err error
	if parameters == nil {
		parameters = make(map[string]string)
	}

	diskParams := ManagedDiskParameters{
		DeviceSettings: make(map[string]string),
		Tags:           make(map[string]string),
		VolumeContext:  parameters,
	}
	var originTags, tagValueDelimiter, pvcName, pvcNamespace, pvName string
	for k, v := range parameters {
		switch strings.ToLower(k) {
		case consts.SkuNameField:
			diskParams.AccountType = v
		case consts.LocationField:
			diskParams.Location = v
		case consts.StorageAccountTypeField:
			diskParams.AccountType = v
		case consts.CachingModeField:
			diskParams.CachingMode = v1.AzureDataDiskCachingMode(v)
		case consts.SubscriptionIDField:
			diskParams.SubscriptionID = v
		case consts.ResourceGroupField:
			diskParams.ResourceGroup = v
		case consts.DiskIOPSReadWriteField:
			if _, err = strconv.Atoi(v); err != nil {
				return diskParams, fmt.Errorf("parse %s:%s failed with error: %v", consts.DiskIOPSReadWriteField, v, err)
			}
			diskParams.DiskIOPSReadWrite = v
		case consts.DiskMBPSReadWriteField:
			if _, err = strconv.Atoi(v); err != nil {
				return diskParams, fmt.Errorf("parse %s:%s failed with error: %v", consts.DiskMBPSReadWriteField, v, err)
			}
			diskParams.DiskMBPSReadWrite = v
		case consts.LogicalSectorSizeField:
			diskParams.LogicalSectorSize, err = strconv.Atoi(v)
			if err != nil {
				return diskParams, fmt.Errorf("parse %s failed with error: %v", v, err)
			}
		case consts.DiskNameField:
			diskParams.DiskName = v
		case consts.DesIDField:
			diskParams.DiskEncryptionSetID = v
		case consts.DiskEncryptionTypeField:
			diskParams.DiskEncryptionType = v
		case consts.TagsField:
			originTags = v
		case azure.WriteAcceleratorEnabled:
			diskParams.WriteAcceleratorEnabled = v
		case consts.MaxSharesField:
			diskParams.MaxShares, err = strconv.Atoi(v)
			if err != nil {
				return diskParams, fmt.Errorf("parse %s failed with error: %v", v, err)
			}
			if diskParams.MaxShares < 1 {
				return diskParams, fmt.Errorf("parse %s returned with invalid value: %d", v, diskParams.MaxShares)
			}
		case consts.PvcNameKey:
			pvcName = v
			diskParams.Tags[consts.PvcNameTag] = v
		case consts.PvcNamespaceKey:
			pvcNamespace = v
			diskParams.Tags[consts.PvcNamespaceTag] = v
		case consts.PvNameKey:
			pvName = v
			diskParams.Tags[consts.PvNameTag] = v
		case consts.FsTypeField:
			diskParams.FsType = strings.ToLower(v)
		case consts.KindField:
			// fix csi migration issue: https://github.com/kubernetes/kubernetes/issues/103433
			diskParams.VolumeContext[consts.KindField] = string(v1.AzureManagedDisk)
		case consts.PerfProfileField:
			if !optimization.IsValidPerfProfile(v) {
				return diskParams, fmt.Errorf("perf profile %s is not supported, supported tuning modes are none and basic", v)
			}
			diskParams.PerfProfile = v
		case consts.NetworkAccessPolicyField:
			diskParams.NetworkAccessPolicy = v
		case consts.PublicNetworkAccessField:
			diskParams.PublicNetworkAccess = v
		case consts.DiskAccessIDField:
			diskParams.DiskAccessID = v
		case consts.EnableBurstingField:
			if strings.EqualFold(v, consts.TrueValue) {
				diskParams.EnableBursting = ptr.To(true)
			}
		case consts.UserAgentField:
			diskParams.UserAgent = v
		case consts.EnableAsyncAttachField:
			// no op, only for backward compatibility
		case consts.ZonedField:
			// no op, only for backward compatibility with in-tree driver
		case consts.PerformancePlusField:
			value, err := strconv.ParseBool(v)
			if err != nil {
				return diskParams, fmt.Errorf("invalid %s: %s in storage class", consts.PerformancePlusField, v)
			}
			diskParams.PerformancePlus = &value
		case consts.AttachDiskInitialDelayField:
			if _, err = strconv.Atoi(v); err != nil {
				return diskParams, fmt.Errorf("parse %s failed with error: %v", v, err)
			}
		case consts.TagValueDelimiterField:
			tagValueDelimiter = v
		default:
			// accept all device settings params
			// device settings need to start with azureconstants.DeviceSettingsKeyPrefix
			if deviceSettings, err := optimization.GetDeviceSettingFromAttribute(k); err == nil {
				diskParams.DeviceSettings[filepath.Join(consts.DummyBlockDevicePathLinux, deviceSettings)] = v
			} else {
				return diskParams, fmt.Errorf("invalid parameter %s in storage class", k)
			}
		}
	}

	customTagsMap, err := util.ConvertTagsToMap(originTags, tagValueDelimiter)
	if err != nil {
		return diskParams, err
	}
	for k, v := range customTagsMap {
		if strings.Contains(v, "$") {
			v = replaceTemplateVariables(v, pvcName, pvcNamespace, pvName)
		}
		diskParams.Tags[k] = v
	}

	if strings.EqualFold(diskParams.AccountType, string(armcompute.DiskStorageAccountTypesPremiumV2LRS)) {
		if diskParams.CachingMode != "" && !strings.EqualFold(string(diskParams.CachingMode), string(v1.AzureDataDiskCachingNone)) {
			return diskParams, fmt.Errorf("cachingMode %s is not supported for %s", diskParams.CachingMode, armcompute.DiskStorageAccountTypesPremiumV2LRS)
		}
	}

	// support disk name with $ in it, e.g. ${pvc.metadata.name}
	if strings.Contains(diskParams.DiskName, "$") {
		diskParams.DiskName = replaceTemplateVariables(diskParams.DiskName, pvcName, pvcNamespace, pvName)
	}

	return diskParams, nil
}

// PickAvailabilityZone selects 1 zone given topology requirement.
// if not found or topology requirement is not zone format, empty string is returned.
func PickAvailabilityZone(requirement *csi.TopologyRequirement, region, topologyKey string) string {
	if requirement == nil {
		return ""
	}
	for _, topology := range requirement.GetPreferred() {
		if zone, exists := topology.GetSegments()[consts.WellKnownTopologyKey]; exists {
			if IsValidAvailabilityZone(zone, region) {
				return zone
			}
		}
		if zone, exists := topology.GetSegments()[topologyKey]; exists {
			if IsValidAvailabilityZone(zone, region) {
				return zone
			}
		}
	}
	for _, topology := range requirement.GetRequisite() {
		if zone, exists := topology.GetSegments()[consts.WellKnownTopologyKey]; exists {
			if IsValidAvailabilityZone(zone, region) {
				return zone
			}
		}
		if zone, exists := topology.GetSegments()[topologyKey]; exists {
			if IsValidAvailabilityZone(zone, region) {
				return zone
			}
		}
	}
	return ""
}

func checkDiskName(diskName string) bool {
	length := len(diskName)

	for i, v := range diskName {
		if !(unicode.IsLetter(v) || unicode.IsDigit(v) || v == '_' || v == '.' || v == '-') ||
			(i == 0 && !(unicode.IsLetter(v) || unicode.IsDigit(v))) ||
			(i == length-1 && !(unicode.IsLetter(v) || unicode.IsDigit(v) || v == '_')) {
			return false
		}
	}

	return true
}

// InsertDiskProperties inserts disk properties to map
func InsertDiskProperties(disk *armcompute.Disk, publishConext map[string]string) {
	if disk == nil || publishConext == nil {
		return
	}

	if disk.SKU != nil {
		publishConext[consts.SkuNameField] = string(*disk.SKU.Name)
	}
	prop := disk.Properties
	if prop != nil {
		// Azure stack hub does not have NetworkAccessPolicy property, so the prop.NetworkAccessPolicy will always be nil
		if prop.NetworkAccessPolicy != nil {
			publishConext[consts.NetworkAccessPolicyField] = string(*prop.NetworkAccessPolicy)
		}
		if prop.DiskIOPSReadWrite != nil {
			publishConext[consts.DiskIOPSReadWriteField] = strconv.Itoa(int(*prop.DiskIOPSReadWrite))
		}
		if prop.DiskMBpsReadWrite != nil {
			publishConext[consts.DiskMBPSReadWriteField] = strconv.Itoa(int(*prop.DiskMBpsReadWrite))
		}
		if prop.CreationData != nil && prop.CreationData.LogicalSectorSize != nil {
			publishConext[consts.LogicalSectorSizeField] = strconv.Itoa(int(*prop.CreationData.LogicalSectorSize))
		}
		if prop.Encryption != nil &&
			prop.Encryption.DiskEncryptionSetID != nil {
			publishConext[consts.DesIDField] = *prop.Encryption.DiskEncryptionSetID
		}
		if prop.MaxShares != nil {
			publishConext[consts.MaxSharesField] = strconv.Itoa(int(*prop.MaxShares))
		}
	}
}

func SleepIfThrottled(err error, defaultSleepSec int) {
	if err != nil && IsThrottlingError(err) {
		retryAfter := getRetryAfterSeconds(err)
		if retryAfter == 0 {
			retryAfter = defaultSleepSec
		}
		klog.Warningf("sleep %d more seconds, waiting for throttling complete", retryAfter)
		time.Sleep(time.Duration(retryAfter) * time.Second)
	}
}

func IsThrottlingError(err error) bool {
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		return strings.Contains(errMsg, strings.ToLower(consts.TooManyRequests)) || strings.Contains(errMsg, consts.ClientThrottled)
	}
	return false
}

// getRetryAfterSeconds returns the number of seconds to wait from the error message
func getRetryAfterSeconds(err error) int {
	if err == nil {
		return 0
	}
	re := regexp.MustCompile(`RetryAfter: (\d+)s`)
	match := re.FindStringSubmatch(err.Error())
	if len(match) > 1 {
		if retryAfter, err := strconv.Atoi(match[1]); err == nil {
			if retryAfter > consts.MaxThrottlingSleepSec {
				return consts.MaxThrottlingSleepSec
			}
			return retryAfter
		}
	}
	return 0
}

// SetKeyValueInMap set key/value pair in map
// key in the map is case insensitive, if key already exists, overwrite existing value
func SetKeyValueInMap(m map[string]string, key, value string) {
	if m == nil {
		return
	}
	for k := range m {
		if strings.EqualFold(k, key) {
			m[k] = value
			return
		}
	}
	m[key] = value
}

// GenerateVolumeName returns a PV name with clusterName prefix. The function
// should be used to generate a name of GCE PD or Cinder volume. It basically
// adds "<clusterName>-dynamic-" before the PV name, making sure the resulting
// string fits given length and cuts "dynamic" if not.
func GenerateVolumeName(clusterName, pvName string, maxLength int) string {
	prefix := clusterName + "-dynamic"
	pvLen := len(pvName)
	// cut the "<clusterName>-dynamic" to fit full pvName into maxLength
	// +1 for the '-' dash
	if pvLen+1+len(prefix) > maxLength {
		prefix = prefix[:maxLength-pvLen-1]
	}
	return prefix + "-" + pvName
}

// RemoveOptionIfExists removes the given option from the list of options
// return the new list and a boolean indicating whether the option was found.
func RemoveOptionIfExists(options []string, removeOption string) ([]string, bool) {
	for i, option := range options {
		if option == removeOption {
			return append(options[:i], options[i+1:]...), true
		}
	}
	return options, false
}

func replaceTemplateVariables(str, pvcName, pvcNamespace, pvName string) string {
	// replace $pvc.metadata.name with pvcName
	str = strings.ReplaceAll(str, consts.PvcMetaDataName, pvcName)
	// replace $pvc.metadata.namespace with pvcNamespace
	str = strings.ReplaceAll(str, consts.PvcMetaDataNamespace, pvcNamespace)
	// replace $pv.metadata.name with pvName
	return strings.ReplaceAll(str, consts.PvMetaDataName, pvName)
}
