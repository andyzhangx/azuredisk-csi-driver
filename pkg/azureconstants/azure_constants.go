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

package azureconstants

import (
	"regexp"
)

const (
	AzureDiskCSIDriverName        = "azuredisk_csi_driver"
	CachingModeField              = "cachingmode"
	DefaultAzureCredentialFileEnv = "AZURE_CREDENTIAL_FILE"
	DefaultCredFilePathLinux      = "/etc/kubernetes/azure.json"
	DefaultCredFilePathWindows    = "C:\\k\\azure.json"
	DefaultDriverName             = "disk.csi.azure.com"
	DesIDField                    = "diskencryptionsetid"
	DiskAccessIDField             = "diskaccessid"
	DiskEncryptionSetID           = "diskencryptionsetid"
	DiskIOPSReadWriteField        = "diskiopsreadwrite"
	DiskMBPSReadWriteField        = "diskmbpsreadwrite"
	DiskNameField                 = "diskname"
	EnableBurstingField           = "enablebursting"
	ErrDiskNotFound               = "not found"
	FsTypeField                   = "fstype"
	IncrementalField              = "incremental"
	KindField                     = "kind"
	LocationField                 = "location"
	LogicalSectorSizeField        = "logicalsectorsize"
	LUN                           = "LUN"
	MaxSharesField                = "maxshares"
	MinimumDiskSizeGiB            = 1
	NetworkAccessPolicyField      = "networkaccesspolicy"
	NotFound                      = "NotFound"
	PerfProfileBasic              = "basic"
	PerfProfileField              = "perfprofile"
	PerfProfileNone               = "none"
	PremiumAccountPrefix          = "premium"
	PvcNameKey                    = "csi.storage.k8s.io/pvc/name"
	PvcNamespaceKey               = "csi.storage.k8s.io/pvc/namespace"
	PvcNamespaceTag               = "kubernetes.io-created-for-pvc-namespace"
	PvcNameTag                    = "kubernetes.io-created-for-pvc-name"
	PvNameKey                     = "csi.storage.k8s.io/pv/name"
	PvNameTag                     = "kubernetes.io-created-for-pv-name"
	RateLimited                   = "rate limited"
	RequestedSizeGib              = "requestedsizegib"
	ResizeRequired                = "resizeRequired"
	SubscriptionIDField           = "subscriptionid"
	ResourceGroupField            = "resourcegroup"
	ResourceNotFound              = "ResourceNotFound"
	SkuNameField                  = "skuname"
	SourceDiskSearchMaxDepth      = 10
	SourceSnapshot                = "snapshot"
	SourceVolume                  = "volume"
	StandardSsdAccountPrefix      = "standardssd"
	StorageAccountTypeField       = "storageaccounttype"
	TagsField                     = "tags"
	ThrottlingKey                 = "throttlingKey"
	TrueValue                     = "true"
	FalseValue                    = "false"
	UserAgentField                = "useragent"
	VolumeAttributePartition      = "partition"
	WellKnownTopologyKey          = "topology.kubernetes.io/zone"
	WriteAcceleratorEnabled       = "writeacceleratorenabled"
	ZonedField                    = "zoned"
	EnableAsyncAttachField        = "enableasyncattach"
	TooManyRequests               = "TooManyRequests"
	ClientThrottled               = "client throttled"
	// define different sleep time when hit throttling
	SnapshotOpThrottlingSleepSec = 50
)

var (
	// ManagedDiskPath is described here: https://docs.microsoft.com/en-us/rest/api/compute/disks/createorupdate#create-a-managed-disk-from-an-existing-managed-disk-in-the-same-or-different-subscription.
	ManagedDiskPath   = "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s"
	ManagedDiskPathRE = regexp.MustCompile(`(?i).*/subscriptions/(?:.*)/resourceGroups/(?:.*)/providers/Microsoft.Compute/disks/(.+)`)
)
