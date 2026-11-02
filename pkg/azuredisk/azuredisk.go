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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	clientcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/mount-utils"
	"k8s.io/utils/ptr"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"

	consts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
	csicommon "sigs.k8s.io/azuredisk-csi-driver/pkg/csi-common"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/mounter"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/optimization"
	volumehelper "sigs.k8s.io/azuredisk-csi-driver/pkg/util"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient"
	azcache "sigs.k8s.io/cloud-provider-azure/pkg/cache"
	azurecloudconsts "sigs.k8s.io/cloud-provider-azure/pkg/consts"
	azure "sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

var (
	errPVNotFound            = errors.New("persistent volume not found")
	errPVMetadataUnavailable = errors.New("persistent volume metadata is unavailable")

	// taintRemovalBackoff is the exponential backoff configuration for node taint removal
	taintRemovalBackoff = wait.Backoff{
		Duration: 500 * time.Millisecond,
		Factor:   2,
		Steps:    10, // Max delay = 0.5 * 2^9 = ~4 minutes
	}
)

const (
	volumeAttachmentListTimeoutSeconds = 2
	informerCacheSyncTimeout           = 30 * time.Second
)

// CSIDriver defines the interface for a CSI driver.
type CSIDriver interface {
	csi.ControllerServer
	csi.NodeServer
	csi.IdentityServer

	Run(ctx context.Context) error
}

type hostUtil interface {
	PathIsDevice(string) (bool, error)
}

// Driver is the implementation of the Azure Disk CSI Driver.
type Driver struct {
	csicommon.CSIDriver
	// Embed UnimplementedXXXServer to ensure the driver returns Unimplemented for any
	// new RPC methods that might be introduced in future versions of the spec.
	csi.UnimplementedControllerServer
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	perfOptimizationEnabled            bool
	cloudConfigSecretName              string
	cloudConfigSecretNamespace         string
	customUserAgent                    string
	userAgentSuffix                    string
	cloud                              *azure.Cloud
	clientFactory                      azclient.ClientFactory
	diskController                     *ManagedDiskController
	eventRecorder                      record.EventRecorder
	migrationMonitor                   *MigrationProgressMonitor
	mounter                            *mount.SafeFormatAndMount
	deviceHelper                       optimization.Interface
	nodeInfo                           *optimization.NodeInfo
	ioHandler                          azureutils.IOHandler
	hostUtil                           hostUtil
	useCSIProxyGAInterface             bool
	enableDiskOnlineResize             bool
	allowEmptyCloudConfig              bool
	enableListVolumes                  bool
	enableListSnapshots                bool
	supportZone                        bool
	getNodeInfoFromLabels              bool
	enableDiskCapacityCheck            bool
	enableTrafficManager               bool
	trafficManagerPort                 int64
	vmssCacheTTLInSeconds              int64
	listVMSSWithInstanceView           bool
	volStatsCacheExpireInMinutes       int64
	attachDetachInitialDelayInMs       int64
	detachOperationMinTimeoutInSeconds int64
	vmssDetachTimeoutInSeconds         int64
	getDiskTimeoutInSeconds            int64
	vmType                             string
	enableWindowsHostProcess           bool
	useWinCIMAPI                       bool
	getNodeIDFromIMDS                  bool
	enableOtelTracing                  bool
	shouldWaitForSnapshotReady         bool
	checkDiskLUNCollision              bool
	checkDiskCountForBatching          bool
	forceDetachBackoff                 bool
	waitForDetach                      bool
	waitForDetachDiskComplete          bool
	endpoint                           string
	disableAVSetNodes                  bool
	removeNotReadyTaint                bool
	neverStopTaintRemoval              bool
	kubeClient                         clientset.Interface
	enableKataMount                    bool
	isKataNode                         bool
	kataDirectVolume                   kataDirectVolumer
	// a timed cache storing volume stats <volumeID, volumeStats>
	volStatsCache           azcache.Resource
	maxConcurrentFormat     int64
	concurrentFormatTimeout int64
	// formatSem limits the number of concurrent mkfs operations, mirroring
	// mount.SafeFormatAndMount's WithMaxConcurrentFormat semaphore. nil means unlimited.
	formatSem               chan any
	formatTimeout           time.Duration
	enableMinimumRetryAfter bool
	volumeLocks             *volumehelper.VolumeLocks
	// a timed cache for throttling
	throttlingCache azcache.Resource
	// a timed cache for disk lun collision check throttling
	checkDiskLunThrottlingCache azcache.Resource
	enableMigrationMonitor      bool
	// whether to convert ReadWrite cachingMode to ReadOnly for intree PVs to avoid issues
	convertRWCachingModeForIntreePV bool
	nodeLister                      cache.GenericLister
	nodeInformerSynced              cache.InformerSynced
	nodeInformerFactory             metadatainformer.SharedInformerFactory
	// HTTP client for wireserver calls
	httpClient *http.Client
	// nodeDrivenAttachDetachEnabled controls adoption of the Alpha node-driven attach/detach architecture.
	nodeDrivenAttachDetachEnabled bool
	// informer factory and PV lister for cached API access
	informerFactory informers.SharedInformerFactory
	pvLister        corelisters.PersistentVolumeLister
	pvListerSynced  cache.InformerSynced
	// pvIndexer resolves a disk URI to its PV without listing every PV
	pvIndexer cache.Indexer
	// owning AKS cluster ARM ID, resolved once from node resource group tags and reused thereafter
	clusterResourceID     string
	clusterResourceIDLock sync.Mutex
	// interval between Azure async operation polls; defaults to 5s when unset
	pollInterval time.Duration
}

// NewDriver Creates a NewCSIDriver object. Assumes vendor version is equal to driver version &
// does not support optional driver plugin info manifest field. Refer to CSI spec for more details.
func NewDriver(options *DriverOptions) *Driver {
	driver := Driver{}
	if options.FeatureGates == nil {
		options.FeatureGates = NewDriverFeatureGate()
	}
	driver.Name = options.DriverName
	driver.Version = driverVersion
	driver.NodeID = options.NodeID
	driver.VolumeAttachLimit = options.VolumeAttachLimit
	driver.ReservedDataDiskSlotNum = options.ReservedDataDiskSlotNum
	driver.perfOptimizationEnabled = options.EnablePerfOptimization
	driver.cloudConfigSecretName = options.CloudConfigSecretName
	driver.cloudConfigSecretNamespace = options.CloudConfigSecretNamespace
	driver.customUserAgent = options.CustomUserAgent
	driver.userAgentSuffix = options.UserAgentSuffix
	driver.useCSIProxyGAInterface = options.UseCSIProxyGAInterface
	driver.enableDiskOnlineResize = options.EnableDiskOnlineResize
	driver.allowEmptyCloudConfig = options.AllowEmptyCloudConfig
	driver.enableListVolumes = options.EnableListVolumes
	driver.enableListSnapshots = options.EnableListVolumes
	driver.supportZone = options.SupportZone
	driver.getNodeInfoFromLabels = options.GetNodeInfoFromLabels
	driver.enableDiskCapacityCheck = options.EnableDiskCapacityCheck
	driver.attachDetachInitialDelayInMs = options.AttachDetachInitialDelayInMs
	driver.detachOperationMinTimeoutInSeconds = options.DetachOperationMinTimeoutInSeconds
	driver.vmssDetachTimeoutInSeconds = options.VMSSDetachTimeoutInSeconds
	driver.enableTrafficManager = options.EnableTrafficManager
	driver.trafficManagerPort = options.TrafficManagerPort
	driver.vmssCacheTTLInSeconds = options.VMSSCacheTTLInSeconds
	driver.listVMSSWithInstanceView = options.ListVMSSWithInstanceView
	driver.volStatsCacheExpireInMinutes = options.VolStatsCacheExpireInMinutes
	driver.getDiskTimeoutInSeconds = options.GetDiskTimeoutInSeconds
	driver.vmType = options.VMType
	driver.enableWindowsHostProcess = options.EnableWindowsHostProcess
	driver.useWinCIMAPI = options.UseWinCIMAPI
	driver.getNodeIDFromIMDS = options.GetNodeIDFromIMDS
	driver.enableOtelTracing = options.EnableOtelTracing
	driver.shouldWaitForSnapshotReady = options.WaitForSnapshotReady
	driver.checkDiskLUNCollision = options.CheckDiskLUNCollision
	driver.checkDiskCountForBatching = options.CheckDiskCountForBatching
	driver.forceDetachBackoff = options.ForceDetachBackoff
	driver.waitForDetach = options.WaitForDetach
	driver.waitForDetachDiskComplete = options.WaitForDetachDiskComplete
	driver.endpoint = options.Endpoint
	driver.disableAVSetNodes = options.DisableAVSetNodes
	driver.removeNotReadyTaint = options.RemoveNotReadyTaint
	driver.neverStopTaintRemoval = options.NeverStopTaintRemoval
	driver.maxConcurrentFormat = options.MaxConcurrentFormat
	driver.concurrentFormatTimeout = options.ConcurrentFormatTimeout
	if options.MaxConcurrentFormat > 0 {
		driver.formatSem = make(chan any, int(options.MaxConcurrentFormat))
		driver.formatTimeout = time.Duration(options.ConcurrentFormatTimeout) * time.Second
	}
	driver.enableMinimumRetryAfter = options.EnableMinimumRetryAfter
	driver.nodeDrivenAttachDetachEnabled = options.FeatureGates.Enabled(NodeDrivenAttachDetach)
	if driver.nodeDrivenAttachDetachEnabled {
		klog.Warningf("Alpha feature gate %s is enabled", NodeDrivenAttachDetach)
	}
	driver.volumeLocks = volumehelper.NewVolumeLocks()
	driver.ioHandler = azureutils.NewOSIOHandler()
	driver.hostUtil = hostutil.NewHostUtil()
	driver.enableKataMount = options.FeatureGates.Enabled(KataMount)
	if driver.enableKataMount {
		klog.Warningf("Alpha feature gate %s is enabled", KataMount)
	}
	driver.kataDirectVolume = &kataDirectVolume{}
	driver.enableMigrationMonitor = options.EnableMigrationMonitor
	driver.convertRWCachingModeForIntreePV = options.ConvertRWCachingModeForIntreePV

	if driver.NodeID == "" {
		// nodeid is not needed in controller component
		klog.Warning("nodeid is empty")
	}
	topologyKey = fmt.Sprintf("topology.%s/zone", driver.Name)

	getter := func(_ context.Context, _ string) (interface{}, error) { return nil, nil }
	var err error
	if driver.throttlingCache, err = azcache.NewTimedCache(5*time.Minute, getter, false); err != nil {
		klog.Fatalf("%v", err)
	}
	if driver.checkDiskLunThrottlingCache, err = azcache.NewTimedCache(30*time.Minute, getter, false); err != nil {
		klog.Fatalf("%v", err)
	}

	if options.VolStatsCacheExpireInMinutes <= 0 {
		options.VolStatsCacheExpireInMinutes = 10 // default expire in 10 minutes
	}
	if driver.volStatsCache, err = azcache.NewTimedCache(time.Duration(options.VolStatsCacheExpireInMinutes)*time.Minute, getter, false); err != nil {
		klog.Fatalf("%v", err)
	}

	userAgent := GetUserAgent(driver.Name, driver.customUserAgent, driver.userAgentSuffix)
	klog.V(2).Infof("driver userAgent: %s", userAgent)

	kubeConfig, err := azureutils.GetKubeConfig(options.Kubeconfig, options.KubeAPIQPS, options.KubeAPIBurst)
	if err != nil {
		klog.Warningf("get kubeconfig(%s) failed with error: %v", options.Kubeconfig, err)
	}
	var kubeClient clientset.Interface
	if kubeConfig != nil {
		kubeClient, err = clientset.NewForConfig(kubeConfig)
		if err != nil {
			klog.Warningf("get kubeclient failed with error: %v", err)
		}
	}
	driver.kubeClient = kubeClient

	if driver.NodeID != "" {
		// Initialize HTTP client for wireserver calls (node component only)
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		driver.httpClient = &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		}
	}

	if driver.enableKataMount {
		if err := driver.initKataNode(context.Background()); err != nil {
			klog.Fatalf("failed to initialize Kata node configuration: %v", err)
		}
	}

	cloud, err := azureutils.GetCloudProviderFromClient(context.Background(), kubeClient, driver.cloudConfigSecretName, driver.cloudConfigSecretNamespace,
		userAgent, driver.allowEmptyCloudConfig, driver.enableTrafficManager, driver.enableMinimumRetryAfter, driver.trafficManagerPort)
	if err != nil {
		klog.Fatalf("failed to get Azure Cloud Provider, error: %v", err)
	}
	driver.cloud = cloud

	if driver.cloud != nil {
		driver.clientFactory = driver.cloud.ComputeClientFactory
		if driver.vmType != "" {
			klog.V(2).Infof("override VMType(%s) in cloud config as %s", driver.cloud.VMType, driver.vmType)
			driver.cloud.VMType = driver.vmType
		}

		if driver.NodeID == "" {
			// Disable UseInstanceMetadata for controller to mitigate a timeout issue using IMDS
			// https://github.com/kubernetes-sigs/azuredisk-csi-driver/issues/168
			klog.V(2).Infof("disable UseInstanceMetadata for controller")
			driver.cloud.Config.UseInstanceMetadata = false

			if driver.cloud.VMType == azurecloudconsts.VMTypeStandard && driver.cloud.DisableAvailabilitySetNodes {
				klog.V(2).Infof("set DisableAvailabilitySetNodes as false since VMType is %s", driver.cloud.VMType)
				driver.cloud.DisableAvailabilitySetNodes = false
			}

			if driver.cloud.VMType == azurecloudconsts.VMTypeVMSS && !driver.cloud.DisableAvailabilitySetNodes && driver.disableAVSetNodes {
				klog.V(2).Infof("DisableAvailabilitySetNodes for controller since current VMType is vmss")
				driver.cloud.DisableAvailabilitySetNodes = true
			}
			klog.V(2).Infof("cloud: %s, location: %s, rg: %s, VMType: %s, PrimaryScaleSetName: %s, PrimaryAvailabilitySetName: %s, DisableAvailabilitySetNodes: %v", driver.cloud.Cloud, driver.cloud.Location, driver.cloud.ResourceGroup, driver.cloud.VMType, driver.cloud.PrimaryScaleSetName, driver.cloud.PrimaryAvailabilitySetName, driver.cloud.DisableAvailabilitySetNodes)
		}

		klog.V(2).Infof("vmssCacheTTLInSeconds: %d, listVMSSWithInstanceView: %v",
			driver.vmssCacheTTLInSeconds, driver.listVMSSWithInstanceView)
		driver.cloud.ListVmssVirtualMachinesWithoutInstanceView = !driver.listVMSSWithInstanceView

		if driver.vmssCacheTTLInSeconds > 0 {
			klog.V(2).Infof("reset vmssCacheTTLInSeconds as %d", driver.vmssCacheTTLInSeconds)
			driver.cloud.VMCacheTTLInSeconds = int(driver.vmssCacheTTLInSeconds)
			driver.cloud.VmssCacheTTLInSeconds = int(driver.vmssCacheTTLInSeconds)
		}

		driver.diskController = NewManagedDiskController(driver.cloud)
		driver.diskController.AttachDetachInitialDelayInMs = int(driver.attachDetachInitialDelayInMs)
		driver.diskController.ForceDetachBackoff = driver.forceDetachBackoff
		driver.diskController.WaitForDetach = driver.waitForDetach
		driver.diskController.WaitForDetachDiskComplete = driver.waitForDetachDiskComplete
		driver.diskController.CheckDiskCountForBatching = driver.checkDiskCountForBatching
		driver.diskController.DetachOperationMinTimeoutInSeconds = int(driver.detachOperationMinTimeoutInSeconds)
		driver.diskController.VMSSDetachTimeoutInSeconds = int(driver.vmssDetachTimeoutInSeconds)
		klog.V(2).Infof("set DetachOperationMinTimeoutInSeconds as %d", driver.diskController.DetachOperationMinTimeoutInSeconds)
		if driver.diskController.DetachOperationMinTimeoutInSeconds <= 0 {
			klog.V(2).Infof("reset DetachOperationMinTimeoutInSeconds as %d", defaultDetachOperationMinTimeoutInSeconds)
			driver.diskController.DetachOperationMinTimeoutInSeconds = defaultDetachOperationMinTimeoutInSeconds
		}
		klog.V(2).Infof("set VMSSDetachTimeoutInSeconds as %d", driver.diskController.VMSSDetachTimeoutInSeconds)
		if driver.diskController.VMSSDetachTimeoutInSeconds <= 0 || driver.diskController.VMSSDetachTimeoutInSeconds > driver.diskController.DetachOperationMinTimeoutInSeconds {
			if driver.diskController.DetachOperationMinTimeoutInSeconds <= defaultVMSSDetachTimeoutInSeconds {
				klog.V(2).Infof("reset VMSSDetachTimeoutInSeconds as DetachOperationMinTimeoutInSeconds %d with no additional polling", driver.diskController.DetachOperationMinTimeoutInSeconds)
				driver.diskController.VMSSDetachTimeoutInSeconds = driver.diskController.DetachOperationMinTimeoutInSeconds
			} else {
				klog.V(2).Infof("reset VMSSDetachTimeoutInSeconds as %d (default)", defaultVMSSDetachTimeoutInSeconds)
				driver.diskController.VMSSDetachTimeoutInSeconds = defaultVMSSDetachTimeoutInSeconds
			}
		}

		if kubeClient != nil && driver.NodeID == "" && driver.enableMigrationMonitor {
			eventBroadcaster := record.NewBroadcaster()
			eventBroadcaster.StartRecordingToSink(&clientcorev1.EventSinkImpl{
				Interface: kubeClient.CoreV1().Events(""),
			})
			eventRecorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{
				Component: driver.Name,
			})
			driver.eventRecorder = eventRecorder
			driver.migrationMonitor = NewMigrationProgressMonitor(kubeClient, eventRecorder, driver.diskController)

			// Recover any ongoing migrations after restart
			go func() {
				time.Sleep(30 * time.Second) // Wait for controller to fully start
				// Periodically check every 10 minutes - in worst case kubernetes administrator can add the label to subscribe the events
				for {
					if err := driver.recoverMigrationMonitorsFromLabels(context.Background()); err != nil {
						klog.Errorf("Failed to recover migration monitors: %v", err)
					}
					time.Sleep(10 * time.Minute)
				}
			}()
		}
	}

	if kubeConfig != nil && driver.checkDiskCountForBatching && driver.NodeID == "" {
		// Create a metadata-only node informer to cache node labels locally (controller only)
		metadataClient, err := metadata.NewForConfig(kubeConfig)
		if err != nil {
			klog.Warningf("failed to create metadata client: %v, node informer will not be used", err)
		} else {
			driver.nodeInformerFactory = metadatainformer.NewSharedInformerFactory(metadataClient, 0)
			nodeGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
			nodeInformer := driver.nodeInformerFactory.ForResource(nodeGVR)
			driver.nodeLister = nodeInformer.Lister()
			driver.nodeInformerSynced = nodeInformer.Informer().HasSynced
			if driver.diskController != nil {
				driver.diskController.nodeLister = driver.nodeLister
			}
		}
	}

	driver.deviceHelper = optimization.NewSafeDeviceHelper()

	if driver.getPerfOptimizationEnabled() {
		driver.nodeInfo, err = optimization.NewNodeInfo(context.TODO(), driver.getCloud(), driver.NodeID)
		if err != nil {
			klog.Warningf("Failed to get node info. Error: %v", err)
		}
	}

	driver.mounter, err = mounter.NewSafeMounter(driver.enableWindowsHostProcess, driver.useWinCIMAPI, driver.useCSIProxyGAInterface, int(driver.maxConcurrentFormat), time.Duration(driver.concurrentFormatTimeout)*time.Second)
	if err != nil {
		klog.Fatalf("Failed to get safe mounter. Error: %v", err)
	}

	controllerCap := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
		csi.ControllerServiceCapability_RPC_MODIFY_VOLUME,
	}
	if driver.enableListVolumes {
		controllerCap = append(controllerCap, csi.ControllerServiceCapability_RPC_LIST_VOLUMES, csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES)
	}
	if driver.enableListSnapshots {
		controllerCap = append(controllerCap, csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS)
	}

	driver.AddControllerServiceCapabilities(controllerCap)
	driver.AddVolumeCapabilityAccessModes(
		[]csi.VolumeCapability_AccessMode_Mode{
			csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER,
		})
	driver.AddNodeServiceCapabilities([]csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
		csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
	})

	driver.initializePVInformer(kubeClient)

	if kubeClient != nil && driver.removeNotReadyTaint && driver.NodeID != "" {
		// Remove taint from node to indicate driver startup success
		// This is done at the last possible moment to prevent race conditions or false positive removals
		time.AfterFunc(time.Duration(options.TaintRemovalInitialDelayInSeconds)*time.Second, func() {
			removeTaintInBackground(kubeClient, driver.NodeID, driver.Name, taintRemovalBackoff, driver.neverStopTaintRemoval, removeNotReadyTaint)
		})
	}
	return &driver
}

func (d *Driver) initializePVInformer(kubeClient clientset.Interface) {
	if kubeClient == nil || !d.nodeDrivenAttachDetachEnabled {
		return
	}

	d.informerFactory = informers.NewSharedInformerFactory(kubeClient, 10*time.Minute)
	pvInformer := d.informerFactory.Core().V1().PersistentVolumes()
	if err := pvInformer.Informer().AddIndexers(cache.Indexers{pvDiskURIIndex: pvDiskURIIndexFunc}); err != nil {
		klog.Warningf("failed to add diskURI indexer to PV informer, will fall back to API list: %v", err)
	} else {
		d.pvIndexer = pvInformer.Informer().GetIndexer()
	}
	d.pvLister = pvInformer.Lister()
	d.pvListerSynced = pvInformer.Informer().HasSynced
}

func (d *Driver) waitForPVInformerCacheSync(ctx context.Context, timeout time.Duration) bool {
	syncCtx, syncCancel := context.WithTimeout(ctx, timeout)
	defer syncCancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), d.pvListerSynced) {
		d.pvIndexer = nil
		return false
	}
	return true
}

// Run driver initialization
func (d *Driver) Run(ctx context.Context) error {
	versionMeta, err := GetVersionYAML(d.Name)
	if err != nil {
		klog.Fatalf("%v", err)
	}
	klog.Infof("\nDRIVER INFORMATION:\n-------------------\n%s\n\nStreaming logs below:", versionMeta)

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			grpcprom.NewServerMetrics().UnaryServerInterceptor(),
			csicommon.LogGRPC,
		),
	}
	if d.enableOtelTracing {
		tracerProvider, err := InitOtelTracing()
		if err != nil {
			klog.Fatalf("Failed to initialize otel tracing: %v", err)
		}
		// TracerProvider will flush traces on shutdown
		defer func() {
			if err := tracerProvider.Shutdown(context.Background()); err != nil {
				klog.Errorf("Could not shutdown otel tracerProvider: %v", err)
			}
		}()
		opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}

	s := grpc.NewServer(opts...)
	csi.RegisterIdentityServer(s, d)
	csi.RegisterControllerServer(s, d)
	csi.RegisterNodeServer(s, d)

	// Start the node informer if it was set up during driver initialization
	if d.nodeInformerFactory != nil {
		d.nodeInformerFactory.Start(ctx.Done())
		syncCtx, syncCancel := context.WithTimeout(ctx, informerCacheSyncTimeout)
		defer syncCancel()
		if !cache.WaitForCacheSync(syncCtx.Done(), d.nodeInformerSynced) {
			klog.Warningf("metadata node informer cache has not synced yet, will continue to sync in background")
		} else {
			klog.V(2).Infof("metadata node informer cache synced successfully")
		}
		klog.V(2).Infof("started metadata node informer for GetNodeInfoFromLabels caching")
	}

	// Start informer factory if initialized
	if d.informerFactory != nil {
		d.informerFactory.Start(ctx.Done())
		if !d.waitForPVInformerCacheSync(ctx, informerCacheSyncTimeout) {
			klog.Warningf("PV informer cache has not synced, falling back to direct API access")
		} else {
			klog.V(2).Infof("PV informer cache synced successfully")
		}
	}

	go func() {
		//graceful shutdown
		<-ctx.Done()

		// Shutdown informer factory
		if d.informerFactory != nil {
			d.informerFactory.Shutdown()
		}

		// Stop migration monitor if it exists
		if d.migrationMonitor != nil {
			d.migrationMonitor.Stop()
		}

		// Shutdown node informer if it was started
		if d.nodeInformerFactory != nil {
			d.nodeInformerFactory.Shutdown()
		}

		s.GracefulStop()
	}()
	// Driver d act as IdentityServer, ControllerServer and NodeServer
	listener, err := csicommon.Listen(ctx, d.endpoint)
	if err != nil {
		klog.Fatalf("failed to listen to endpoint, error: %v", err)
	}
	err = s.Serve(listener)
	if errors.Is(err, grpc.ErrServerStopped) {
		klog.Infof("gRPC server stopped serving")
		return nil
	}
	return err
}

func (d *Driver) isGetDiskThrottled(ctx context.Context) bool {
	cache, err := d.throttlingCache.Get(ctx, consts.GetDiskThrottlingKey, azcache.CacheReadTypeDefault)
	if err != nil {
		klog.Warningf("throttlingCache(%s) return with error: %s", consts.GetDiskThrottlingKey, err)
		return false
	}
	return cache != nil
}

func (d *Driver) isCheckDiskLunThrottled(ctx context.Context) bool {
	cache, err := d.checkDiskLunThrottlingCache.Get(ctx, consts.CheckDiskLunThrottlingKey, azcache.CacheReadTypeDefault)
	if err != nil {
		klog.Warningf("throttlingCache(%s) return with error: %s", consts.CheckDiskLunThrottlingKey, err)
		return false
	}
	return cache != nil
}

func (d *Driver) checkDiskExists(ctx context.Context, diskURI string) (disk *armcompute.Disk, err error) {
	ctx, span := startSpan(ctx, "checkDiskExists",
		attribute.String(attrDiskURI, diskURI))
	defer func() {
		recordSpanResult(span, err)
		recordThrottleIfThrottled(ctx, err)
		span.End()
	}()

	if d.isGetDiskThrottled(ctx) {
		recordThrottleEvent(ctx, eventThrottled, "")
		klog.Warningf("skip checkDiskExists(%s) since it's still in throttling", diskURI)
		return nil, nil
	}

	newCtx, cancel := context.WithTimeout(ctx, time.Duration(d.getDiskTimeoutInSeconds)*time.Second)
	defer cancel()
	return d.diskController.GetDiskByURI(newCtx, diskURI)
}

func (d *Driver) checkDiskCapacity(ctx context.Context, subsID, resourceGroup, diskName string, requestGiB int) (bool, error) {
	if d.isGetDiskThrottled(ctx) {
		klog.Warningf("skip checkDiskCapacity(%s, %s) since it's still in throttling", resourceGroup, diskName)
		return true, nil
	}
	disk, err := d.diskController.GetDisk(ctx, subsID, resourceGroup, diskName)
	// Because we can not judge the reason of the error. Maybe the disk does not exist.
	// So here we do not handle the error.
	if err == nil {
		if !reflect.DeepEqual(disk, armcompute.Disk{}) && disk.Properties.DiskSizeGB != nil && int(*disk.Properties.DiskSizeGB) != requestGiB {
			return false, status.Errorf(codes.AlreadyExists, "the request volume already exists, but its capacity(%v) is different from (%v)", *disk.Properties.DiskSizeGB, requestGiB)
		}
	}
	return true, nil
}

func (d *Driver) getVolumeLocks() *volumehelper.VolumeLocks {
	return d.volumeLocks
}

// setControllerCapabilities sets the controller capabilities field. It is intended for use with unit tests.
func (d *Driver) setControllerCapabilities(caps []*csi.ControllerServiceCapability) {
	d.Cap = caps
}

// setNodeCapabilities sets the node capabilities field. It is intended for use with unit tests.
func (d *Driver) setNodeCapabilities(nodeCaps []*csi.NodeServiceCapability) {
	d.NSCap = nodeCaps
}

// setName sets the Name field. It is intended for use with unit tests.
func (d *Driver) setName(name string) {
	d.Name = name
}

// setName sets the NodeId field. It is intended for use with unit tests.
func (d *Driver) setNodeID(nodeID string) {
	d.NodeID = nodeID
}

// setName sets the Version field. It is intended for use with unit tests.
func (d *Driver) setVersion(version string) {
	d.Version = version
}

// getCloud returns the value of the cloud field. It is intended for use with unit tests.
func (d *Driver) getCloud() *azure.Cloud {
	return d.cloud
}

// setCloud sets the cloud field. It is intended for use with unit tests.
func (d *Driver) setCloud(cloud *azure.Cloud) {
	d.cloud = cloud
}

// getMounter returns the value of the mounter field. It is intended for use with unit tests.
func (d *Driver) getMounter() *mount.SafeFormatAndMount {
	return d.mounter
}

// setMounter sets the mounter field. It is intended for use with unit tests.
func (d *Driver) setMounter(mounter *mount.SafeFormatAndMount) {
	d.mounter = mounter
}

// getPerfOptimizationEnabled returns the value of the perfOptimizationEnabled field. It is intended for use with unit tests.
func (d *Driver) getPerfOptimizationEnabled() bool {
	return d.perfOptimizationEnabled
}

// setPerfOptimizationEnabled sets the value of the perfOptimizationEnabled field. It is intended for use with unit tests.
func (d *Driver) setPerfOptimizationEnabled(enabled bool) {
	d.perfOptimizationEnabled = enabled
}

// getDeviceHelper returns the value of the deviceHelper field. It is intended for use with unit tests.
func (d *Driver) getDeviceHelper() optimization.Interface {
	return d.deviceHelper
}

// getNodeInfo returns the value of the nodeInfo field. It is intended for use with unit tests.
func (d *Driver) getNodeInfo() *optimization.NodeInfo {
	return d.nodeInfo
}

func (d *Driver) getHostUtil() hostUtil {
	return d.hostUtil
}

// getSnapshotCompletionPercent returns the completion percent of snapshot
func (d *Driver) getSnapshotCompletionPercent(ctx context.Context, subsID, resourceGroup, snapshotName string) (float32, error) {
	snapshotClient, err := d.clientFactory.GetSnapshotClientForSub(subsID)
	if err != nil {
		return 0.0, err
	}
	copySnapshot, err := snapshotClient.Get(ctx, resourceGroup, snapshotName)
	if err != nil {
		return 0.0, err
	}

	if copySnapshot.Properties == nil || copySnapshot.Properties.CompletionPercent == nil {
		// If CompletionPercent is nil, it means the snapshot is complete
		klog.V(2).Infof("snapshot(%s) under rg(%s) has no SnapshotProperties or CompletionPercent is nil", snapshotName, resourceGroup)
		return 100.0, nil
	}

	return *copySnapshot.Properties.CompletionPercent, nil
}

// waitForSnapshotReady wait for completionPercent of snapshot is 100.0
func (d *Driver) waitForSnapshotReady(ctx context.Context, subsID, resourceGroup, snapshotName string, intervel, timeout time.Duration) error {
	completionPercent, err := d.getSnapshotCompletionPercent(ctx, subsID, resourceGroup, snapshotName)
	if err != nil {
		return err
	}

	if completionPercent >= float32(100.0) {
		klog.V(2).Infof("snapshot(%s) under rg(%s) complete", snapshotName, resourceGroup)
		return nil
	}

	timeTick := time.Tick(intervel)
	timeAfter := time.After(timeout)
	for {
		select {
		case <-timeTick:
			completionPercent, err = d.getSnapshotCompletionPercent(ctx, subsID, resourceGroup, snapshotName)
			if err != nil {
				return err
			}

			if completionPercent >= float32(100.0) {
				klog.V(2).Infof("snapshot(%s) under rg(%s) complete", snapshotName, resourceGroup)
				return nil
			}
			klog.V(2).Infof("snapshot(%s) under rg(%s) completionPercent: %f", snapshotName, resourceGroup, completionPercent)
		case <-timeAfter:
			return fmt.Errorf("timeout waiting for snapshot(%s) under rg(%s)", snapshotName, resourceGroup)
		}
	}
}

// getUsedLunsFromVolumeAttachments returns a list of used luns from VolumeAttachments
func (d *Driver) getUsedLunsFromVolumeAttachments(ctx context.Context, nodeName string) ([]int, error) {
	kubeClient := d.cloud.KubeClient
	if kubeClient == nil || kubeClient.StorageV1() == nil || kubeClient.StorageV1().VolumeAttachments() == nil {
		return nil, fmt.Errorf("kubeClient or kubeClient.StorageV1() or kubeClient.StorageV1().VolumeAttachments() is nil")
	}

	kubeSpanCtx, kubeSpan := startSpan(ctx, "ListVolumeAttachments", attribute.String(attrNode, nodeName))
	volumeAttachments, err := kubeClient.StorageV1().VolumeAttachments().List(kubeSpanCtx, metav1.ListOptions{
		TimeoutSeconds: ptr.To(int64(volumeAttachmentListTimeoutSeconds))})
	recordSpanResult(kubeSpan, err)
	kubeSpan.End()
	if err != nil {
		return nil, err
	}

	usedLuns := make([]int, 0)
	if volumeAttachments == nil {
		klog.V(2).Infof("volumeAttachments is nil")
		return usedLuns, nil
	}

	klog.V(2).Infof("volumeAttachments count: %d, nodeName: %s", len(volumeAttachments.Items), nodeName)
	for _, va := range volumeAttachments.Items {
		klog.V(6).Infof("attacher: %s, nodeName: %s, Status: %v, PV: %s, attachmentMetadata: %v", va.Spec.Attacher, va.Spec.NodeName,
			va.Status.Attached, ptr.Deref(va.Spec.Source.PersistentVolumeName, ""), va.Status.AttachmentMetadata)
		if va.Spec.Attacher == d.Name && strings.EqualFold(va.Spec.NodeName, nodeName) && va.Status.Attached {
			if k, ok := va.Status.AttachmentMetadata[consts.LUN]; ok {
				lun, err := strconv.Atoi(k)
				if err != nil {
					klog.Warningf("VolumeAttachment(%s) lun(%s) is not a valid integer", va.Name, k)
					continue
				}
				usedLuns = append(usedLuns, lun)
			}
		}
	}
	return usedLuns, nil
}

// getUsedLunsFromNode returns a list of sorted used luns from Node
func (d *Driver) getUsedLunsFromNode(ctx context.Context, nodeName k8stypes.NodeName) ([]int, error) {
	disks, _, err := d.diskController.GetNodeDataDisks(ctx, nodeName, azcache.CacheReadTypeDefault)
	if err != nil {
		klog.Errorf("error of getting data disks for node %s: %v", nodeName, err)
		return nil, err
	}

	usedLuns := make([]int, 0)
	// get all disks attached to the node
	for _, disk := range disks {
		if disk.Lun == nil {
			klog.Warningf("disk(%s) lun is nil", *disk.Name)
			continue
		}
		usedLuns = append(usedLuns, int(*disk.Lun))
	}
	return usedLuns, nil
}

// GetNodeInfoFromNodeLister gets zone, instanceType from node labels using the cached nodeLister.
func GetNodeInfoFromNodeLister(nodeName string, nodeLister cache.GenericLister) (string, string, error) {
	if nodeLister == nil {
		return "", "", fmt.Errorf("nodeLister is nil")
	}

	obj, err := nodeLister.Get(nodeName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			klog.V(4).Infof("GetNodeInfoFromNodeLister: node(%s) not found in lister cache", nodeName)
			return "", "", nil
		}
		return "", "", fmt.Errorf("get node(%s) from lister failed: %v", nodeName, err)
	}

	pom, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		return "", "", fmt.Errorf("node(%s) from lister is not *metav1.PartialObjectMetadata", nodeName)
	}

	if len(pom.Labels) == 0 {
		return "", "", fmt.Errorf("node(%s) label is empty", nodeName)
	}

	zone := pom.Labels[consts.WellKnownTopologyKey]
	instanceType := pom.Labels[consts.InstanceTypeKey]
	klog.V(4).Infof("GetNodeInfoFromNodeLister: node(%s): zone=%s, instanceType=%s", nodeName, zone, instanceType)
	return zone, instanceType, nil
}

// pvDiskURIIndex is the informer index that maps a PV to its CSI disk URI
// (volume handle) for O(1) lookups in getPVFromDiskURI.
const pvDiskURIIndex = "diskURI"

// pvDiskURIIndexFunc indexes PersistentVolumes by their lowercased CSI volume
// handle so a disk URI resolves to its PV without listing every PV.
func pvDiskURIIndexFunc(obj interface{}) ([]string, error) {
	pv, ok := obj.(*v1.PersistentVolume)
	if !ok || pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle == "" {
		return nil, nil
	}
	return []string{strings.ToLower(pv.Spec.CSI.VolumeHandle)}, nil
}

func (d *Driver) getPVFromDiskURI(ctx context.Context, diskURI string) (*v1.PersistentVolume, error) {
	klog.Infof("Looking for PV with handle %s", diskURI)

	if d.kubeClient == nil {
		return nil, fmt.Errorf("%w: Kubernetes client is not initialized", errPVMetadataUnavailable)
	}

	// Use the cache only to identify a candidate PV name. QAD routing depends on
	// current annotations, so always read the candidate from the API.
	if d.pvIndexer != nil {
		objs, err := d.pvIndexer.ByIndex(pvDiskURIIndex, strings.ToLower(diskURI))
		if err != nil {
			klog.Warningf("failed to look up PersistentVolume by disk URI from cache, falling back to API list: %v", err)
		} else {
			for _, obj := range objs {
				candidate, ok := obj.(*v1.PersistentVolume)
				if !ok || candidate.Name == "" || candidate.Spec.CSI == nil || candidate.Spec.CSI.Driver != d.Name ||
					!strings.EqualFold(candidate.Spec.CSI.VolumeHandle, diskURI) {
					continue
				}
				pv, getErr := d.kubeClient.CoreV1().PersistentVolumes().Get(ctx, candidate.Name, metav1.GetOptions{})
				if getErr != nil {
					if apierrors.IsNotFound(getErr) {
						break
					}
					return nil, fmt.Errorf("failed to get PersistentVolume %s: %v", candidate.Name, getErr)
				}
				if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == d.Name &&
					strings.EqualFold(pv.Spec.CSI.VolumeHandle, diskURI) {
					klog.Infof("Found PV %s with handle %s (refreshed from API)", pv.Name, diskURI)
					return pv, nil
				}
				break
			}
		}
	}

	// The informer may not have observed a newly created PV yet.
	pvList, err := d.kubeClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list PersistentVolumes: %v", err)
	}
	for _, pv := range pvList.Items {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == d.Name &&
			strings.EqualFold(pv.Spec.CSI.VolumeHandle, diskURI) {
			klog.Infof("Found PV %s with handle %s", pv.Name, diskURI)
			return &pv, nil
		}
	}
	return nil, fmt.Errorf("%w with diskURI(%s)", errPVNotFound, diskURI)
}

// hasQADInfo reports whether the PV carries the QAD adoption marker (the
// attach-sequence annotation) that selects the node-driven attach/detach path.
func (d *Driver) hasQADInfo(pv *v1.PersistentVolume) bool {
	if pv == nil {
		klog.V(2).Infof("PV is nil")
		return false
	}

	pvName := pv.Name
	if pvName == "" {
		klog.V(2).Infof("PV name is empty")
		return false
	}

	// The attach-sequence annotation, seeded during controller-side adoption, is
	// the signal that this PV uses the QAD attach/detach path.
	if attachSequence, exists := pv.Annotations[consts.AttachSequenceAnnotation]; exists {
		klog.V(2).Infof("Found PV %s with attach sequence: %s", pvName, attachSequence)
		return true
	}
	klog.V(2).Infof("Found PV %s but no QAD configuration", pvName)
	return false
}

// getNodeInfoFromLabels get zone, instanceType from node labels
func GetNodeInfoFromLabels(ctx context.Context, nodeName string, kubeClient clientset.Interface) (string, string, error) {
	if kubeClient == nil || kubeClient.CoreV1() == nil {
		return "", "", fmt.Errorf("kubeClient is nil")
	}

	kubeSpanCtx, kubeSpan := startSpan(ctx, "GetNode", attribute.String(attrNode, nodeName))
	node, err := kubeClient.CoreV1().Nodes().Get(kubeSpanCtx, nodeName, metav1.GetOptions{})
	recordSpanResult(kubeSpan, err)
	kubeSpan.End()
	if err != nil {
		return "", "", fmt.Errorf("get node(%s) failed with %v", nodeName, err)
	}

	if len(node.Labels) == 0 {
		return "", "", fmt.Errorf("node(%s) label is empty", nodeName)
	}

	zone := node.Labels[consts.WellKnownTopologyKey]
	instanceType := node.Labels[consts.InstanceTypeKey]
	klog.V(4).Infof("GetNodeInfoFromLabels: node(%s) from API server: zone=%s, instanceType=%s", nodeName, zone, instanceType)
	return zone, instanceType, nil
}

// getDefaultDiskIOPSReadWrite according to requestGiB
//
//	ref: https://docs.microsoft.com/en-us/azure/virtual-machines/disks-types#ultra-disk-iops
func getDefaultDiskIOPSReadWrite(requestGiB int) int {
	iops := azurecloudconsts.DefaultDiskIOPSReadWrite
	if requestGiB > iops {
		iops = requestGiB
	}
	if iops > 160000 {
		iops = 160000
	}
	return iops
}

// getDefaultDiskMBPSReadWrite according to requestGiB
//
//	ref: https://docs.microsoft.com/en-us/azure/virtual-machines/disks-types#ultra-disk-throughput
func getDefaultDiskMBPSReadWrite(requestGiB int) int {
	bandwidth := azurecloudconsts.DefaultDiskMBpsReadWrite
	iops := getDefaultDiskIOPSReadWrite(requestGiB)
	if iops/256 > bandwidth {
		bandwidth = int(volumehelper.RoundUpSize(int64(iops), 256))
	}
	if bandwidth > iops/4 {
		bandwidth = int(volumehelper.RoundUpSize(int64(iops), 4))
	}
	if bandwidth > 4000 {
		bandwidth = 4000
	}
	return bandwidth
}

// getVMSSInstanceName get instance name from vmss compute name, e.g. "aks-agentpool-20657377-vmss_2" -> "aks-agentpool-20657377-vmss000002"
func getVMSSInstanceName(computeName string) (string, error) {
	names := strings.Split(computeName, "_")
	if len(names) != 2 {
		return "", fmt.Errorf("invalid vmss compute name: %s", computeName)
	}

	instanceID, err := strconv.Atoi(names[1])
	if err != nil {
		return "", fmt.Errorf("parsing vmss compute name(%s) failed with %v", computeName, err)
	}
	return fmt.Sprintf("%s%06s", names[0], strconv.FormatInt(int64(instanceID), 36)), nil
}

// Struct for JSON patch operations
type JSONPatch struct {
	OP    string      `json:"op,omitempty"`
	Path  string      `json:"path,omitempty"`
	Value interface{} `json:"value"`
}

// removeTaintInBackground is a goroutine that retries removeNotReadyTaint with exponential backoff
func removeTaintInBackground(k8sClient clientset.Interface, nodeName, driverName string, backoff wait.Backoff, neverStop bool, removalFunc func(clientset.Interface, string, string) error) {
	backoffErr := wait.ExponentialBackoff(backoff, func() (bool, error) {
		if err := removalFunc(k8sClient, nodeName, driverName); err != nil {
			klog.Errorf("taint removal returned with error: %v", err)
			return false, nil
		}
		return true, nil
	})

	klog.Errorf("taint removal returned with error: %v", backoffErr)
	if neverStop {
		klog.V(2).Infof("Starting taint removal loop, will retry indefinitely")
		for {
			klog.V(6).Infof("Waiting for around 5 minutes before retrying taint removal")
			time.Sleep(4*time.Minute + wait.Jitter(time.Minute, 1.0))
			if err := removalFunc(k8sClient, nodeName, driverName); err != nil {
				klog.Errorf("taint removal returned with error: %v", err)
				return
			}
		}
	}
}

// removeNotReadyTaint removes the taint disk.csi.azure.com/agent-not-ready from the local node
// This taint can be optionally applied by users to prevent startup race conditions such as
// https://github.com/kubernetes/kubernetes/issues/95911
func removeNotReadyTaint(clientset clientset.Interface, nodeName, driverName string) error {
	ctx := context.Background()
	node, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if err := checkAllocatable(ctx, clientset, nodeName, driverName); err != nil {
		return err
	}

	taintKeyToRemove := driverName + consts.AgentNotReadyNodeTaintKeySuffix
	klog.V(6).Infof("removing taint with key %s from local node %s", taintKeyToRemove, nodeName)
	var taintsToKeep []corev1.Taint
	for _, taint := range node.Spec.Taints {
		klog.V(6).Infof("checking taint key %s, value %s, effect %s", taint.Key, taint.Value, taint.Effect)
		if taint.Key != taintKeyToRemove {
			taintsToKeep = append(taintsToKeep, taint)
		} else {
			klog.V(2).Infof("queued taint for removal with key %s, effect %s", taint.Key, taint.Effect)
		}
	}

	if len(taintsToKeep) == len(node.Spec.Taints) {
		klog.V(6).Infof("No taints to remove on node, skipping taint removal")
		return nil
	}

	patchRemoveTaints := []JSONPatch{
		{
			OP:    "test",
			Path:  "/spec/taints",
			Value: node.Spec.Taints,
		},
		{
			OP:    "replace",
			Path:  "/spec/taints",
			Value: taintsToKeep,
		},
	}

	patch, err := json.Marshal(patchRemoveTaints)
	if err != nil {
		return err
	}

	_, err = clientset.CoreV1().Nodes().Patch(ctx, nodeName, k8stypes.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return err
	}
	klog.V(2).Infof("removed taint with key %s from local node %s successfully", taintKeyToRemove, nodeName)
	return nil
}

func checkAllocatable(ctx context.Context, clientset clientset.Interface, nodeName, driverName string) error {
	csiNode, err := clientset.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("isAllocatableSet: failed to get CSINode for %s: %w", nodeName, err)
	}

	for _, driver := range csiNode.Spec.Drivers {
		if driver.Name == driverName {
			if driver.Allocatable != nil && driver.Allocatable.Count != nil {
				klog.V(6).Infof("CSINode Allocatable value is set for driver on node %s, count %d", nodeName, *driver.Allocatable.Count)
				return nil
			}
			return fmt.Errorf("isAllocatableSet: allocatable value not set for driver on node %s", nodeName)
		}
	}

	return fmt.Errorf("isAllocatableSet: driver not found on node %s", nodeName)
}
