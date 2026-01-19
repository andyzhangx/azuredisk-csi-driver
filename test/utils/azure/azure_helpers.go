/*
Copyright 2020 The Kubernetes Authors.

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

package azure

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	compute "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	network "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v6"
	resources "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/diskclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/interfaceclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/resourcegroupclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/sshpublickeyresourceclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/subnetclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachineclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualnetworkclient"
)

type Client struct {
	groupsClient        resourcegroupclient.Interface
	vmClient            virtualmachineclient.Interface
	nicClient           interfaceclient.Interface
	subnetsClient       subnetclient.Interface
	vnetClient          virtualnetworkclient.Interface
	disksClient         diskclient.Interface
	sshPublicKeysClient sshpublickeyresourceclient.Interface
}

func GetAzureClient(cloud, subscriptionID, clientID, tenantID, clientSecret, aadFederatedTokenFile string) (*Client, error) {
	armConfig := &azclient.ARMClientConfig{
		Cloud:    cloud,
		TenantID: tenantID,
	}
	clientOps, _, err := azclient.GetAzureCloudConfigAndEnvConfig(armConfig)
	if err != nil {
		return nil, err
	}
	useFederatedWorkloadIdentityExtension := false
	if aadFederatedTokenFile != "" {
		useFederatedWorkloadIdentityExtension = true
	}
	credProvider, err := azclient.NewAuthProvider(armConfig, &azclient.AzureAuthConfig{
		AADClientID:                           clientID,
		AADClientSecret:                       clientSecret,
		AADFederatedTokenFile:                 aadFederatedTokenFile,
		UseFederatedWorkloadIdentityExtension: useFederatedWorkloadIdentityExtension,
	})
	if err != nil {
		return nil, err
	}
	cred := credProvider.GetAzIdentity()
	factory, err := azclient.NewClientFactory(&azclient.ClientFactoryConfig{
		SubscriptionID: subscriptionID,
	}, armConfig, clientOps, cred)
	if err != nil {
		return nil, err
	}
	return &Client{
		groupsClient:        factory.GetResourceGroupClient(),
		vmClient:            factory.GetVirtualMachineClient(),
		nicClient:           factory.GetInterfaceClient(),
		subnetsClient:       factory.GetSubnetClient(),
		vnetClient:          factory.GetVirtualNetworkClient(),
		disksClient:         factory.GetDiskClient(),
		sshPublicKeysClient: factory.GetSSHPublicKeyResourceClient(),
	}, nil
}
func (az *Client) GetAzureDisksClient() (diskclient.Interface, error) {
	return az.disksClient, nil
}

func (az *Client) EnsureSSHPublicKey(ctx context.Context, resourceGroupName, location, keyName string) (publicKey string, err error) {
	_, err = az.sshPublicKeysClient.Create(ctx, resourceGroupName, keyName, compute.SSHPublicKeyResource{Location: &location})
	if err != nil {
		return "", err
	}
	result, err := az.sshPublicKeysClient.GenerateKeyPair(ctx, resourceGroupName, keyName)
	if err != nil {
		return "", err
	}
	return *result.PublicKey, nil
}

func (az *Client) EnsureResourceGroup(ctx context.Context, name, location string, managedBy *string) (resourceGroup *resources.ResourceGroup, err error) {
	var tags map[string]*string
	group, err := az.groupsClient.Get(ctx, name)
	if err == nil && group != nil && group.Tags != nil {
		tags = group.Tags
	} else {
		tags = make(map[string]*string)
	}
	if managedBy == nil && group != nil {
		managedBy = group.ManagedBy
	}
	// Tags for correlating resource groups with prow jobs on testgrid
	tags["buildID"] = to.Ptr(os.Getenv("BUILD_ID"))
	tags["jobName"] = to.Ptr(os.Getenv("JOB_NAME"))
	tags["creationTimestamp"] = to.Ptr(time.Now().UTC().Format(time.RFC3339))

	response, err := az.groupsClient.CreateOrUpdate(ctx, name, resources.ResourceGroup{
		Name:      &name,
		Location:  &location,
		ManagedBy: managedBy,
		Tags:      tags,
	})
	if err != nil {
		return response, err
	}

	return response, nil
}

func (az *Client) DeleteResourceGroup(ctx context.Context, groupName string) error {
	_, err := az.groupsClient.Get(ctx, groupName)
	if err == nil {
		err = az.groupsClient.Delete(ctx, groupName)
		if err != nil {
			return fmt.Errorf("cannot delete resource group %v: %v", groupName, err)
		}
	}
	return nil
}

func (az *Client) EnsureVirtualMachine(ctx context.Context, groupName, location, vmName string) (compute.VirtualMachine, error) {
	nic, err := az.EnsureNIC(ctx, groupName, location, vmName+"-nic", vmName+"-vnet", vmName+"-subnet")
	if err != nil {
		return compute.VirtualMachine{}, err
	}

	publicKey, err := az.EnsureSSHPublicKey(ctx, groupName, location, "test-key")
	if err != nil {
		return compute.VirtualMachine{}, err
	}

	resp, err := az.vmClient.CreateOrUpdate(
		ctx,
		groupName,
		vmName,
		compute.VirtualMachine{
			Location: to.Ptr(location),
			Properties: &compute.VirtualMachineProperties{
				HardwareProfile: &compute.HardwareProfile{
					VMSize: to.Ptr(compute.VirtualMachineSizeTypesStandardDS2V2),
				},
				StorageProfile: &compute.StorageProfile{
					ImageReference: &compute.ImageReference{
						Publisher: to.Ptr("Canonical"),
						Offer:     to.Ptr("0001-com-ubuntu-server-jammy"),
						SKU:       to.Ptr("22_04-lts-gen2"),
						Version:   to.Ptr("latest"),
					},
				},
				OSProfile: &compute.OSProfile{
					ComputerName:  to.Ptr(vmName),
					AdminUsername: to.Ptr("azureuser"),
					AdminPassword: to.Ptr("Azureuser1234"),
					LinuxConfiguration: &compute.LinuxConfiguration{
						DisablePasswordAuthentication: to.Ptr(true),
						SSH: &compute.SSHConfiguration{
							PublicKeys: []*compute.SSHPublicKey{
								{
									Path:    to.Ptr("/home/azureuser/.ssh/authorized_keys"),
									KeyData: &publicKey,
								},
							},
						},
					},
				},
				NetworkProfile: &compute.NetworkProfile{
					NetworkInterfaces: []*compute.NetworkInterfaceReference{
						{
							ID: nic.ID,
							Properties: &compute.NetworkInterfaceReferenceProperties{
								Primary: to.Ptr(true),
							},
						},
					},
				},
			},
		},
	)
	if err != nil {
		return compute.VirtualMachine{}, fmt.Errorf("cannot create vm: %v", err)
	}

	return *resp, nil
}

func (az *Client) EnsureNIC(ctx context.Context, groupName, location, nicName, vnetName, subnetName string) (network.Interface, error) {
	_, err := az.EnsureVirtualNetworkAndSubnet(ctx, groupName, location, vnetName, subnetName)
	if err != nil {
		return network.Interface{}, err
	}

	subnet, err := az.GetVirtualNetworkSubnet(ctx, groupName, vnetName, subnetName)
	if err != nil {
		return network.Interface{}, fmt.Errorf("cannot get subnet %s of virtual network %s in %s: %v", subnetName, vnetName, groupName, err)
	}

	nic, err := az.nicClient.CreateOrUpdate(
		ctx,
		groupName,
		nicName,
		network.Interface{
			Name:     to.Ptr(nicName),
			Location: to.Ptr(location),
			Properties: &network.InterfacePropertiesFormat{
				IPConfigurations: []*network.InterfaceIPConfiguration{
					{
						Name: to.Ptr("ipConfig1"),
						Properties: &network.InterfaceIPConfigurationPropertiesFormat{
							Subnet:                    &subnet,
							PrivateIPAllocationMethod: to.Ptr(network.IPAllocationMethodDynamic),
						},
					},
				},
			},
		},
	)
	if err != nil {
		return network.Interface{}, fmt.Errorf("cannot create nic: %v", err)
	}

	return *nic, nil
}

func (az *Client) EnsureVirtualNetworkAndSubnet(ctx context.Context, groupName, location, vnetName, subnetName string) (network.VirtualNetwork, error) {
	vnet, err := az.vnetClient.CreateOrUpdate(
		ctx,
		groupName,
		vnetName,
		network.VirtualNetwork{
			Location: to.Ptr(location),
			Properties: &network.VirtualNetworkPropertiesFormat{
				AddressSpace: &network.AddressSpace{
					AddressPrefixes: []*string{to.Ptr("10.0.0.0/8")},
				},
				Subnets: []*network.Subnet{
					{
						Name: to.Ptr(subnetName),
						Properties: &network.SubnetPropertiesFormat{
							AddressPrefix: to.Ptr("10.0.0.0/16"),
						},
					},
				},
			},
		})

	if err != nil {
		return network.VirtualNetwork{}, fmt.Errorf("cannot create virtual network: %v", err)
	}

	return *vnet, nil
}

func (az *Client) GetVirtualNetworkSubnet(ctx context.Context, groupName, vnetName, subnetName string) (network.Subnet, error) {
	subnet, err := az.subnetsClient.Get(ctx, groupName, vnetName, subnetName, nil)
	return *subnet, err
}
