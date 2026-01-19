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
	"fmt"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
	volumehelper "sigs.k8s.io/azuredisk-csi-driver/pkg/util"
)

func GenerateCSISnapshot(sourceVolumeID string, snapshot *armcompute.Snapshot) (*csi.Snapshot, error) {
	if snapshot == nil || snapshot.Properties == nil {
		return nil, fmt.Errorf("snapshot property is nil")
	}

	if snapshot.Properties.TimeCreated == nil {
		return nil, fmt.Errorf("TimeCreated of snapshot property is nil")
	}

	if snapshot.Properties.DiskSizeGB == nil {
		return nil, fmt.Errorf("diskSizeGB of snapshot property is nil")
	}

	ready, _ := isCSISnapshotReady(*snapshot.Properties.ProvisioningState)
	if sourceVolumeID == "" {
		sourceVolumeID = GetSourceVolumeID(snapshot)
	}

	// The provisioningState represents the state of resource provisioning which indicates whether the snapshot
	// is created successfully or not. However the snapshot may not be ready to use immediately after creation.
	// We need to check the CompletionPercent if exists to determine if the snapshot is ready to use.
	// This is needed because of Premium V2 disks & Ultra disks, which take some time to be ready to use after creation.
	// In case of Premium V1 disks, the snapshot is ready to use immediately after creation and so we won't have that
	// completionPercent field in the properties. Hence we will treat it 100% if CompletionPercent is nil.
	if ready {
		completionPercent := float32(0.0)
		if snapshot.Properties.CompletionPercent == nil {
			// If CompletionPercent is nil, it means the snapshot is complete
			completionPercent = float32(100.0)
		} else {
			completionPercent = *snapshot.Properties.CompletionPercent
		}

		if completionPercent < float32(100.0) {
			klog.V(2).Infof("snapshot(%s) in progress, completion percent: %f", *snapshot.Name, completionPercent)
			ready = false
		}
	}

	return &csi.Snapshot{
		SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
		SnapshotId:     *snapshot.ID,
		SourceVolumeId: sourceVolumeID,
		CreationTime:   timestamppb.New(*snapshot.Properties.TimeCreated),
		ReadyToUse:     ready,
	}, nil
}

// There are 4 scenarios for listing snapshots.
// 1. StartingToken is null, and MaxEntries is null. Return all snapshots from zero.
// 2. StartingToken is null, and MaxEntries is not null. Return `MaxEntries` snapshots from zero.
// 3. StartingToken is not null, and MaxEntries is null. Return all snapshots from `StartingToken`.
// 4. StartingToken is not null, and MaxEntries is not null. Return `MaxEntries` snapshots from `StartingToken`.
func GetEntriesAndNextToken(req *csi.ListSnapshotsRequest, snapshots []*armcompute.Snapshot) (*csi.ListSnapshotsResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.Aborted, "request is nil")
	}

	var err error
	start := 0
	if req.StartingToken != "" {
		start, err = strconv.Atoi(req.StartingToken)
		if err != nil {
			return nil, status.Errorf(codes.Aborted, "ListSnapshots starting token(%s) parsing with error: %v", req.StartingToken, err)

		}
		if start >= len(snapshots) {
			return nil, status.Errorf(codes.Aborted, "ListSnapshots starting token(%d) is greater than total number of snapshots", start)
		}
		if start < 0 {
			return nil, status.Errorf(codes.Aborted, "ListSnapshots starting token(%d) can not be negative", start)
		}
	}

	maxEntries := len(snapshots) - start
	if req.MaxEntries > 0 && int(req.MaxEntries) < maxEntries {
		maxEntries = int(req.MaxEntries)
	}
	entries := []*csi.ListSnapshotsResponse_Entry{}
	for count := 0; start < len(snapshots) && count < maxEntries; start++ {
		if (req.SourceVolumeId != "" && req.SourceVolumeId == GetSourceVolumeID(snapshots[start])) || req.SourceVolumeId == "" {
			csiSnapshot, err := GenerateCSISnapshot(req.SourceVolumeId, snapshots[start])
			if err != nil {
				return nil, fmt.Errorf("failed to generate snapshot entry: %v", err)
			}
			entries = append(entries, &csi.ListSnapshotsResponse_Entry{Snapshot: csiSnapshot})
			count++
		}
	}

	nextToken := len(snapshots)
	if start < len(snapshots) {
		nextToken = start
	}

	listSnapshotResp := &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: strconv.Itoa(nextToken),
	}

	return listSnapshotResp, nil
}

func GetSnapshotNameFromURI(snapshotURI string) (string, error) {
	matches := diskSnapshotPathRE.FindStringSubmatch(snapshotURI)
	if len(matches) != 2 {
		return "", fmt.Errorf("could not get snapshot name from %s, correct format: %s", snapshotURI, diskSnapshotPathRE)
	}
	return matches[1], nil
}

func GetSourceVolumeID(snapshot *armcompute.Snapshot) string {
	if snapshot != nil &&
		snapshot.Properties != nil &&
		snapshot.Properties.CreationData != nil &&
		snapshot.Properties.CreationData.SourceResourceID != nil {
		return *snapshot.Properties.CreationData.SourceResourceID
	}
	return ""
}

func isCSISnapshotReady(state string) (bool, error) {
	switch strings.ToLower(state) {
	case "succeeded":
		return true, nil
	default:
		return false, nil
	}
}
