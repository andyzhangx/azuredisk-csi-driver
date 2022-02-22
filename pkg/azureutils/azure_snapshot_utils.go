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

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2021-07-01/compute"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	volumehelper "sigs.k8s.io/azuredisk-csi-driver/pkg/util"
)

func GenerateCSISnapshot(sourceVolumeID string, snapshot *compute.Snapshot) (*csi.Snapshot, error) {
	if snapshot == nil || snapshot.SnapshotProperties == nil {
		return nil, fmt.Errorf("snapshot property is nil")
	}

	tp := timestamppb.New(snapshot.SnapshotProperties.TimeCreated.ToTime())
	if tp == nil {
		return nil, fmt.Errorf("failed to convert timestamp(%v)", snapshot.SnapshotProperties.TimeCreated.ToTime())
	}
	ready, _ := isCSISnapshotReady(*snapshot.SnapshotProperties.ProvisioningState)

	if snapshot.SnapshotProperties.DiskSizeGB == nil {
		return nil, fmt.Errorf("diskSizeGB of snapshot property is nil")
	}

	if sourceVolumeID == "" {
		sourceVolumeID = GetSourceVolumeID(snapshot)
	}

	return &csi.Snapshot{
		SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.SnapshotProperties.DiskSizeGB)),
		SnapshotId:     *snapshot.ID,
		SourceVolumeId: sourceVolumeID,
		CreationTime:   tp,
		ReadyToUse:     ready,
	}, nil
}

// There are 4 scenarios for listing snapshots.
// 1. StartingToken is null, and MaxEntries is null. Return all snapshots from zero.
// 2. StartingToken is null, and MaxEntries is not null. Return `MaxEntries` snapshots from zero.
// 3. StartingToken is not null, and MaxEntries is null. Return all snapshots from `StartingToken`.
// 4. StartingToken is not null, and MaxEntries is not null. Return `MaxEntries` snapshots from `StartingToken`.
func GetEntriesAndNextToken(req *csi.ListSnapshotsRequest, snapshots []compute.Snapshot) (*csi.ListSnapshotsResponse, error) {
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
		if (req.SourceVolumeId != "" && req.SourceVolumeId == GetSourceVolumeID(&snapshots[start])) || req.SourceVolumeId == "" {
			csiSnapshot, err := GenerateCSISnapshot(req.SourceVolumeId, &snapshots[start])
			if err != nil {
				return nil, fmt.Errorf("failed to generate snapshot entry: %w", err)
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

func GetSourceVolumeID(snapshot *compute.Snapshot) string {
	if snapshot != nil &&
		snapshot.SnapshotProperties != nil &&
		snapshot.SnapshotProperties.CreationData != nil &&
		snapshot.SnapshotProperties.CreationData.SourceResourceID != nil {
		return *snapshot.SnapshotProperties.CreationData.SourceResourceID
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
