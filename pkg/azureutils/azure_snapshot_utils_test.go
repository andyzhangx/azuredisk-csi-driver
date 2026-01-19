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
	"reflect"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/utils/ptr"
	volumehelper "sigs.k8s.io/azuredisk-csi-driver/pkg/util"
)

func TestGenerateCSISnapshot(t *testing.T) {
	testCases := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "snap shot property not exist",
			testFunc: func(t *testing.T) {
				snapshot := &armcompute.Snapshot{}
				sourceVolumeID := "unit-test"
				_, err := GenerateCSISnapshot(sourceVolumeID, snapshot)
				expectedErr := fmt.Errorf("snapshot property is nil")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "diskSizeGB of snapshot property is nil",
			testFunc: func(t *testing.T) {
				provisioningState := "true"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
					},
				}
				sourceVolumeID := "unit-test"
				_, err := GenerateCSISnapshot(sourceVolumeID, snapshot)
				expectedErr := fmt.Errorf("diskSizeGB of snapshot property is nil")
				if !reflect.DeepEqual(err, expectedErr) {
					t.Errorf("actualErr: (%v), expectedErr: (%v)", err, expectedErr)
				}
			},
		},
		{
			name: "valid request",
			testFunc: func(t *testing.T) {
				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				snapshot := armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
					},
					ID: &snapshotID,
				}
				sourceVolumeID := "unit-test"
				response, err := GenerateCSISnapshot(sourceVolumeID, &snapshot)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				ready := true
				expectedresponse := &csi.Snapshot{
					SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
					SnapshotId:     *snapshot.ID,
					SourceVolumeId: sourceVolumeID,
					CreationTime:   tp,
					ReadyToUse:     ready,
				}
				if !reflect.DeepEqual(expectedresponse, response) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", response, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
		{
			name: "sourceVolumeID property is missed",
			testFunc: func(t *testing.T) {
				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				sourceResourceID := "unit test"
				snapshot := &armcompute.Snapshot{
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
						CreationData: &armcompute.CreationData{
							SourceResourceID: &sourceResourceID,
						},
					},
					ID: &snapshotID,
				}
				sourceVolumeID := ""
				response, err := GenerateCSISnapshot(sourceVolumeID, snapshot)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				ready := true
				expectedresponse := &csi.Snapshot{
					SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
					SnapshotId:     *snapshot.ID,
					SourceVolumeId: sourceResourceID,
					CreationTime:   tp,
					ReadyToUse:     ready,
				}
				if !reflect.DeepEqual(expectedresponse, response) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", response, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
		{
			name: "snapshot not ready when completion percent less than 100",
			testFunc: func(t *testing.T) {
				provisioningState := "succeeded"
				DiskSize := int32(10)
				snapshotID := "test"
				completionPercent := float32(80.0)
				snapshot := &armcompute.Snapshot{
					Name: ptr.To(fmt.Sprintf("snapshot-%s", snapshotID)),
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &time.Time{},
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
						CompletionPercent: &completionPercent,
					},
					ID: &snapshotID,
				}
				sourceVolumeID := "unit-test"
				response, err := GenerateCSISnapshot(sourceVolumeID, snapshot)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				expectedresponse := &csi.Snapshot{
					SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
					SnapshotId:     *snapshot.ID,
					SourceVolumeId: sourceVolumeID,
					CreationTime:   tp,
					ReadyToUse:     false,
				}
				if !reflect.DeepEqual(expectedresponse, response) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", response, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
		{
			name: "snapshot not ready when provisioning state is updating even if completion percent is 100",
			testFunc: func(t *testing.T) {
				provisioningState := "updating"
				DiskSize := int32(10)
				snapshotID := "test"
				completionPercent := float32(100.0)
				now := time.Now()
				snapshot := &armcompute.Snapshot{
					Name: ptr.To(fmt.Sprintf("snapshot-%s", snapshotID)),
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &now,
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
						CompletionPercent: &completionPercent,
					},
					ID: &snapshotID,
				}
				sourceVolumeID := "unit-test"
				response, err := GenerateCSISnapshot(sourceVolumeID, snapshot)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				expectedresponse := &csi.Snapshot{
					SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
					SnapshotId:     *snapshot.ID,
					SourceVolumeId: sourceVolumeID,
					CreationTime:   tp,
					ReadyToUse:     false,
				}
				if !reflect.DeepEqual(expectedresponse, response) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", response, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
		{
			name: "snapshot ready when provisioning state is succeeded and completion percent is 100",
			testFunc: func(t *testing.T) {
				provisioningState := "succeeded"
				DiskSize := int32(20)
				snapshotID := "snapshot-ready"
				completionPercent := float32(100.0)
				now := time.Now()
				snapshot := &armcompute.Snapshot{
					Name: ptr.To(fmt.Sprintf("snapshot-%s", snapshotID)),
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &now,
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
						CompletionPercent: &completionPercent,
					},
					ID: &snapshotID,
				}
				sourceVolumeID := "vol-123"
				response, err := GenerateCSISnapshot(sourceVolumeID, snapshot)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				expectedresponse := &csi.Snapshot{
					SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
					SnapshotId:     *snapshot.ID,
					SourceVolumeId: sourceVolumeID,
					CreationTime:   tp,
					ReadyToUse:     true,
				}
				if !reflect.DeepEqual(expectedresponse, response) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", response, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
		{
			name: "snapshot ready when provisioning state is succeeded and completion percent is nil explicitly set",
			testFunc: func(t *testing.T) {
				provisioningState := "succeeded"
				DiskSize := int32(15)
				snapshotID := "snapshot-ready-nil-completion"
				now := time.Now()
				var completionPercent *float32
				snapshot := &armcompute.Snapshot{
					Name: ptr.To(fmt.Sprintf("snapshot-%s", snapshotID)),
					Properties: &armcompute.SnapshotProperties{
						TimeCreated:       &now,
						ProvisioningState: &provisioningState,
						DiskSizeGB:        &DiskSize,
						CompletionPercent: completionPercent,
					},
					ID: &snapshotID,
				}
				sourceVolumeID := "vol-456"
				response, err := GenerateCSISnapshot(sourceVolumeID, snapshot)
				tp := timestamppb.New(*snapshot.Properties.TimeCreated)
				expectedresponse := &csi.Snapshot{
					SizeBytes:      volumehelper.GiBToBytes(int64(*snapshot.Properties.DiskSizeGB)),
					SnapshotId:     *snapshot.ID,
					SourceVolumeId: sourceVolumeID,
					CreationTime:   tp,
					ReadyToUse:     true,
				}
				if !reflect.DeepEqual(expectedresponse, response) || err != nil {
					t.Errorf("actualresponse: (%+v), expectedresponse: (%+v)\n", response, expectedresponse)
					t.Errorf("err:%v", err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.testFunc)
	}

}

func TestGetEntriesAndNextToken(t *testing.T) {
	provisioningState := "succeeded"
	DiskSize := int32(10)
	snapshotID := "test"
	sourceVolumeID := "unit-test"
	creationdate := armcompute.CreationData{
		SourceResourceID: &sourceVolumeID,
	}
	snapshot := &armcompute.Snapshot{
		Properties: &armcompute.SnapshotProperties{
			TimeCreated:       &time.Time{},
			ProvisioningState: &provisioningState,
			DiskSizeGB:        &DiskSize,
			CreationData:      &creationdate,
		},
		ID: &snapshotID,
	}
	snapshots := []*armcompute.Snapshot{}
	snapshots = append(snapshots, snapshot)
	entries := []*csi.ListSnapshotsResponse_Entry{}
	csiSnapshot, _ := GenerateCSISnapshot(sourceVolumeID, snapshot)
	entries = append(entries, &csi.ListSnapshotsResponse_Entry{Snapshot: csiSnapshot})
	tests := []struct {
		request          *csi.ListSnapshotsRequest
		snapshots        []*armcompute.Snapshot
		expectedResponse *csi.ListSnapshotsResponse
		expectedError    error
	}{
		{
			nil,
			[]*armcompute.Snapshot{},
			nil,
			status.Errorf(codes.Aborted, "request is nil"),
		},
		{
			&csi.ListSnapshotsRequest{
				MaxEntries:    2,
				StartingToken: "a",
			},
			[]*armcompute.Snapshot{},
			nil,
			status.Errorf(codes.Aborted, "ListSnapshots starting token(a) parsing with error: strconv.Atoi: parsing \"a\": invalid syntax"),
		},
		{
			&csi.ListSnapshotsRequest{
				MaxEntries:    2,
				StartingToken: "01",
			},
			[]*armcompute.Snapshot{},
			nil,
			status.Errorf(codes.Aborted, "ListSnapshots starting token(1) is greater than total number of snapshots"),
		},
		{
			&csi.ListSnapshotsRequest{
				MaxEntries:    2,
				StartingToken: "0",
			},
			[]*armcompute.Snapshot{},
			nil,
			status.Errorf(codes.Aborted, "ListSnapshots starting token(0) is greater than total number of snapshots"),
		},
		{
			&csi.ListSnapshotsRequest{
				MaxEntries:    2,
				StartingToken: "-1",
			},
			[]*armcompute.Snapshot{},
			nil,
			status.Errorf(codes.Aborted, "ListSnapshots starting token(-1) can not be negative"),
		},
		{
			&csi.ListSnapshotsRequest{
				MaxEntries:    2,
				StartingToken: "0",
			},
			append([]*armcompute.Snapshot{}, &armcompute.Snapshot{}),
			nil,
			fmt.Errorf("failed to generate snapshot entry: %v", fmt.Errorf("snapshot property is nil")),
		},
		{
			&csi.ListSnapshotsRequest{
				MaxEntries:     2,
				SourceVolumeId: sourceVolumeID,
			},
			snapshots,
			&csi.ListSnapshotsResponse{
				Entries:   entries,
				NextToken: "1",
			},
			error(nil),
		},
		{
			&csi.ListSnapshotsRequest{
				MaxEntries:     1,
				SourceVolumeId: sourceVolumeID,
			},
			append(snapshots, snapshot),
			&csi.ListSnapshotsResponse{
				Entries:   entries,
				NextToken: "1",
			},
			error(nil),
		},
	}

	for _, test := range tests {
		resultResponse, resultError := GetEntriesAndNextToken(test.request, test.snapshots)
		if !reflect.DeepEqual(resultResponse, test.expectedResponse) || (!reflect.DeepEqual(resultError, test.expectedError)) {
			t.Errorf("request: %v, snapshotListPage: %v, resultResponse: %v, expectedResponse: %v, resultError: %v, expectedError: %v", test.request, test.snapshots, resultResponse, test.expectedResponse, resultError, test.expectedError)
		}
	}
}

func TestGetSnapshotName(t *testing.T) {
	tests := []struct {
		options   string
		expected1 string
		expected2 error
	}{
		{
			options:   "testurl/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/snapshots/snapshot-name",
			expected1: "snapshot-name",
			expected2: nil,
		},
		{
			options:   "testurl/subscriptions/12/resourcegroups/23/providers/microsoft.compute/SNAPSHOTS/snapshot-name",
			expected1: "snapshot-name",
			expected2: nil,
		},
		{
			options:   "testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name",
			expected1: "",
			expected2: fmt.Errorf("could not get snapshot name from testurl/subscriptions/23/providers/Microsoft.Compute/snapshots/snapshot-name, correct format: %s", diskSnapshotPathRE),
		},
	}

	for _, test := range tests {
		result1, result2 := GetSnapshotNameFromURI(test.options)
		if !reflect.DeepEqual(result1, test.expected1) || !reflect.DeepEqual(result2, test.expected2) {
			t.Errorf("input: %q, getSnapshotName result1: %q, expected1: %q, result2: %q, expected2: %q", test.options, result1, test.expected1,
				result2, test.expected2)
		}
	}
}

func TestIsCSISnapshotReady(t *testing.T) {
	tests := []struct {
		state        string
		expectedResp bool
	}{
		{
			state:        "Succeeded",
			expectedResp: true,
		},
		{
			state:        "succeeded",
			expectedResp: true,
		},
		{
			state:        "fail",
			expectedResp: false,
		},
	}
	for _, test := range tests {
		flag, err := isCSISnapshotReady(test.state)

		if flag != test.expectedResp {
			t.Errorf("testdesc: %v \n expected result:%t \n actual result:%t", test.state, test.expectedResp, flag)
		}
		assert.Nil(t, err)
	}
}
