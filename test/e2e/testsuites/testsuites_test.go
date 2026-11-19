/*
Copyright 2026 The Kubernetes Authors.

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

package testsuites

import "testing"

func TestGetWinImageTag(t *testing.T) {
	for _, tt := range []struct {
		version string
		want    string
	}{
		{version: "", want: "ltsc2019"},
		{version: "windows-2019", want: "ltsc2019"},
		{version: "windows-2022", want: "ltsc2022"},
		{version: "windows-2025", want: "ltsc2025"},
	} {
		t.Run(tt.version, func(t *testing.T) {
			if got := getWinImageTag(tt.version); got != tt.want {
				t.Errorf("getWinImageTag(%q) = %q, want %q", tt.version, got, tt.want)
			}
		})
	}
}
