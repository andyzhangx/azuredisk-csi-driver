//go:build linux
// +build linux

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

package azuredisk

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// newBlankImage creates a sparse file of the given size we can use as a fake
// block device for signature tests. Size defaults to 256 MiB, large enough to
// cover the highest backup-SB offset we probe (294912 * 4096 ≈ 1.2 GiB is the
// max, so for backup-SB tests we bump this).
func newBlankImage(t *testing.T, sizeBytes int64) string {
	t.Helper()
	f, err := os.CreateTemp("", "peekfs-*.img")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	defer f.Close()
	// Sparse: just Truncate to the target size.
	if err := f.Truncate(sizeBytes); err != nil {
		os.Remove(f.Name())
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	return f.Name()
}

func writeAt(t *testing.T, path string, off int64, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	if _, err := f.WriteAt(data, off); err != nil {
		t.Fatalf("write %d bytes at %d: %v", len(data), off, err)
	}
}

func TestPeekFilesystemSignature_Blank(t *testing.T) {
	img := newBlankImage(t, 8<<20) // 8 MiB, no signatures anywhere
	sig, err := peekFilesystemSignature(img)
	if err != nil {
		t.Fatalf("peekFilesystemSignature returned unexpected err on blank image: %v", err)
	}
	if sig.Type != "" {
		t.Fatalf("expected no signature on blank image; got %+v", sig)
	}
}

func TestPeekFilesystemSignature_UnreadableDevice(t *testing.T) {
	sig, err := peekFilesystemSignature(filepath.Join(t.TempDir(), "does-not-exist"))
	if !errors.Is(err, ErrDeviceUnreadable) {
		t.Fatalf("expected ErrDeviceUnreadable on missing device; got err=%v sig=%+v", err, sig)
	}
}

func TestPeekFilesystemSignature_PrimaryExt(t *testing.T) {
	img := newBlankImage(t, 8<<20)
	// Write ext magic 0xEF53 at primary SB offset 1024 + 0x38 = 1080.
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], extSuperblockMagic)
	writeAt(t, img, extPrimarySuperblockOffset+int64(extSuperblockMagicOffset), buf[:])

	sig, err := peekFilesystemSignature(img)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sig.Type != "ext2/3/4" {
		t.Fatalf("expected ext2/3/4; got %+v", sig)
	}
	if sig.Location != "primary superblock" {
		t.Fatalf("expected primary superblock location; got %q", sig.Location)
	}
}

func TestPeekFilesystemSignature_BackupExtOnly(t *testing.T) {
	// This is the T4 scenario from kubernetes/kubernetes#140376:
	// primary SB corrupted (blkid returns empty), backup SB still valid.
	// Use 256 MiB image so we cover backup at block 32768 * 4096 = 128 MiB.
	img := newBlankImage(t, 256<<20)

	// Ensure primary SB has no magic (image is sparse-zero, already the case).
	// Write ext magic at backup SB offset for 4KiB block size, block 32768:
	// off = 32768 * 4096 + 0x38 = 134217784.
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], extSuperblockMagic)
	off := int64(32768)*4096 + int64(extSuperblockMagicOffset)
	writeAt(t, img, off, buf[:])

	sig, err := peekFilesystemSignature(img)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sig.Type != "ext2/3/4" {
		t.Fatalf("expected ext2/3/4 from backup SB; got %+v", sig)
	}
	if sig.Location == "primary superblock" {
		t.Fatalf("expected backup superblock location; got %q", sig.Location)
	}
}

func TestPeekFilesystemSignature_XFS(t *testing.T) {
	img := newBlankImage(t, 8<<20)
	// XFS magic "XFSB" (0x58465342) at offset 0, big-endian.
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], xfsSuperblockMagic)
	writeAt(t, img, 0, buf[:])

	sig, err := peekFilesystemSignature(img)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sig.Type != "xfs" {
		t.Fatalf("expected xfs; got %+v", sig)
	}
}

func TestPeekFilesystemSignature_Btrfs(t *testing.T) {
	// btrfs primary SB starts at 64 KiB and needs at least ~68 KiB image.
	img := newBlankImage(t, 8<<20)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], btrfsMagic)
	writeAt(t, img, btrfsSuperblockOffset+btrfsMagicOffset, buf[:])

	sig, err := peekFilesystemSignature(img)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sig.Type != "btrfs" {
		t.Fatalf("expected btrfs; got %+v", sig)
	}
}

func TestPeekFilesystemSignature_TinyDevice(t *testing.T) {
	// 4 KiB image: too small even for the primary SB scan. Should still
	// succeed with no signature (no read error propagates out).
	img := newBlankImage(t, 4096)
	sig, err := peekFilesystemSignature(img)
	if err != nil {
		t.Fatalf("unexpected err on 4KiB image: %v", err)
	}
	if sig.Type != "" {
		t.Fatalf("expected no signature on tiny image; got %+v", sig)
	}
}

func TestVerifyNoFilesystemSignature(t *testing.T) {
	// Blank: should return nil.
	blank := newBlankImage(t, 8<<20)
	if err := verifyNoFilesystemSignature(blank); err != nil {
		t.Fatalf("expected nil on blank; got %v", err)
	}

	// Ext primary SB: should return an error containing devPath and "ext".
	withSig := newBlankImage(t, 8<<20)
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], extSuperblockMagic)
	writeAt(t, withSig, extPrimarySuperblockOffset+int64(extSuperblockMagicOffset), buf[:])

	err := verifyNoFilesystemSignature(withSig)
	if err == nil {
		t.Fatal("expected error when signature is present")
	}
	// Sanity-check that the error message mentions the device path and wipefs
	// hint. These are the two facts we want the operator to see in `kubectl
	// describe pod`.
	msg := err.Error()
	for _, want := range []string{withSig, "wipefs", "ext"} {
		if !contains(msg, want) {
			t.Errorf("expected error message %q to contain %q", msg, want)
		}
	}

	// Unreadable device.
	err = verifyNoFilesystemSignature(filepath.Join(t.TempDir(), "nope"))
	if err == nil {
		t.Fatal("expected error on unreadable device")
	}
}

// contains is a tiny helper to avoid importing strings in the test file.
func contains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
