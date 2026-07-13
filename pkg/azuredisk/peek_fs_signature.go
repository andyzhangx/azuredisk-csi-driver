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

// Package azuredisk contains the peekFilesystemSignature helper. It is used to
// protect against the "primary superblock corrupted → blkid returns empty →
// FormatAndMount silently reformats" data-loss chain described in
// kubernetes/kubernetes#140376 and the Azure Disk CSI driver PR #3711 thread.
//
// The idea: before we let the caller reformat a disk that blkid reports as
// unformatted, we read a small set of well-known offsets on the raw block
// device. If any of them match a filesystem magic that we recognise, we treat
// the disk as "possibly-formatted, do not reformat". The check is deliberately
// signature-based (not "does it look like there is data"): false positives are
// far cheaper than false negatives here (a false positive fails NodeStageVolume,
// which is retried; a false negative reformats a customer disk).
//
// See peek_fs_signature_test.go for the encoded fixtures we validate against.
package azuredisk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"k8s.io/klog/v2"
)

// FilesystemSignature identifies a filesystem detected on the raw device.
type FilesystemSignature struct {
	// Type is one of "ext2/3/4", "xfs", "btrfs". Empty when no signature matched.
	Type string
	// Location describes where the signature was found, for logging only
	// (e.g. "primary superblock", "backup superblock @ block 32768").
	Location string
}

// String renders a signature for log output.
func (s FilesystemSignature) String() string {
	if s.Type == "" {
		return "<none>"
	}
	return fmt.Sprintf("%s (%s)", s.Type, s.Location)
}

// Filesystem magic constants.
const (
	// ext2/3/4 superblock magic. Little-endian uint16 at offset 0x38 within a
	// 1024-byte superblock.
	extSuperblockMagic uint16 = 0xEF53
	// extSuperblockSize is the size we read from each candidate SB offset.
	extSuperblockSize = 1024
	// extPrimarySuperblockOffset is the byte offset of the primary
	// superblock (block 1 for 1KiB block fs, otherwise the first 1024 bytes
	// of block 0 are the boot sector and the SB starts at 1024).
	extPrimarySuperblockOffset int64 = 1024
	// extSuperblockMagicOffset is the offset of s_magic within a 1024-byte
	// superblock read.
	extSuperblockMagicOffset = 0x38

	// XFS superblock magic ("XFSB") at offset 0.
	xfsSuperblockMagic uint32 = 0x58465342
	// btrfs superblock magic ("_BHRfS_M") at offset 0x10040 within a
	// 4KiB superblock, i.e. bytes 0x40 of the 64KiB region starting at
	// 0x10000.
	btrfsSuperblockOffset int64  = 0x10000
	btrfsMagicOffset      int64  = 0x40
	btrfsMagic            uint64 = 0x4D5F53665248425F // "_BHRfS_M" little-endian
)

// candidateExtBackupSuperblockBlocks lists the block numbers that mke2fs uses
// by default to place backup superblocks (assuming a sparse_super filesystem,
// which has been the mke2fs default for a decade). These cover block sizes
// 1KiB, 2KiB, and 4KiB. Numbers are the standard set that e2fsck -b accepts
// out of the box; we deliberately keep this list short (5 entries) because we
// only want strong evidence, not an exhaustive scan.
var candidateExtBackupSuperblockBlocks = []int64{
	32768, 98304, 163840, 229376, 294912,
}

// candidateBlockSizes are the block sizes we try when computing backup SB byte
// offsets from block numbers. 4KiB is by far the most common on Azure Disk,
// but we also try 1KiB/2KiB to cover manually-created filesystems.
var candidateBlockSizes = []int64{4096, 2048, 1024}

// ErrDeviceUnreadable is returned when we cannot open or read the device at
// all (e.g. permission denied, device does not exist). The caller should
// treat this as "unknown, be conservative" and NOT proceed to reformat.
var ErrDeviceUnreadable = errors.New("device is not readable for signature peek")

// peekFilesystemSignature reads a small, fixed set of offsets on devPath and
// returns a FilesystemSignature if any of them match a known filesystem magic.
// Read errors and unreadable devices return ErrDeviceUnreadable — callers must
// treat that as "abort, be conservative" and NOT proceed to reformat.
//
// The function is idempotent, has no side effects on the device, and reads at
// most 5*1024 + 4096 + 8 bytes (about 9 KiB) even in the worst case.
//
// NOTE: this function does NOT open the device with O_DIRECT. In the failure
// mode we care about (a previous fsck crash corrupted the primary superblock
// only), the kernel's page cache is still empty for this device because the
// only prior read was blkid's, which uses BLKFLSBUF. If a caller needs strict
// direct-IO reads it can wrap this function.
func peekFilesystemSignature(devPath string) (FilesystemSignature, error) {
	f, err := os.OpenFile(devPath, os.O_RDONLY, 0)
	if err != nil {
		return FilesystemSignature{}, fmt.Errorf("%w: %v", ErrDeviceUnreadable, err)
	}
	defer f.Close()

	// 1) primary ext2/3/4 superblock
	if sig, ok := readExtSuperblockAt(f, extPrimarySuperblockOffset, "primary superblock"); ok {
		return sig, nil
	}

	// 2) ext backup superblocks. For each candidate block number, try each
	// candidate block size. Stop at the first hit.
	for _, blk := range candidateExtBackupSuperblockBlocks {
		for _, bs := range candidateBlockSizes {
			off := blk * bs
			label := fmt.Sprintf("backup superblock @ block %d (bs=%d)", blk, bs)
			if sig, ok := readExtSuperblockAt(f, off, label); ok {
				return sig, nil
			}
		}
	}

	// 3) XFS primary superblock
	if sig, ok := readXFSSuperblock(f); ok {
		return sig, nil
	}

	// 4) btrfs primary superblock
	if sig, ok := readBtrfsSuperblock(f); ok {
		return sig, nil
	}

	// No match. This is the "safe to reformat" case.
	return FilesystemSignature{}, nil
}

// readExtSuperblockAt reads 1024 bytes at off and checks for the ext magic at
// the standard offset. Returns (sig, true) on match. Any read error other
// than a short read at EOF is silently treated as "no match" so the scan can
// continue at further offsets.
func readExtSuperblockAt(f *os.File, off int64, location string) (FilesystemSignature, bool) {
	var buf [extSuperblockSize]byte
	n, err := f.ReadAt(buf[:], off)
	if err != nil && err != io.EOF {
		// Read error partway through the scan (e.g. offset past end of
		// device). This is expected for small disks; treat as no match.
		return FilesystemSignature{}, false
	}
	if n < extSuperblockMagicOffset+2 {
		return FilesystemSignature{}, false
	}
	magic := binary.LittleEndian.Uint16(buf[extSuperblockMagicOffset : extSuperblockMagicOffset+2])
	if magic != extSuperblockMagic {
		return FilesystemSignature{}, false
	}
	return FilesystemSignature{Type: "ext2/3/4", Location: location}, true
}

// readXFSSuperblock reads 4 bytes at offset 0 and checks for "XFSB".
func readXFSSuperblock(f *os.File) (FilesystemSignature, bool) {
	var buf [4]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		return FilesystemSignature{}, false
	}
	if binary.BigEndian.Uint32(buf[:]) != xfsSuperblockMagic {
		return FilesystemSignature{}, false
	}
	return FilesystemSignature{Type: "xfs", Location: "primary superblock @ 0"}, true
}

// readBtrfsSuperblock reads 8 bytes at the btrfs primary superblock magic
// offset. btrfs also has backup superblocks at 64MiB, 256GiB, 1PiB but we
// stop at the primary because a broken primary btrfs SB is much rarer than
// broken ext primary SB (btrfs uses COW writes).
func readBtrfsSuperblock(f *os.File) (FilesystemSignature, bool) {
	var buf [8]byte
	if _, err := f.ReadAt(buf[:], btrfsSuperblockOffset+btrfsMagicOffset); err != nil {
		return FilesystemSignature{}, false
	}
	if binary.LittleEndian.Uint64(buf[:]) != btrfsMagic {
		return FilesystemSignature{}, false
	}
	return FilesystemSignature{Type: "btrfs", Location: "primary superblock @ 0x10040"}, true
}

// verifyNoFilesystemSignature is the caller-facing guard used from
// formatAndMount. It returns nil when the device is safe to reformat and a
// non-nil error otherwise. The error message is deliberately explicit so it
// shows up in CSI NodeStageVolume error events and pod describe output — the
// engineer can then decide (via override annotation, forced remediation, or a
// manual mkfs) how to proceed.
func verifyNoFilesystemSignature(devPath string) error {
	sig, err := peekFilesystemSignature(devPath)
	if err != nil {
		// Unreadable device — be conservative. Do NOT reformat.
		klog.Warningf("peekFilesystemSignature(%s): %v — refusing to auto-format", devPath, err)
		return fmt.Errorf("cannot verify filesystem state on %s: %w; refusing to auto-format to avoid data loss", devPath, err)
	}
	if sig.Type == "" {
		klog.V(4).Infof("peekFilesystemSignature(%s): no signature; safe to format", devPath)
		return nil
	}
	klog.Warningf("peekFilesystemSignature(%s): detected %s — refusing to auto-format", devPath, sig)
	return fmt.Errorf("detected existing %s on %s but the primary superblock is not readable by blkid; "+
		"refusing to auto-format to avoid data loss (issue kubernetes/kubernetes#140376). "+
		"If this is intentional, wipe the device manually with `wipefs -a %s` before retrying",
		sig, devPath, devPath)
}
