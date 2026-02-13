/*
Copyright 2026 The Butler Authors.

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

package ipam

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"net"
	"sort"
	"strings"
)

var (
	// ErrPoolExhausted indicates no contiguous block of the requested size is available.
	ErrPoolExhausted = errors.New("pool exhausted: no contiguous block available")

	// ErrInvalidCIDR indicates an unparseable CIDR string.
	ErrInvalidCIDR = errors.New("invalid CIDR notation")

	// ErrInvalidRange indicates start IP is after end IP.
	ErrInvalidRange = errors.New("invalid IP range: start is after end")

	// ErrOutOfRange indicates the requested range falls outside the pool.
	ErrOutOfRange = errors.New("range falls outside pool boundaries")
)

// PoolState represents the current state of a network pool for allocation purposes.
// It decouples the allocator from Kubernetes types.
type PoolState struct {
	// AllocatableStart is the first IP available for tenant allocation.
	AllocatableStart string
	// AllocatableEnd is the last IP available for tenant allocation.
	AllocatableEnd string
	// ReservedCIDRs are CIDR ranges excluded from allocation (e.g., management cluster IPs).
	ReservedCIDRs []string
	// ExistingAllocs are currently allocated ranges.
	ExistingAllocs []AllocatedRange
}

// AllocatedRange represents a contiguous block of allocated IPs.
type AllocatedRange struct {
	Start string
	End   string
}

// AllocationResult contains the result of a successful allocation.
type AllocationResult struct {
	Start     string
	End       string
	CIDR      string // CIDR notation if power-of-2 aligned, otherwise "start-end"
	Addresses []string
	Count     int32
}

// FreeBlock represents a contiguous block of free IPs in the pool.
type FreeBlock struct {
	StartOffset uint32
	EndOffset   uint32
	Size        uint32
}

// FragmentationInfo contains pool health metrics.
type FragmentationInfo struct {
	FragmentationPercent int32
	LargestFreeBlock    int32
	FreeBlockCount      int32
	TotalFreeIPs        int32
}

// IPToUint32 converts a 4-byte IPv4 address to a uint32.
func IPToUint32(ip net.IP) uint32 {
	ip = ip.To4()
	if ip == nil {
		return 0
	}
	return binary.BigEndian.Uint32(ip)
}

// Uint32ToIP converts a uint32 to a 4-byte IPv4 address.
func Uint32ToIP(n uint32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, n)
	return ip
}

// ParseCIDR parses a CIDR string and returns the first IP, last IP, and total host count.
func ParseCIDR(cidr string) (start net.IP, end net.IP, count int32, err error) {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("%w: %s", ErrInvalidCIDR, err)
	}

	ones, totalBits := network.Mask.Size()
	if totalBits != 32 {
		return nil, nil, 0, fmt.Errorf("%w: only IPv4 supported", ErrInvalidCIDR)
	}

	startUint := IPToUint32(network.IP.To4())
	hostBits := uint(totalBits - ones)
	total := uint32(1) << hostBits
	endUint := startUint + total - 1

	if total > math.MaxInt32 {
		count = math.MaxInt32
	} else {
		count = int32(total)
	}

	return Uint32ToIP(startUint), Uint32ToIP(endUint), count, nil
}

// CountIPsInRange returns the number of IPs between start and end (inclusive).
func CountIPsInRange(start, end string) (int32, error) {
	startIP := net.ParseIP(start).To4()
	endIP := net.ParseIP(end).To4()
	if startIP == nil || endIP == nil {
		return 0, fmt.Errorf("invalid IP address")
	}

	s := IPToUint32(startIP)
	e := IPToUint32(endIP)
	if s > e {
		return 0, ErrInvalidRange
	}

	diff := e - s + 1
	if diff > math.MaxInt32 {
		return math.MaxInt32, nil
	}
	return int32(diff), nil
}

// EnumerateIPs returns all IPs between start and end (inclusive).
func EnumerateIPs(start, end string) ([]string, error) {
	startIP := net.ParseIP(start).To4()
	endIP := net.ParseIP(end).To4()
	if startIP == nil || endIP == nil {
		return nil, fmt.Errorf("invalid IP address")
	}

	s := IPToUint32(startIP)
	e := IPToUint32(endIP)
	if s > e {
		return nil, ErrInvalidRange
	}

	count := e - s + 1
	if count > 65536 {
		return nil, fmt.Errorf("range too large to enumerate: %d IPs", count)
	}

	ips := make([]string, 0, count)
	for i := s; i <= e; i++ {
		ips = append(ips, Uint32ToIP(i).String())
	}
	return ips, nil
}

// FormatCIDR returns CIDR notation if the range is power-of-2 aligned,
// otherwise returns "start-end" format.
func FormatCIDR(start, end string) string {
	startIP := net.ParseIP(start).To4()
	endIP := net.ParseIP(end).To4()
	if startIP == nil || endIP == nil {
		return fmt.Sprintf("%s-%s", start, end)
	}

	s := IPToUint32(startIP)
	e := IPToUint32(endIP)
	if s > e {
		return fmt.Sprintf("%s-%s", start, end)
	}

	count := e - s + 1

	// Check if count is a power of 2
	if count == 0 || (count&(count-1)) != 0 {
		return fmt.Sprintf("%s-%s", start, end)
	}

	// Check if start is aligned to the block size
	if s%count != 0 {
		return fmt.Sprintf("%s-%s", start, end)
	}

	// Calculate prefix length
	prefixLen := 32 - bits.TrailingZeros32(count)
	return fmt.Sprintf("%s/%d", start, prefixLen)
}

// ValidateCIDRContains checks if the inner CIDR is completely contained within the outer CIDR.
func ValidateCIDRContains(outer, inner string) (bool, error) {
	_, outerNet, err := net.ParseCIDR(outer)
	if err != nil {
		return false, fmt.Errorf("invalid outer CIDR %q: %w", outer, err)
	}

	innerStart, innerEnd, _, err := ParseCIDR(inner)
	if err != nil {
		return false, fmt.Errorf("invalid inner CIDR %q: %w", inner, err)
	}

	return outerNet.Contains(innerStart) && outerNet.Contains(innerEnd), nil
}

// ValidateRangeWithinCIDR checks if a start-end range is within a CIDR.
func ValidateRangeWithinCIDR(cidr, start, end string) (bool, error) {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return false, fmt.Errorf("invalid CIDR %q: %w", cidr, err)
	}

	startIP := net.ParseIP(start).To4()
	endIP := net.ParseIP(end).To4()
	if startIP == nil || endIP == nil {
		return false, fmt.Errorf("invalid IP address")
	}

	return network.Contains(startIP) && network.Contains(endIP), nil
}

// BuildBitmap creates a bitmap representing the allocatable range.
// true = used/reserved, false = free.
func BuildBitmap(pool PoolState) ([]bool, uint32, error) {
	startIP := net.ParseIP(pool.AllocatableStart).To4()
	endIP := net.ParseIP(pool.AllocatableEnd).To4()
	if startIP == nil || endIP == nil {
		return nil, 0, fmt.Errorf("invalid pool range")
	}

	poolStart := IPToUint32(startIP)
	poolEnd := IPToUint32(endIP)
	if poolStart > poolEnd {
		return nil, 0, ErrInvalidRange
	}

	size := poolEnd - poolStart + 1
	if size > 1<<20 { // Cap at ~1M IPs to prevent OOM
		return nil, 0, fmt.Errorf("pool too large: %d IPs (max 1048576)", size)
	}

	bitmap := make([]bool, size)

	// Mark reserved CIDRs
	for _, reserved := range pool.ReservedCIDRs {
		rStart, rEnd, _, err := ParseCIDR(reserved)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid reserved CIDR %q: %w", reserved, err)
		}
		rs := IPToUint32(rStart.To4())
		re := IPToUint32(rEnd.To4())

		for i := rs; i <= re; i++ {
			if i >= poolStart && i <= poolEnd {
				bitmap[i-poolStart] = true
			}
		}
	}

	// Mark existing allocations
	for _, alloc := range pool.ExistingAllocs {
		aStartIP := net.ParseIP(alloc.Start).To4()
		aEndIP := net.ParseIP(alloc.End).To4()
		if aStartIP == nil || aEndIP == nil {
			continue
		}
		as := IPToUint32(aStartIP)
		ae := IPToUint32(aEndIP)

		for i := as; i <= ae; i++ {
			if i >= poolStart && i <= poolEnd {
				bitmap[i-poolStart] = true
			}
		}
	}

	return bitmap, poolStart, nil
}

// findFreeBlocks scans the bitmap and returns all contiguous free blocks.
func findFreeBlocks(bitmap []bool) []FreeBlock {
	var blocks []FreeBlock
	inFree := false
	var blockStart uint32

	for i, used := range bitmap {
		if !used && !inFree {
			// Start of a free block
			inFree = true
			blockStart = uint32(i)
		} else if used && inFree {
			// End of a free block
			blocks = append(blocks, FreeBlock{
				StartOffset: blockStart,
				EndOffset:   uint32(i) - 1,
				Size:        uint32(i) - blockStart,
			})
			inFree = false
		}
	}

	// Handle trailing free block
	if inFree {
		blocks = append(blocks, FreeBlock{
			StartOffset: blockStart,
			EndOffset:   uint32(len(bitmap)) - 1,
			Size:        uint32(len(bitmap)) - blockStart,
		})
	}

	return blocks
}

// AllocateRange finds the best-fit contiguous block of the requested size.
// Best-fit selects the smallest free block that satisfies the request,
// minimizing fragmentation.
func AllocateRange(pool PoolState, requestedCount int32) (*AllocationResult, error) {
	if requestedCount <= 0 {
		return nil, fmt.Errorf("requested count must be positive")
	}

	bitmap, poolStartUint, err := BuildBitmap(pool)
	if err != nil {
		return nil, err
	}

	freeBlocks := findFreeBlocks(bitmap)
	if len(freeBlocks) == 0 {
		return nil, ErrPoolExhausted
	}

	// Best-fit: find the smallest block that fits
	var bestBlock *FreeBlock
	for i := range freeBlocks {
		block := &freeBlocks[i]
		if block.Size >= uint32(requestedCount) {
			if bestBlock == nil || block.Size < bestBlock.Size {
				bestBlock = block
			}
		}
	}

	if bestBlock == nil {
		return nil, ErrPoolExhausted
	}

	// Allocate from the start of the best-fit block
	allocStart := poolStartUint + bestBlock.StartOffset
	allocEnd := allocStart + uint32(requestedCount) - 1

	startStr := Uint32ToIP(allocStart).String()
	endStr := Uint32ToIP(allocEnd).String()

	addresses, err := EnumerateIPs(startStr, endStr)
	if err != nil {
		return nil, fmt.Errorf("failed to enumerate allocated IPs: %w", err)
	}

	return &AllocationResult{
		Start:     startStr,
		End:       endStr,
		CIDR:      FormatCIDR(startStr, endStr),
		Addresses: addresses,
		Count:     requestedCount,
	}, nil
}

// AllocatePinnedRange validates and allocates a specific IP range.
// Returns an error if the range is outside the pool, overlaps reserved ranges,
// or conflicts with existing allocations.
func AllocatePinnedRange(pool PoolState, start, end string) (*AllocationResult, error) {
	startIP := net.ParseIP(start).To4()
	endIP := net.ParseIP(end).To4()
	if startIP == nil || endIP == nil {
		return nil, fmt.Errorf("invalid pinned range IPs: %s-%s", start, end)
	}

	pinnedStart := IPToUint32(startIP)
	pinnedEnd := IPToUint32(endIP)
	if pinnedStart > pinnedEnd {
		return nil, fmt.Errorf("%w: pinned start %s is after end %s", ErrInvalidRange, start, end)
	}

	// Verify the range is within the allocatable pool
	poolStartIP := net.ParseIP(pool.AllocatableStart).To4()
	poolEndIP := net.ParseIP(pool.AllocatableEnd).To4()
	if poolStartIP == nil || poolEndIP == nil {
		return nil, fmt.Errorf("invalid pool range")
	}

	poolStart := IPToUint32(poolStartIP)
	poolEnd := IPToUint32(poolEndIP)
	if pinnedStart < poolStart || pinnedEnd > poolEnd {
		return nil, fmt.Errorf("%w: pinned range %s-%s is outside pool %s-%s",
			ErrOutOfRange, start, end, pool.AllocatableStart, pool.AllocatableEnd)
	}

	// Build bitmap and check for conflicts
	bitmap, bitmapStart, err := BuildBitmap(pool)
	if err != nil {
		return nil, err
	}

	for i := pinnedStart; i <= pinnedEnd; i++ {
		offset := i - bitmapStart
		if int(offset) >= len(bitmap) {
			return nil, fmt.Errorf("%w: pinned IP %s outside bitmap", ErrOutOfRange, Uint32ToIP(i))
		}
		if bitmap[offset] {
			return nil, fmt.Errorf("pinned range conflicts at IP %s (reserved or already allocated)", Uint32ToIP(i))
		}
	}

	addresses, err := EnumerateIPs(start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to enumerate pinned IPs: %w", err)
	}

	count := pinnedEnd - pinnedStart + 1
	return &AllocationResult{
		Start:     start,
		End:       end,
		CIDR:      FormatCIDR(start, end),
		Addresses: addresses,
		Count:     int32(count),
	}, nil
}

// ComputeFragmentation calculates pool health metrics from a bitmap.
func ComputeFragmentation(pool PoolState) (*FragmentationInfo, error) {
	bitmap, _, err := BuildBitmap(pool)
	if err != nil {
		return nil, err
	}

	freeBlocks := findFreeBlocks(bitmap)

	totalFree := uint32(0)
	largestFree := uint32(0)
	for _, block := range freeBlocks {
		totalFree += block.Size
		if block.Size > largestFree {
			largestFree = block.Size
		}
	}

	var fragmentPercent int32
	if totalFree == 0 {
		fragmentPercent = 0
	} else if len(freeBlocks) <= 1 {
		fragmentPercent = 0 // Single contiguous free block = no fragmentation
	} else {
		// Fragmentation = 1 - (largest_free_block / total_free)
		// 0% = all free space is contiguous
		// 100% = free space is maximally fragmented
		fragmentPercent = int32(100 - (largestFree * 100 / totalFree))
	}

	info := &FragmentationInfo{
		FragmentationPercent: fragmentPercent,
		FreeBlockCount:       int32(len(freeBlocks)),
		TotalFreeIPs:         capInt32(totalFree),
	}
	if largestFree > math.MaxInt32 {
		info.LargestFreeBlock = math.MaxInt32
	} else {
		info.LargestFreeBlock = int32(largestFree)
	}

	return info, nil
}

// ValidatePoolSpec validates the internal consistency of a network pool specification.
func ValidatePoolSpec(cidr string, reservedCIDRs []string, allocStart, allocEnd string) []string {
	var errs []string

	_, _, _, err := ParseCIDR(cidr)
	if err != nil {
		errs = append(errs, fmt.Sprintf("invalid pool CIDR: %v", err))
		return errs // Can't validate further
	}

	// Validate reserved ranges are within pool CIDR
	for _, reserved := range reservedCIDRs {
		contained, err := ValidateCIDRContains(cidr, reserved)
		if err != nil {
			errs = append(errs, fmt.Sprintf("invalid reserved CIDR %q: %v", reserved, err))
		} else if !contained {
			errs = append(errs, fmt.Sprintf("reserved CIDR %q is not within pool CIDR %q", reserved, cidr))
		}
	}

	// Validate allocation range is within pool CIDR
	if allocStart != "" && allocEnd != "" {
		within, err := ValidateRangeWithinCIDR(cidr, allocStart, allocEnd)
		if err != nil {
			errs = append(errs, fmt.Sprintf("invalid allocation range: %v", err))
		} else if !within {
			errs = append(errs, fmt.Sprintf("allocation range %s-%s is not within pool CIDR %q", allocStart, allocEnd, cidr))
		}

		startIP := net.ParseIP(allocStart).To4()
		endIP := net.ParseIP(allocEnd).To4()
		if startIP != nil && endIP != nil {
			if IPToUint32(startIP) > IPToUint32(endIP) {
				errs = append(errs, fmt.Sprintf("allocation start %s is after end %s", allocStart, allocEnd))
			}
		}
	}

	return errs
}

// PoolCapacityInfo computes capacity information for a pool.
func PoolCapacityInfo(pool PoolState) (totalIPs int32, allocatedIPs int32, availableIPs int32, err error) {
	bitmap, _, err := BuildBitmap(pool)
	if err != nil {
		return 0, 0, 0, err
	}

	total := int32(len(bitmap))
	reserved := int32(0)
	allocated := int32(0)

	// Count reserved IPs (from reserved CIDRs — always marked, even if no allocations exist)
	// We need to distinguish reserved from allocated. Build a separate bitmap for just reserved.
	startIP := net.ParseIP(pool.AllocatableStart).To4()
	poolStartUint := IPToUint32(startIP)

	reservedBitmap := make([]bool, len(bitmap))
	for _, cidr := range pool.ReservedCIDRs {
		rStart, rEnd, _, parseErr := ParseCIDR(cidr)
		if parseErr != nil {
			continue
		}
		rs := IPToUint32(rStart.To4())
		re := IPToUint32(rEnd.To4())
		for i := rs; i <= re; i++ {
			if i >= poolStartUint && i <= poolStartUint+uint32(len(bitmap))-1 {
				reservedBitmap[i-poolStartUint] = true
			}
		}
	}

	for i, used := range bitmap {
		if reservedBitmap[i] {
			reserved++
		} else if used {
			allocated++
		}
	}

	usable := total - reserved
	available := usable - allocated

	return usable, allocated, available, nil
}

// SortPoolRefsByPriority sorts pool references by priority (lower = higher priority).
// Stable sort preserves order for equal priorities.
func SortPoolRefsByPriority(refs []PoolRef) {
	sort.SliceStable(refs, func(i, j int) bool {
		pi := int32(0)
		pj := int32(0)
		if refs[i].Priority != nil {
			pi = *refs[i].Priority
		}
		if refs[j].Priority != nil {
			pj = *refs[j].Priority
		}
		return pi < pj
	})
}

// PoolRef is a simplified pool reference for sorting.
type PoolRef struct {
	Name     string
	Priority *int32
}

func capInt32(v uint32) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(v)
}

// ParseIPRange parses a range string that could be either CIDR notation or "start-end" format.
func ParseIPRange(rangeStr string) (start, end string, err error) {
	if strings.Contains(rangeStr, "/") {
		// CIDR notation
		startIP, endIP, _, err := ParseCIDR(rangeStr)
		if err != nil {
			return "", "", err
		}
		return startIP.String(), endIP.String(), nil
	}

	if strings.Contains(rangeStr, "-") {
		parts := strings.SplitN(rangeStr, "-", 2)
		startIP := net.ParseIP(strings.TrimSpace(parts[0])).To4()
		endIP := net.ParseIP(strings.TrimSpace(parts[1])).To4()
		if startIP == nil || endIP == nil {
			return "", "", fmt.Errorf("invalid IP range: %s", rangeStr)
		}
		return startIP.String(), endIP.String(), nil
	}

	// Single IP
	ip := net.ParseIP(rangeStr).To4()
	if ip == nil {
		return "", "", fmt.Errorf("invalid IP: %s", rangeStr)
	}
	return ip.String(), ip.String(), nil
}
