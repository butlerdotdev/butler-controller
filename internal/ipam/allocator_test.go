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
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// --- IPToUint32 / Uint32ToIP ---

func TestIPToUint32(t *testing.T) {
	tests := []struct {
		name string
		ip   string
		want uint32
	}{
		{"zero", "0.0.0.0", 0},
		{"one", "0.0.0.1", 1},
		{"class A", "10.0.0.1", 0x0a000001},
		{"class C", "192.168.1.1", 0xc0a80101},
		{"broadcast", "255.255.255.255", 0xffffffff},
		{"10.40.0.128", "10.40.0.128", 0x0a280080},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			got := IPToUint32(ip)
			if got != tt.want {
				t.Errorf("IPToUint32(%s) = %d, want %d", tt.ip, got, tt.want)
			}
		})
	}
}

func TestIPToUint32_Nil(t *testing.T) {
	got := IPToUint32(nil)
	if got != 0 {
		t.Errorf("IPToUint32(nil) = %d, want 0", got)
	}
}

func TestUint32ToIP(t *testing.T) {
	tests := []struct {
		name string
		n    uint32
		want string
	}{
		{"zero", 0, "0.0.0.0"},
		{"one", 1, "0.0.0.1"},
		{"max", 0xffffffff, "255.255.255.255"},
		{"10.40.0.128", 0x0a280080, "10.40.0.128"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Uint32ToIP(tt.n).String()
			if got != tt.want {
				t.Errorf("Uint32ToIP(%d) = %s, want %s", tt.n, got, tt.want)
			}
		})
	}
}

func TestIPRoundTrip(t *testing.T) {
	ips := []string{"10.40.0.1", "192.168.0.0", "172.16.255.254", "0.0.0.0", "255.255.255.255"}
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr).To4()
		result := Uint32ToIP(IPToUint32(ip))
		if !ip.Equal(result) {
			t.Errorf("round-trip failed for %s: got %s", ipStr, result)
		}
	}
}

// --- ParseCIDR ---

func TestParseCIDR(t *testing.T) {
	tests := []struct {
		name      string
		cidr      string
		wantStart string
		wantEnd   string
		wantCount int32
		wantErr   bool
	}{
		{"/32 single host", "10.0.0.1/32", "10.0.0.1", "10.0.0.1", 1, false},
		{"/31 two hosts", "10.0.0.0/31", "10.0.0.0", "10.0.0.1", 2, false},
		{"/30 four hosts", "10.0.0.0/30", "10.0.0.0", "10.0.0.3", 4, false},
		{"/29 eight hosts", "10.40.0.0/29", "10.40.0.0", "10.40.0.7", 8, false},
		{"/24 class C", "10.40.0.0/24", "10.40.0.0", "10.40.0.255", 256, false},
		{"/16 class B", "10.40.0.0/16", "10.40.0.0", "10.40.255.255", 65536, false},
		{"invalid CIDR", "not-a-cidr", "", "", 0, true},
		{"missing prefix", "10.0.0.0", "", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, count, err := ParseCIDR(tt.cidr)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseCIDR(%s) error = %v, wantErr = %v", tt.cidr, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if start.String() != tt.wantStart {
				t.Errorf("start = %s, want %s", start, tt.wantStart)
			}
			if end.String() != tt.wantEnd {
				t.Errorf("end = %s, want %s", end, tt.wantEnd)
			}
			if count != tt.wantCount {
				t.Errorf("count = %d, want %d", count, tt.wantCount)
			}
		})
	}
}

// --- CountIPsInRange ---

func TestCountIPsInRange(t *testing.T) {
	tests := []struct {
		name    string
		start   string
		end     string
		want    int32
		wantErr bool
	}{
		{"single IP", "10.0.0.1", "10.0.0.1", 1, false},
		{"two IPs", "10.0.0.1", "10.0.0.2", 2, false},
		{"256 IPs", "10.0.0.0", "10.0.0.255", 256, false},
		{"reversed", "10.0.0.2", "10.0.0.1", 0, true},
		{"invalid start", "bad", "10.0.0.1", 0, true},
		{"invalid end", "10.0.0.1", "bad", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CountIPsInRange(tt.start, tt.end)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CountIPsInRange(%s, %s) error = %v, wantErr = %v", tt.start, tt.end, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

// --- EnumerateIPs ---

func TestEnumerateIPs(t *testing.T) {
	tests := []struct {
		name    string
		start   string
		end     string
		want    int
		wantErr bool
	}{
		{"single IP", "10.0.0.1", "10.0.0.1", 1, false},
		{"three IPs", "10.0.0.1", "10.0.0.3", 3, false},
		{"full /24", "10.0.0.0", "10.0.0.255", 256, false},
		{"reversed", "10.0.0.2", "10.0.0.1", 0, true},
		{"too large", "10.0.0.0", "10.1.0.0", 0, true}, // 65537 IPs > 65536 cap
		{"invalid IP", "bad", "10.0.0.1", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EnumerateIPs(tt.start, tt.end)
			if (err != nil) != tt.wantErr {
				t.Fatalf("EnumerateIPs(%s, %s) error = %v, wantErr = %v", tt.start, tt.end, err, tt.wantErr)
			}
			if !tt.wantErr {
				if len(got) != tt.want {
					t.Errorf("got %d IPs, want %d", len(got), tt.want)
				}
				// Verify first and last
				if len(got) > 0 {
					if got[0] != tt.start {
						t.Errorf("first IP = %s, want %s", got[0], tt.start)
					}
					if got[len(got)-1] != tt.end {
						t.Errorf("last IP = %s, want %s", got[len(got)-1], tt.end)
					}
				}
			}
		})
	}
}

func TestEnumerateIPs_ExactCap(t *testing.T) {
	// 65536 IPs should succeed (exactly at cap)
	ips, err := EnumerateIPs("10.0.0.0", "10.0.255.255")
	if err != nil {
		t.Fatalf("unexpected error for 65536 IPs: %v", err)
	}
	if len(ips) != 65536 {
		t.Errorf("got %d IPs, want 65536", len(ips))
	}
}

// --- FormatCIDR ---

func TestFormatCIDR(t *testing.T) {
	tests := []struct {
		name  string
		start string
		end   string
		want  string
	}{
		{"/32 aligned", "10.0.0.1", "10.0.0.1", "10.0.0.1/32"},
		{"/31 aligned", "10.0.0.0", "10.0.0.1", "10.0.0.0/31"},
		{"/30 aligned", "10.0.0.0", "10.0.0.3", "10.0.0.0/30"},
		{"/29 aligned", "10.0.0.0", "10.0.0.7", "10.0.0.0/29"},
		{"/28 aligned", "10.0.0.0", "10.0.0.15", "10.0.0.0/28"},
		{"/24 aligned", "10.0.0.0", "10.0.0.255", "10.0.0.0/24"},
		{"not power of 2 (3 IPs)", "10.0.0.1", "10.0.0.3", "10.0.0.1-10.0.0.3"},
		{"power of 2 but unaligned", "10.0.0.1", "10.0.0.2", "10.0.0.1-10.0.0.2"},
		{"5 IPs", "10.0.0.0", "10.0.0.4", "10.0.0.0-10.0.0.4"},
		{"reversed range", "10.0.0.5", "10.0.0.1", "10.0.0.5-10.0.0.1"},
		{"invalid start", "bad", "10.0.0.1", "bad-10.0.0.1"},
		{"aligned /29 at offset", "10.0.0.8", "10.0.0.15", "10.0.0.8/29"},
		{"aligned /28 at offset", "10.0.0.16", "10.0.0.31", "10.0.0.16/28"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatCIDR(tt.start, tt.end)
			if got != tt.want {
				t.Errorf("FormatCIDR(%s, %s) = %q, want %q", tt.start, tt.end, got, tt.want)
			}
		})
	}
}

// --- ValidateCIDRContains ---

func TestValidateCIDRContains(t *testing.T) {
	tests := []struct {
		name    string
		outer   string
		inner   string
		want    bool
		wantErr bool
	}{
		{"same CIDR", "10.40.0.0/24", "10.40.0.0/24", true, false},
		{"inner within outer", "10.40.0.0/24", "10.40.0.0/26", true, false},
		{"inner at end of outer", "10.40.0.0/24", "10.40.0.192/26", true, false},
		{"inner outside outer", "10.40.0.0/24", "10.40.1.0/26", false, false},
		{"inner partially overlaps", "10.40.0.0/25", "10.40.0.64/26", true, false},
		{"inner extends beyond", "10.40.0.0/25", "10.40.0.0/24", false, false},
		{"invalid outer", "bad", "10.0.0.0/24", false, true},
		{"invalid inner", "10.0.0.0/24", "bad", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ValidateCIDRContains(tt.outer, tt.inner)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// --- ValidateRangeWithinCIDR ---

func TestValidateRangeWithinCIDR(t *testing.T) {
	tests := []struct {
		name    string
		cidr    string
		start   string
		end     string
		want    bool
		wantErr bool
	}{
		{"range within /24", "10.40.0.0/24", "10.40.0.10", "10.40.0.200", true, false},
		{"full /24", "10.40.0.0/24", "10.40.0.0", "10.40.0.255", true, false},
		{"start outside", "10.40.0.0/24", "10.39.255.255", "10.40.0.10", false, false},
		{"end outside", "10.40.0.0/24", "10.40.0.200", "10.40.1.5", false, false},
		{"invalid CIDR", "bad", "10.0.0.1", "10.0.0.2", false, true},
		{"invalid IP", "10.0.0.0/24", "bad", "10.0.0.1", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ValidateRangeWithinCIDR(tt.cidr, tt.start, tt.end)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// --- BuildBitmap ---

func TestBuildBitmap(t *testing.T) {
	t.Run("empty pool", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.7"}},
		}
		bitmap, startUint, err := BuildBitmap(pool)
		if err != nil {
			t.Fatal(err)
		}
		if len(bitmap) != 8 {
			t.Fatalf("bitmap length = %d, want 8", len(bitmap))
		}
		if startUint != IPToUint32(net.ParseIP("10.0.0.0")) {
			t.Errorf("startUint mismatch")
		}
		for i, used := range bitmap {
			if used {
				t.Errorf("bitmap[%d] = true, want false (empty pool)", i)
			}
		}
	})

	t.Run("with reserved CIDR", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.15"}},
			ReservedCIDRs:     []string{"10.0.0.0/30"}, // reserves .0-.3
		}
		bitmap, _, err := BuildBitmap(pool)
		if err != nil {
			t.Fatal(err)
		}
		// First 4 IPs should be reserved
		for i := 0; i < 4; i++ {
			if !bitmap[i] {
				t.Errorf("bitmap[%d] should be reserved (true)", i)
			}
		}
		for i := 4; i < 16; i++ {
			if bitmap[i] {
				t.Errorf("bitmap[%d] should be free (false)", i)
			}
		}
	})

	t.Run("with existing allocation", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.15"}},
			ExistingAllocs: []AllocatedRange{
				{Start: "10.0.0.4", End: "10.0.0.7"},
			},
		}
		bitmap, _, err := BuildBitmap(pool)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 4; i++ {
			if bitmap[i] {
				t.Errorf("bitmap[%d] should be free", i)
			}
		}
		for i := 4; i < 8; i++ {
			if !bitmap[i] {
				t.Errorf("bitmap[%d] should be allocated (true)", i)
			}
		}
		for i := 8; i < 16; i++ {
			if bitmap[i] {
				t.Errorf("bitmap[%d] should be free", i)
			}
		}
	})

	t.Run("reserved outside allocatable range ignored", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{{Start: "10.0.0.128", End: "10.0.0.135"}},
			ReservedCIDRs:     []string{"10.0.0.0/25"}, // entirely before allocatable range
		}
		bitmap, _, err := BuildBitmap(pool)
		if err != nil {
			t.Fatal(err)
		}
		for i, used := range bitmap {
			if used {
				t.Errorf("bitmap[%d] should be free (reserved range outside pool)", i)
			}
		}
	})

	t.Run("invalid range", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{{Start: "10.0.0.10", End: "10.0.0.5"}},
		}
		_, _, err := BuildBitmap(pool)
		if err == nil {
			t.Fatal("expected error for reversed range")
		}
	})

	t.Run("pool too large", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.255.255.255"}}, // ~16M IPs
		}
		_, _, err := BuildBitmap(pool)
		if err == nil {
			t.Fatal("expected error for pool > 1M IPs")
		}
	})
}

// --- AllocateRange ---

func TestAllocateRange(t *testing.T) {
	tests := []struct {
		name           string
		pool           PoolState
		requestedCount int32
		wantStart      string
		wantEnd        string
		wantErr        error
	}{
		{
			name: "empty pool first allocation",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.255"}},
			},
			requestedCount: 8,
			wantStart:      "10.0.0.0",
			wantEnd:        "10.0.0.7",
		},
		{
			name: "single IP allocation",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.255"}},
			},
			requestedCount: 1,
			wantStart:      "10.0.0.0",
			wantEnd:        "10.0.0.0",
		},
		{
			name: "skips existing allocation",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.31"}},
				ExistingAllocs: []AllocatedRange{
					{Start: "10.0.0.0", End: "10.0.0.7"},
				},
			},
			requestedCount: 8,
			wantStart:      "10.0.0.8",
			wantEnd:        "10.0.0.15",
		},
		{
			name: "skips reserved range",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.31"}},
				ReservedCIDRs:     []string{"10.0.0.0/29"}, // .0-.7
			},
			requestedCount: 4,
			wantStart:      "10.0.0.8",
			wantEnd:        "10.0.0.11",
		},
		{
			name: "fills gap between allocations",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.31"}},
				ExistingAllocs: []AllocatedRange{
					{Start: "10.0.0.0", End: "10.0.0.3"},
					{Start: "10.0.0.12", End: "10.0.0.15"},
				},
			},
			// Gap of 8 IPs (.4-.11), gap of 16 IPs (.16-.31)
			// Best-fit should pick the 8-IP gap for a request of 8
			requestedCount: 8,
			wantStart:      "10.0.0.4",
			wantEnd:        "10.0.0.11",
		},
		{
			name: "best-fit picks smallest fitting gap",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.63"}},
				ExistingAllocs: []AllocatedRange{
					{Start: "10.0.0.0", End: "10.0.0.2"},   // uses .0-.2
					{Start: "10.0.0.13", End: "10.0.0.14"}, // uses .13-.14 → gap .3-.12 = 10 IPs
					{Start: "10.0.0.15", End: "10.0.0.17"}, // gap = 0 after .14
					{Start: "10.0.0.68", End: "10.0.0.68"}, // outside pool, ignored
				},
			},
			// Free blocks: .3-.12 (10 IPs), .18-.63 (46 IPs)
			// Request 5: best-fit picks .3-.12 (size 10) over .18-.63 (size 46)
			requestedCount: 5,
			wantStart:      "10.0.0.3",
			wantEnd:        "10.0.0.7",
		},
		{
			name: "best-fit with gaps of 10, 50, 3 - request 5 picks 10",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.99"}},
				ExistingAllocs: []AllocatedRange{
					// Layout: [10 free][5 alloc][3 free][2 alloc][50 free][30 alloc]
					// .0-.9 free (10)
					{Start: "10.0.0.10", End: "10.0.0.14"}, // 5 allocated
					// .15-.17 free (3)
					{Start: "10.0.0.18", End: "10.0.0.19"}, // 2 allocated
					// .20-.69 free (50)
					{Start: "10.0.0.70", End: "10.0.0.99"}, // 30 allocated
				},
			},
			// Free blocks: [10, 3, 50]
			// Request 5: best-fit picks 10 (smallest >= 5), not 50 or 3
			requestedCount: 5,
			wantStart:      "10.0.0.0",
			wantEnd:        "10.0.0.4",
		},
		{
			name: "exhausted pool",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.7"}},
				ExistingAllocs: []AllocatedRange{
					{Start: "10.0.0.0", End: "10.0.0.7"},
				},
			},
			requestedCount: 1,
			wantErr:        ErrPoolExhausted,
		},
		{
			name: "no block large enough",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.31"}},
				ExistingAllocs: []AllocatedRange{
					{Start: "10.0.0.4", End: "10.0.0.7"},
					{Start: "10.0.0.12", End: "10.0.0.15"},
					{Start: "10.0.0.20", End: "10.0.0.23"},
					{Start: "10.0.0.28", End: "10.0.0.31"},
				},
			},
			// Free blocks: .0-.3 (4), .8-.11 (4), .16-.19 (4), .24-.27 (4)
			// Request 5: no block >= 5
			requestedCount: 5,
			wantErr:        ErrPoolExhausted,
		},
		{
			name: "full pool minus one",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.7"}},
				ExistingAllocs: []AllocatedRange{
					{Start: "10.0.0.0", End: "10.0.0.6"},
				},
			},
			requestedCount: 1,
			wantStart:      "10.0.0.7",
			wantEnd:        "10.0.0.7",
		},
		{
			name: "entire pool as one allocation",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.255"}},
			},
			requestedCount: 256,
			wantStart:      "10.0.0.0",
			wantEnd:        "10.0.0.255",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AllocateRange(tt.pool, tt.requestedCount)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("got error %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Start != tt.wantStart {
				t.Errorf("start = %s, want %s", result.Start, tt.wantStart)
			}
			if result.End != tt.wantEnd {
				t.Errorf("end = %s, want %s", result.End, tt.wantEnd)
			}
			if result.Count != tt.requestedCount {
				t.Errorf("count = %d, want %d", result.Count, tt.requestedCount)
			}
			if len(result.Addresses) != int(tt.requestedCount) {
				t.Errorf("addresses length = %d, want %d", len(result.Addresses), tt.requestedCount)
			}
		})
	}
}

func TestAllocateRange_InvalidCount(t *testing.T) {
	pool := PoolState{
		AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.255"}},
	}

	_, err := AllocateRange(pool, 0)
	if err == nil {
		t.Fatal("expected error for count=0")
	}

	_, err = AllocateRange(pool, -1)
	if err == nil {
		t.Fatal("expected error for count=-1")
	}
}

func TestAllocateRange_NoOverlap(t *testing.T) {
	// Simulate sequential allocations and verify no overlap
	pool := PoolState{
		AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.255"}},
	}

	allocations := make([]AllocationResult, 0)
	for i := 0; i < 32; i++ { // 32 * 8 = 256 IPs
		result, err := AllocateRange(pool, 8)
		if err != nil {
			t.Fatalf("allocation %d failed: %v", i, err)
		}
		allocations = append(allocations, *result)
		pool.ExistingAllocs = append(pool.ExistingAllocs, AllocatedRange{
			Start: result.Start,
			End:   result.End,
		})
	}

	// Verify no overlap between any pair of allocations
	for i := 0; i < len(allocations); i++ {
		iStart := IPToUint32(net.ParseIP(allocations[i].Start))
		iEnd := IPToUint32(net.ParseIP(allocations[i].End))
		for j := i + 1; j < len(allocations); j++ {
			jStart := IPToUint32(net.ParseIP(allocations[j].Start))
			jEnd := IPToUint32(net.ParseIP(allocations[j].End))
			if iStart <= jEnd && jStart <= iEnd {
				t.Errorf("overlap between allocation %d (%s-%s) and %d (%s-%s)",
					i, allocations[i].Start, allocations[i].End,
					j, allocations[j].Start, allocations[j].End)
			}
		}
	}

	// Pool should be exhausted now
	_, err := AllocateRange(pool, 1)
	if !errors.Is(err, ErrPoolExhausted) {
		t.Errorf("expected ErrPoolExhausted after filling pool, got %v", err)
	}
}

func TestAllocateRange_WithReservedAndAllocated(t *testing.T) {
	// Complex scenario: reserved range in the middle, existing allocations around it
	pool := PoolState{
		AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.63"}},
		ReservedCIDRs:     []string{"10.0.0.16/28"}, // .16-.31 reserved
		ExistingAllocs: []AllocatedRange{
			{Start: "10.0.0.0", End: "10.0.0.7"}, // .0-.7 allocated
		},
	}

	// Free: .8-.15 (8 IPs), .32-.63 (32 IPs)
	// Request 8: best-fit picks .8-.15 (exact fit)
	result, err := AllocateRange(pool, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Start != "10.0.0.8" || result.End != "10.0.0.15" {
		t.Errorf("got %s-%s, want 10.0.0.8-10.0.0.15", result.Start, result.End)
	}
}

func TestAllocateRange_LargePool(t *testing.T) {
	// /16 = 65536 IPs — verify it doesn't OOM and completes quickly
	pool := PoolState{
		AllocatableRanges: []AllocatedRange{{Start: "10.40.0.0", End: "10.40.255.255"}},
	}

	start := time.Now()
	result, err := AllocateRange(pool, 8)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Start != "10.40.0.0" {
		t.Errorf("start = %s, want 10.40.0.0", result.Start)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("allocation took %v, want < 100ms", elapsed)
	}
}

func TestAllocateRange_LargePoolFragmented(t *testing.T) {
	// /16 pool with every other 8-block allocated (severe fragmentation)
	pool := PoolState{
		AllocatableRanges: []AllocatedRange{{Start: "10.40.0.0", End: "10.40.255.255"}},
	}

	// Allocate every other 8-IP block for first 1024 IPs
	baseIP := IPToUint32(net.ParseIP("10.40.0.0"))
	for i := uint32(0); i < 64; i++ {
		s := baseIP + i*16
		e := s + 7
		pool.ExistingAllocs = append(pool.ExistingAllocs, AllocatedRange{
			Start: Uint32ToIP(s).String(),
			End:   Uint32ToIP(e).String(),
		})
	}

	start := time.Now()
	result, err := AllocateRange(pool, 8)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Best-fit should pick one of the 8-IP gaps between allocated blocks
	if result.Count != 8 {
		t.Errorf("count = %d, want 8", result.Count)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("allocation took %v, want < 100ms", elapsed)
	}
}

// --- ComputeFragmentation ---

func TestComputeFragmentation(t *testing.T) {
	tests := []struct {
		name               string
		pool               PoolState
		wantFragPercent    int32
		wantLargestFree    int32
		wantFreeBlockCount int32
		wantTotalFreeIPs   int32
	}{
		{
			name: "empty pool - no fragmentation",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.31"}},
			},
			wantFragPercent:    0,
			wantLargestFree:    32,
			wantFreeBlockCount: 1,
			wantTotalFreeIPs:   32,
		},
		{
			name: "full pool - no fragmentation",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.31"}},
				ExistingAllocs: []AllocatedRange{
					{Start: "10.0.0.0", End: "10.0.0.31"},
				},
			},
			wantFragPercent:    0,
			wantLargestFree:    0,
			wantFreeBlockCount: 0,
			wantTotalFreeIPs:   0,
		},
		{
			name: "one allocation in middle - two free blocks",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.31"}},
				ExistingAllocs: []AllocatedRange{
					{Start: "10.0.0.8", End: "10.0.0.15"},
				},
			},
			// Free: .0-.7 (8 IPs), .16-.31 (16 IPs) = 24 total free, 2 blocks
			// Fragmentation = 100 - (16*100/24) = 100 - 66 = 34% (integer division)
			wantFragPercent:    34,
			wantLargestFree:    16,
			wantFreeBlockCount: 2,
			wantTotalFreeIPs:   24,
		},
		{
			name: "alternating allocated/free - high fragmentation",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.31"}},
				ExistingAllocs: []AllocatedRange{
					{Start: "10.0.0.2", End: "10.0.0.3"},
					{Start: "10.0.0.6", End: "10.0.0.7"},
					{Start: "10.0.0.10", End: "10.0.0.11"},
					{Start: "10.0.0.14", End: "10.0.0.15"},
					{Start: "10.0.0.18", End: "10.0.0.19"},
					{Start: "10.0.0.22", End: "10.0.0.23"},
					{Start: "10.0.0.26", End: "10.0.0.27"},
					{Start: "10.0.0.30", End: "10.0.0.31"},
				},
			},
			// Free blocks: 8 blocks of 2 IPs each = 16 total free
			// Fragmentation = 100 - (2*100/16) = 100 - 12 = 88% (integer division)
			wantFragPercent:    88,
			wantLargestFree:    2,
			wantFreeBlockCount: 8,
			wantTotalFreeIPs:   16,
		},
		{
			name: "single contiguous free block at end",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.31"}},
				ExistingAllocs: []AllocatedRange{
					{Start: "10.0.0.0", End: "10.0.0.15"},
				},
			},
			wantFragPercent:    0,
			wantLargestFree:    16,
			wantFreeBlockCount: 1,
			wantTotalFreeIPs:   16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := ComputeFragmentation(tt.pool)
			if err != nil {
				t.Fatal(err)
			}
			if info.FragmentationPercent != tt.wantFragPercent {
				t.Errorf("FragmentationPercent = %d, want %d", info.FragmentationPercent, tt.wantFragPercent)
			}
			if info.LargestFreeBlock != tt.wantLargestFree {
				t.Errorf("LargestFreeBlock = %d, want %d", info.LargestFreeBlock, tt.wantLargestFree)
			}
			if info.FreeBlockCount != tt.wantFreeBlockCount {
				t.Errorf("FreeBlockCount = %d, want %d", info.FreeBlockCount, tt.wantFreeBlockCount)
			}
			if info.TotalFreeIPs != tt.wantTotalFreeIPs {
				t.Errorf("TotalFreeIPs = %d, want %d", info.TotalFreeIPs, tt.wantTotalFreeIPs)
			}
		})
	}
}

// --- ValidatePoolSpec ---

func TestValidatePoolSpec(t *testing.T) {
	tests := []struct {
		name         string
		cidr         string
		reserved     []string
		ranges       []AllocatedRange
		wantErrCount int
	}{
		{"valid single range", "10.40.0.0/24", []string{"10.40.0.0/26"}, []AllocatedRange{{Start: "10.40.0.128", End: "10.40.0.255"}}, 0},
		{"invalid CIDR", "bad", nil, nil, 1},
		{"reserved outside pool", "10.40.0.0/24", []string{"10.41.0.0/24"}, nil, 1},
		{"range outside pool", "10.40.0.0/24", nil, []AllocatedRange{{Start: "10.41.0.0", End: "10.41.0.255"}}, 1},
		{"range start after end", "10.40.0.0/24", nil, []AllocatedRange{{Start: "10.40.0.200", End: "10.40.0.100"}}, 1},
		{"multiple errors", "10.40.0.0/24", []string{"10.41.0.0/24"}, []AllocatedRange{{Start: "10.42.0.0", End: "10.42.0.255"}}, 2},
		{"no ranges", "10.40.0.0/24", []string{"10.40.0.0/26"}, nil, 0},
		{"invalid reserved CIDR", "10.40.0.0/24", []string{"bad"}, nil, 1},
		// Multi-range validation
		{"valid multi-range sorted", "10.40.0.0/24", nil, []AllocatedRange{
			{Start: "10.40.0.0", End: "10.40.0.63"},
			{Start: "10.40.0.128", End: "10.40.0.255"},
		}, 0},
		{"multi-range unsorted", "10.40.0.0/24", nil, []AllocatedRange{
			{Start: "10.40.0.128", End: "10.40.0.255"},
			{Start: "10.40.0.0", End: "10.40.0.63"},
		}, 1},
		{"multi-range overlapping", "10.40.0.0/24", nil, []AllocatedRange{
			{Start: "10.40.0.0", End: "10.40.0.100"},
			{Start: "10.40.0.50", End: "10.40.0.200"},
		}, 1},
		{"multi-range one outside CIDR", "10.40.0.0/24", nil, []AllocatedRange{
			{Start: "10.40.0.0", End: "10.40.0.63"},
			{Start: "10.40.1.0", End: "10.40.1.50"},
		}, 1},
		{"reserved within range", "10.40.0.0/24", []string{"10.40.0.8/29"}, []AllocatedRange{
			{Start: "10.40.0.0", End: "10.40.0.63"},
		}, 0},
		{"reserved outside all ranges", "10.40.0.0/24", []string{"10.40.0.200/29"}, []AllocatedRange{
			{Start: "10.40.0.0", End: "10.40.0.63"},
		}, 0},
		{"reserved straddles range boundary", "10.40.0.0/24", []string{"10.40.0.0/26"}, []AllocatedRange{
			{Start: "10.40.0.0", End: "10.40.0.31"},
		}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := ValidatePoolSpec(tt.cidr, tt.ranges, tt.reserved)
			if len(errs) != tt.wantErrCount {
				t.Errorf("got %d errors, want %d: %v", len(errs), tt.wantErrCount, errs)
			}
		})
	}
}

// --- PoolCapacityInfo ---

func TestPoolCapacityInfo(t *testing.T) {
	tests := []struct {
		name          string
		pool          PoolState
		wantTotal     int32
		wantAllocated int32
		wantAvailable int32
	}{
		{
			name: "empty pool no reserved",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.31"}},
			},
			wantTotal: 32, wantAllocated: 0, wantAvailable: 32,
		},
		{
			name: "with reserved IPs",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.31"}},
				ReservedCIDRs:     []string{"10.0.0.0/29"}, // 8 reserved
			},
			wantTotal: 24, wantAllocated: 0, wantAvailable: 24,
		},
		{
			name: "with reserved and allocated",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.31"}},
				ReservedCIDRs:     []string{"10.0.0.0/29"}, // 8 reserved
				ExistingAllocs: []AllocatedRange{
					{Start: "10.0.0.8", End: "10.0.0.15"}, // 8 allocated
				},
			},
			wantTotal: 24, wantAllocated: 8, wantAvailable: 16,
		},
		{
			name: "fully allocated (non-reserved)",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.15"}},
				ReservedCIDRs:     []string{"10.0.0.0/29"}, // 8 reserved
				ExistingAllocs: []AllocatedRange{
					{Start: "10.0.0.8", End: "10.0.0.15"}, // 8 allocated
				},
			},
			wantTotal: 8, wantAllocated: 8, wantAvailable: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			total, allocated, available, err := PoolCapacityInfo(tt.pool)
			if err != nil {
				t.Fatal(err)
			}
			if total != tt.wantTotal {
				t.Errorf("total = %d, want %d", total, tt.wantTotal)
			}
			if allocated != tt.wantAllocated {
				t.Errorf("allocated = %d, want %d", allocated, tt.wantAllocated)
			}
			if available != tt.wantAvailable {
				t.Errorf("available = %d, want %d", available, tt.wantAvailable)
			}
		})
	}
}

// --- SortPoolRefsByPriority ---

func TestSortPoolRefsByPriority(t *testing.T) {
	p := func(v int32) *int32 { return &v }

	t.Run("sorts by priority", func(t *testing.T) {
		refs := []PoolRef{
			{Name: "pool-c", Priority: p(10)},
			{Name: "pool-a", Priority: p(0)},
			{Name: "pool-b", Priority: p(5)},
		}
		SortPoolRefsByPriority(refs)
		if refs[0].Name != "pool-a" || refs[1].Name != "pool-b" || refs[2].Name != "pool-c" {
			t.Errorf("wrong order: %v, %v, %v", refs[0].Name, refs[1].Name, refs[2].Name)
		}
	})

	t.Run("nil priority defaults to 0", func(t *testing.T) {
		refs := []PoolRef{
			{Name: "pool-b", Priority: p(5)},
			{Name: "pool-a", Priority: nil},
		}
		SortPoolRefsByPriority(refs)
		if refs[0].Name != "pool-a" || refs[1].Name != "pool-b" {
			t.Errorf("nil priority should sort first: %v, %v", refs[0].Name, refs[1].Name)
		}
	})

	t.Run("stable sort preserves order for equal priorities", func(t *testing.T) {
		refs := []PoolRef{
			{Name: "pool-a", Priority: p(0)},
			{Name: "pool-b", Priority: p(0)},
			{Name: "pool-c", Priority: p(0)},
		}
		SortPoolRefsByPriority(refs)
		if refs[0].Name != "pool-a" || refs[1].Name != "pool-b" || refs[2].Name != "pool-c" {
			t.Errorf("stable sort should preserve order: %v, %v, %v", refs[0].Name, refs[1].Name, refs[2].Name)
		}
	})
}

// --- ParseIPRange ---

func TestParseIPRange(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantStart string
		wantEnd   string
		wantErr   bool
	}{
		{"CIDR /29", "10.0.0.0/29", "10.0.0.0", "10.0.0.7", false},
		{"CIDR /32", "10.0.0.1/32", "10.0.0.1", "10.0.0.1", false},
		{"dash range", "10.0.0.10-10.0.0.20", "10.0.0.10", "10.0.0.20", false},
		{"dash range with spaces", "10.0.0.10 - 10.0.0.20", "10.0.0.10", "10.0.0.20", false},
		{"single IP", "10.0.0.5", "10.0.0.5", "10.0.0.5", false},
		{"invalid CIDR", "bad/24", "", "", true},
		{"invalid dash range", "bad-10.0.0.1", "", "", true},
		{"invalid single", "bad", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := ParseIPRange(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if start != tt.wantStart {
					t.Errorf("start = %s, want %s", start, tt.wantStart)
				}
				if end != tt.wantEnd {
					t.Errorf("end = %s, want %s", end, tt.wantEnd)
				}
			}
		})
	}
}

// --- findFreeBlocks ---

func TestFindFreeBlocks(t *testing.T) {
	tests := []struct {
		name    string
		bitmap  []bool
		want    int // number of free blocks
		wantMax uint32
	}{
		{"all free", []bool{false, false, false, false}, 1, 4},
		{"all used", []bool{true, true, true, true}, 0, 0},
		{"one free at start", []bool{false, true, true, true}, 1, 1},
		{"one free at end", []bool{true, true, true, false}, 1, 1},
		{"two blocks", []bool{false, false, true, false, false, false}, 2, 3},
		{"alternating", []bool{false, true, false, true, false, true, false, true}, 4, 1},
		{"empty bitmap", []bool{}, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blocks := findFreeBlocks(tt.bitmap)
			if len(blocks) != tt.want {
				t.Errorf("got %d blocks, want %d", len(blocks), tt.want)
			}
			var maxSize uint32
			for _, b := range blocks {
				if b.Size > maxSize {
					maxSize = b.Size
				}
			}
			if maxSize != tt.wantMax {
				t.Errorf("max block size = %d, want %d", maxSize, tt.wantMax)
			}
		})
	}
}

// --- Integration-style tests ---

func TestAllocateAndRelease_Workflow(t *testing.T) {
	// Simulate a real-world workflow:
	// 1. Create 4 allocations of 8 IPs each
	// 2. Release the 2nd allocation
	// 3. Allocate 4 IPs — best-fit should use the freed gap
	// 4. Allocate 8 IPs — should use remaining space

	pool := PoolState{
		AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.63"}},
	}

	// Step 1: 4 allocations of 8
	allocs := make([]*AllocationResult, 4)
	for i := 0; i < 4; i++ {
		result, err := AllocateRange(pool, 8)
		if err != nil {
			t.Fatalf("allocation %d failed: %v", i, err)
		}
		allocs[i] = result
		pool.ExistingAllocs = append(pool.ExistingAllocs, AllocatedRange{
			Start: result.Start, End: result.End,
		})
	}

	// Verify layout: .0-.7, .8-.15, .16-.23, .24-.31
	if allocs[0].Start != "10.0.0.0" || allocs[1].Start != "10.0.0.8" ||
		allocs[2].Start != "10.0.0.16" || allocs[3].Start != "10.0.0.24" {
		t.Fatalf("unexpected initial layout")
	}

	// Step 2: Release 2nd allocation (.8-.15)
	pool.ExistingAllocs = []AllocatedRange{
		{Start: "10.0.0.0", End: "10.0.0.7"},
		{Start: "10.0.0.16", End: "10.0.0.23"},
		{Start: "10.0.0.24", End: "10.0.0.31"},
	}

	// Step 3: Allocate 4 IPs
	// Free blocks: .8-.15 (8 IPs), .32-.63 (32 IPs)
	// Best-fit picks .8-.15 (size 8, smallest >= 4)
	result4, err := AllocateRange(pool, 4)
	if err != nil {
		t.Fatalf("4-IP allocation failed: %v", err)
	}
	if result4.Start != "10.0.0.8" || result4.End != "10.0.0.11" {
		t.Errorf("4-IP alloc: got %s-%s, want 10.0.0.8-10.0.0.11", result4.Start, result4.End)
	}

	pool.ExistingAllocs = append(pool.ExistingAllocs, AllocatedRange{
		Start: result4.Start, End: result4.End,
	})

	// Step 4: Allocate 8 IPs
	// Free blocks: .12-.15 (4 IPs), .32-.63 (32 IPs)
	// Best-fit cannot use .12-.15 (too small), uses .32-.63
	result8, err := AllocateRange(pool, 8)
	if err != nil {
		t.Fatalf("8-IP allocation failed: %v", err)
	}
	// Should NOT be .12-.15 (only 4 free) — should be from the large block
	if result8.Start != "10.0.0.32" || result8.End != "10.0.0.39" {
		t.Errorf("8-IP alloc: got %s-%s, want 10.0.0.32-10.0.0.39", result8.Start, result8.End)
	}
}

func TestAllocateRange_CIDRNotation(t *testing.T) {
	// Verify power-of-2 aligned allocations produce CIDR notation
	pool := PoolState{
		AllocatableRanges: []AllocatedRange{{Start: "10.0.0.0", End: "10.0.0.255"}},
	}

	// Allocate 8 IPs starting at .0 — should be 10.0.0.0/29
	result, err := AllocateRange(pool, 8)
	if err != nil {
		t.Fatal(err)
	}
	if result.CIDR != "10.0.0.0/29" {
		t.Errorf("CIDR = %s, want 10.0.0.0/29", result.CIDR)
	}

	// Allocate 5 IPs — not power of 2, should be range format
	pool.ExistingAllocs = []AllocatedRange{
		{Start: "10.0.0.0", End: "10.0.0.7"},
	}
	result, err = AllocateRange(pool, 5)
	if err != nil {
		t.Fatal(err)
	}
	if result.CIDR != "10.0.0.8-10.0.0.12" {
		t.Errorf("CIDR = %s, want 10.0.0.8-10.0.0.12", result.CIDR)
	}
}

func TestAllocatePinnedRange(t *testing.T) {
	pool := PoolState{
		AllocatableRanges: []AllocatedRange{{Start: "10.40.0.0", End: "10.40.0.255"}},
	}

	tests := []struct {
		name        string
		pool        PoolState
		start       string
		end         string
		wantErr     bool
		errContains string
		wantCount   int32
	}{
		{
			name:      "valid pinned range",
			pool:      pool,
			start:     "10.40.0.10",
			end:       "10.40.0.17",
			wantCount: 8,
		},
		{
			name:      "single IP",
			pool:      pool,
			start:     "10.40.0.100",
			end:       "10.40.0.100",
			wantCount: 1,
		},
		{
			name:        "start after end",
			pool:        pool,
			start:       "10.40.0.20",
			end:         "10.40.0.10",
			wantErr:     true,
			errContains: "start",
		},
		{
			name:        "outside pool - before",
			pool:        PoolState{AllocatableRanges: []AllocatedRange{{Start: "10.40.0.64", End: "10.40.0.127"}}},
			start:       "10.40.0.10",
			end:         "10.40.0.17",
			wantErr:     true,
			errContains: "outside allocatable ranges",
		},
		{
			name:        "outside pool - after",
			pool:        PoolState{AllocatableRanges: []AllocatedRange{{Start: "10.40.0.0", End: "10.40.0.127"}}},
			start:       "10.40.0.200",
			end:         "10.40.0.207",
			wantErr:     true,
			errContains: "outside allocatable ranges",
		},
		{
			name: "straddles two ranges",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{
					{Start: "10.92.90.32", End: "10.92.90.63"},
					{Start: "10.92.91.51", End: "10.92.91.191"},
				},
			},
			start:       "10.92.90.62",
			end:         "10.92.91.55",
			wantErr:     true,
			errContains: "straddles allocatable ranges",
		},
		{
			name: "conflicts with reserved",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.40.0.0", End: "10.40.0.255"}},
				ReservedCIDRs:     []string{"10.40.0.0/28"},
			},
			start:       "10.40.0.10",
			end:         "10.40.0.17",
			wantErr:     true,
			errContains: "conflicts",
		},
		{
			name: "conflicts with existing allocation",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.40.0.0", End: "10.40.0.255"}},
				ExistingAllocs:    []AllocatedRange{{Start: "10.40.0.10", End: "10.40.0.17"}},
			},
			start:       "10.40.0.14",
			end:         "10.40.0.21",
			wantErr:     true,
			errContains: "conflicts",
		},
		{
			name: "adjacent to existing - no conflict",
			pool: PoolState{
				AllocatableRanges: []AllocatedRange{{Start: "10.40.0.0", End: "10.40.0.255"}},
				ExistingAllocs:    []AllocatedRange{{Start: "10.40.0.10", End: "10.40.0.17"}},
			},
			start:     "10.40.0.18",
			end:       "10.40.0.25",
			wantCount: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AllocatePinnedRange(tt.pool, tt.start, tt.end)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errContains)
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Start != tt.start {
				t.Errorf("start = %s, want %s", result.Start, tt.start)
			}
			if result.End != tt.end {
				t.Errorf("end = %s, want %s", result.End, tt.end)
			}
			if result.Count != tt.wantCount {
				t.Errorf("count = %d, want %d", result.Count, tt.wantCount)
			}
			if len(result.Addresses) != int(tt.wantCount) {
				t.Errorf("addresses len = %d, want %d", len(result.Addresses), tt.wantCount)
			}
		})
	}
}

func TestAllocatePinnedRange_MigrationWorkflow(t *testing.T) {
	// Simulate migrating 3 existing TCs with manual LB pools into IPAM
	pool := PoolState{
		AllocatableRanges: []AllocatedRange{{Start: "10.40.6.0", End: "10.40.6.255"}},
	}

	// Import existing assignments
	existingPools := []struct {
		start string
		end   string
	}{
		{"10.40.6.10", "10.40.6.30"},   // butlerlabs-site-dev
		{"10.40.6.50", "10.40.6.55"},   // gateway-test
		{"10.40.6.100", "10.40.6.115"}, // another cluster
	}

	for i, ep := range existingPools {
		result, err := AllocatePinnedRange(pool, ep.start, ep.end)
		if err != nil {
			t.Fatalf("migration %d failed: %v", i, err)
		}
		pool.ExistingAllocs = append(pool.ExistingAllocs, AllocatedRange{
			Start: result.Start, End: result.End,
		})
	}

	// Verify new allocation doesn't conflict with migrated ranges
	newResult, err := AllocateRange(pool, 8)
	if err != nil {
		t.Fatalf("new allocation after migration failed: %v", err)
	}

	// Verify no overlap with any migrated range
	newStart := IPToUint32(net.ParseIP(newResult.Start).To4())
	newEnd := IPToUint32(net.ParseIP(newResult.End).To4())
	for _, ep := range existingPools {
		epStart := IPToUint32(net.ParseIP(ep.start).To4())
		epEnd := IPToUint32(net.ParseIP(ep.end).To4())
		if newStart <= epEnd && newEnd >= epStart {
			t.Errorf("new allocation %s-%s overlaps with migrated range %s-%s",
				newResult.Start, newResult.End, ep.start, ep.end)
		}
	}
}

func TestAllocateRange_RealisticDatacenter(t *testing.T) {
	// Simulate a realistic datacenter scenario:
	// /24 pool (256 IPs), reserved .0-.15 for management, 5 nodes + 8 LB per tenant
	pool := PoolState{
		AllocatableRanges: []AllocatedRange{{Start: "10.40.0.0", End: "10.40.0.255"}},
		ReservedCIDRs:     []string{"10.40.0.0/28"}, // .0-.15 reserved for management
	}

	// Allocate for 10 tenants (5 node IPs + 8 LB IPs each = 13 per tenant)
	for i := 0; i < 10; i++ {
		// Node IPs
		nodeResult, err := AllocateRange(pool, 5)
		if err != nil {
			t.Fatalf("tenant %d node allocation failed: %v", i, err)
		}
		pool.ExistingAllocs = append(pool.ExistingAllocs, AllocatedRange{
			Start: nodeResult.Start, End: nodeResult.End,
		})

		// LB IPs
		lbResult, err := AllocateRange(pool, 8)
		if err != nil {
			t.Fatalf("tenant %d LB allocation failed: %v", i, err)
		}
		pool.ExistingAllocs = append(pool.ExistingAllocs, AllocatedRange{
			Start: lbResult.Start, End: lbResult.End,
		})
	}

	// Verify capacity
	total, allocated, available, err := PoolCapacityInfo(pool)
	if err != nil {
		t.Fatal(err)
	}

	expectedAllocated := int32(10 * 13) // 130 IPs allocated
	expectedTotal := int32(240)         // 256 - 16 reserved
	expectedAvailable := expectedTotal - expectedAllocated

	if total != expectedTotal {
		t.Errorf("total = %d, want %d", total, expectedTotal)
	}
	if allocated != expectedAllocated {
		t.Errorf("allocated = %d, want %d", allocated, expectedAllocated)
	}
	if available != expectedAvailable {
		t.Errorf("available = %d, want %d", available, expectedAvailable)
	}
}

// --- Multi-Range Tests ---

func TestBuildBitmap_MultiRange(t *testing.T) {
	t.Run("two ranges with gap", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{
				{Start: "10.0.0.0", End: "10.0.0.7"},
				{Start: "10.0.0.16", End: "10.0.0.23"},
			},
		}
		bitmap, startUint, err := BuildBitmap(pool)
		if err != nil {
			t.Fatal(err)
		}
		// Bitmap spans .0 to .23 = 24 entries
		if len(bitmap) != 24 {
			t.Fatalf("bitmap length = %d, want 24", len(bitmap))
		}
		if startUint != IPToUint32(net.ParseIP("10.0.0.0")) {
			t.Errorf("startUint mismatch")
		}
		// .0-.7 free (range 1), .8-.15 used (gap), .16-.23 free (range 2)
		for i := 0; i < 8; i++ {
			if bitmap[i] {
				t.Errorf("bitmap[%d] should be free (range 1)", i)
			}
		}
		for i := 8; i < 16; i++ {
			if !bitmap[i] {
				t.Errorf("bitmap[%d] should be used (gap)", i)
			}
		}
		for i := 16; i < 24; i++ {
			if bitmap[i] {
				t.Errorf("bitmap[%d] should be free (range 2)", i)
			}
		}
	})

	t.Run("three ranges with reserved in gap", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{
				{Start: "10.0.0.0", End: "10.0.0.7"},
				{Start: "10.0.0.16", End: "10.0.0.23"},
				{Start: "10.0.0.32", End: "10.0.0.39"},
			},
			ReservedCIDRs: []string{"10.0.0.4/30"}, // .4-.7 reserved within range 1
		}
		bitmap, _, err := BuildBitmap(pool)
		if err != nil {
			t.Fatal(err)
		}
		// .4-.7 reserved
		for i := 4; i < 8; i++ {
			if !bitmap[i] {
				t.Errorf("bitmap[%d] should be reserved", i)
			}
		}
		// .8-.15 gap
		for i := 8; i < 16; i++ {
			if !bitmap[i] {
				t.Errorf("bitmap[%d] should be used (gap between range 1 and 2)", i)
			}
		}
		// .16-.23 free (range 2)
		for i := 16; i < 24; i++ {
			if bitmap[i] {
				t.Errorf("bitmap[%d] should be free (range 2)", i)
			}
		}
	})
}

func TestAllocateRange_MultiRange(t *testing.T) {
	t.Run("allocates across two ranges", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{
				{Start: "10.0.0.0", End: "10.0.0.3"},   // 4 IPs
				{Start: "10.0.0.16", End: "10.0.0.23"}, // 8 IPs
			},
		}
		// Request 6 IPs — won't fit in range 1 (4 IPs), must use range 2 (8 IPs)
		result, err := AllocateRange(pool, 6)
		if err != nil {
			t.Fatal(err)
		}
		// Best-fit selects range 2 (8 >= 6, and range 1 is too small)
		if result.Start != "10.0.0.16" {
			t.Errorf("start = %s, want 10.0.0.16", result.Start)
		}
		if result.End != "10.0.0.21" {
			t.Errorf("end = %s, want 10.0.0.21", result.End)
		}
	})

	t.Run("fills first range then second", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{
				{Start: "10.0.0.0", End: "10.0.0.3"},   // 4 IPs
				{Start: "10.0.0.16", End: "10.0.0.23"}, // 8 IPs
			},
		}
		// Request 4 IPs — fits exactly in range 1 (best fit)
		result1, err := AllocateRange(pool, 4)
		if err != nil {
			t.Fatal(err)
		}
		if result1.Start != "10.0.0.0" {
			t.Errorf("first allocation start = %s, want 10.0.0.0", result1.Start)
		}

		// Add to existing allocs, request 4 more — must use range 2
		pool.ExistingAllocs = append(pool.ExistingAllocs, AllocatedRange{
			Start: result1.Start, End: result1.End,
		})
		result2, err := AllocateRange(pool, 4)
		if err != nil {
			t.Fatal(err)
		}
		if result2.Start != "10.0.0.16" {
			t.Errorf("second allocation start = %s, want 10.0.0.16", result2.Start)
		}
	})
}

// TestAllocateRange_FullCIDRFallback verifies that a single range covering the
// full CIDR (the shape produced by buildPoolState when TenantAllocation is nil)
// behaves identically to the legacy single-range path.
func TestAllocateRange_FullCIDRFallback(t *testing.T) {
	// Simulates the controller's buildPoolState fallback: TenantAllocation is nil,
	// so AllocatableRanges gets a single entry spanning the full CIDR.
	pool := PoolState{
		AllocatableRanges: []AllocatedRange{
			{Start: "10.40.0.0", End: "10.40.0.255"},
		},
		ReservedCIDRs: []string{"10.40.0.0/28"}, // .0-.15 reserved
	}

	// Allocate 8 IPs — should skip reserved block and allocate .16-.23
	result, err := AllocateRange(pool, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Start != "10.40.0.16" {
		t.Errorf("start = %s, want 10.40.0.16", result.Start)
	}
	if result.End != "10.40.0.23" {
		t.Errorf("end = %s, want 10.40.0.23", result.End)
	}

	// Capacity should report 240 usable (256 - 16 reserved), 8 allocated, 232 available
	pool.ExistingAllocs = []AllocatedRange{{Start: result.Start, End: result.End}}
	total, allocated, available, err := PoolCapacityInfo(pool)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total != 240 {
		t.Errorf("total = %d, want 240", total)
	}
	if allocated != 8 {
		t.Errorf("allocated = %d, want 8", allocated)
	}
	if available != 232 {
		t.Errorf("available = %d, want 232", available)
	}
}

func TestPoolCapacityInfo_MultiRange(t *testing.T) {
	t.Run("total is sum of range sizes not span", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{
				{Start: "10.0.0.0", End: "10.0.0.7"},   // 8 IPs
				{Start: "10.0.0.32", End: "10.0.0.39"}, // 8 IPs
			},
		}
		total, allocated, available, err := PoolCapacityInfo(pool)
		if err != nil {
			t.Fatal(err)
		}
		// Total should be 16 (8+8), not 40 (span from .0 to .39)
		if total != 16 {
			t.Errorf("total = %d, want 16", total)
		}
		if allocated != 0 {
			t.Errorf("allocated = %d, want 0", allocated)
		}
		if available != 16 {
			t.Errorf("available = %d, want 16", available)
		}
	})

	t.Run("reserved counted only within ranges", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{
				{Start: "10.0.0.0", End: "10.0.0.15"},  // 16 IPs
				{Start: "10.0.0.32", End: "10.0.0.47"}, // 16 IPs
			},
			ReservedCIDRs: []string{
				"10.0.0.0/30",  // .0-.3 reserved within range 1
				"10.0.0.20/30", // .20-.23 reserved in gap (not in any range)
			},
		}
		total, _, available, err := PoolCapacityInfo(pool)
		if err != nil {
			t.Fatal(err)
		}
		// Total = 32 - 4 (reserved in range 1) = 28. Gap reserved doesn't count.
		if total != 28 {
			t.Errorf("total = %d, want 28", total)
		}
		if available != 28 {
			t.Errorf("available = %d, want 28", available)
		}
	})

	t.Run("crop cluster scenario", func(t *testing.T) {
		// Mirrors the actual crop cluster configuration:
		// Range 1: .32-.63 (32 IPs), Range 2: .91.51-.91.191 (141 IPs)
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{
				{Start: "10.92.90.32", End: "10.92.90.63"},  // 32 IPs
				{Start: "10.92.91.51", End: "10.92.91.191"}, // 141 IPs
			},
		}
		total, allocated, available, err := PoolCapacityInfo(pool)
		if err != nil {
			t.Fatal(err)
		}
		if total != 173 {
			t.Errorf("total = %d, want 173", total)
		}
		if allocated != 0 {
			t.Errorf("allocated = %d, want 0", allocated)
		}
		if available != 173 {
			t.Errorf("available = %d, want 173", available)
		}
	})
}

func TestComputeFragmentation_MultiRange(t *testing.T) {
	t.Run("two ranges no allocations", func(t *testing.T) {
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{
				{Start: "10.0.0.0", End: "10.0.0.7"},
				{Start: "10.0.0.16", End: "10.0.0.23"},
			},
		}
		info, err := ComputeFragmentation(pool)
		if err != nil {
			t.Fatal(err)
		}
		// Two separate free blocks (one per range), so fragmentation > 0
		if info.FreeBlockCount != 2 {
			t.Errorf("FreeBlockCount = %d, want 2", info.FreeBlockCount)
		}
		if info.TotalFreeIPs != 16 {
			t.Errorf("TotalFreeIPs = %d, want 16", info.TotalFreeIPs)
		}
	})

	t.Run("crop cluster scenario with partial allocation", func(t *testing.T) {
		// Crop cluster: ranges[0] .32-.63 (32 IPs), ranges[1] .51-.191 (141 IPs)
		// 28 IPs allocated in ranges[0] (.32-.59), 0 in ranges[1]
		pool := PoolState{
			AllocatableRanges: []AllocatedRange{
				{Start: "10.92.90.32", End: "10.92.90.63"},
				{Start: "10.92.91.51", End: "10.92.91.191"},
			},
			ExistingAllocs: []AllocatedRange{
				{Start: "10.92.90.32", End: "10.92.90.59"},
			},
		}
		info, err := ComputeFragmentation(pool)
		if err != nil {
			t.Fatal(err)
		}
		// Free space: 4 IPs in ranges[0] (.60-.63) + 141 IPs in ranges[1] (.51-.191) = 145
		if info.TotalFreeIPs != 145 {
			t.Errorf("TotalFreeIPs = %d, want 145", info.TotalFreeIPs)
		}
		// Two free blocks: tail of ranges[0] and all of ranges[1]
		if info.FreeBlockCount != 2 {
			t.Errorf("FreeBlockCount = %d, want 2", info.FreeBlockCount)
		}
		// Largest free block is ranges[1] = 141 IPs
		if info.LargestFreeBlock != 141 {
			t.Errorf("LargestFreeBlock = %d, want 141", info.LargestFreeBlock)
		}
		// Fragmentation: 1 - (141/145) = ~2.7% — low, which is operationally correct
		if info.FragmentationPercent > 5 {
			t.Errorf("FragmentationPercent = %d, want <= 5 (healthy multi-range pool)", info.FragmentationPercent)
		}
	})
}

// --- ComputePoolSizeIPs ---

func TestComputePoolSizeIPs(t *testing.T) {
	tests := []struct {
		cidr string
		want int32
	}{
		{"10.0.0.0/24", 256},
		{"10.0.0.0/23", 512},
		{"10.0.0.0/32", 1},
		{"10.0.0.0/16", 65536},
	}
	for _, tt := range tests {
		t.Run(tt.cidr, func(t *testing.T) {
			got, err := ComputePoolSizeIPs(tt.cidr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("ComputePoolSizeIPs(%s) = %d, want %d", tt.cidr, got, tt.want)
			}
		})
	}
}

func TestComputePoolSizeIPs_InvalidCIDR(t *testing.T) {
	_, err := ComputePoolSizeIPs("not-a-cidr")
	if err == nil {
		t.Fatal("expected error for invalid CIDR")
	}
}

// --- ComputeReservedIPs ---

func TestComputeReservedIPs(t *testing.T) {
	tests := []struct {
		name     string
		reserved []string
		want     int32
	}{
		{"empty", nil, 0},
		{"single /32", []string{"10.0.0.1/32"}, 1},
		{"single /29", []string{"10.0.0.8/29"}, 8},
		{"multiple non-overlapping", []string{"10.0.0.7/32", "10.0.0.8/29", "10.0.0.16/28"}, 25},
		{"overlapping ranges", []string{"10.0.0.0/28", "10.0.0.8/29"}, 16}, // .8-.15 is within .0-.15
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ComputeReservedIPs(tt.reserved)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("ComputeReservedIPs = %d, want %d", got, tt.want)
			}
		})
	}
}

// --- ComputeUnmanagedRanges ---

func TestComputeUnmanagedRanges(t *testing.T) {
	t.Run("crop mgmt pool", func(t *testing.T) {
		// Reproduces the real crop mgmt pool layout: 10.92.90.0/23
		cidr := "10.92.90.0/23"
		reserved := []string{
			"10.92.90.7/32",  // .7
			"10.92.90.8/29",  // .8-.15
			"10.92.90.16/28", // .16-.31
			"10.92.91.192/26", // .192-.255
		}
		tenantRanges := []AllocatedRange{
			{Start: "10.92.90.32", End: "10.92.90.63"},
			{Start: "10.92.91.51", End: "10.92.91.191"},
		}

		ranges, total, err := ComputeUnmanagedRanges(cidr, reserved, tenantRanges)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Expected unmanaged ranges:
		// .1-.6 (6 IPs) -- .0 is gateway/network, .7 is reserved
		// .64-10.92.91.50 (243 IPs) -- between first tenant range end and second tenant range start
		if len(ranges) != 2 {
			t.Fatalf("expected 2 unmanaged ranges, got %d: %+v", len(ranges), ranges)
		}

		if ranges[0].Start != "10.92.90.1" || ranges[0].End != "10.92.90.6" {
			t.Errorf("range[0] = %s-%s, want 10.92.90.1-10.92.90.6", ranges[0].Start, ranges[0].End)
		}
		if ranges[1].Start != "10.92.90.64" || ranges[1].End != "10.92.91.50" {
			t.Errorf("range[1] = %s-%s, want 10.92.90.64-10.92.91.50", ranges[1].Start, ranges[1].End)
		}

		// Total: 6 + 243 = 249
		if total != 249 {
			t.Errorf("total unmanaged = %d, want 249", total)
		}
	})

	t.Run("no gaps", func(t *testing.T) {
		// Pool where reserved + tenant covers everything except gateway/broadcast
		cidr := "10.0.0.0/30" // 4 IPs: .0 (gateway), .1, .2, .3 (broadcast)
		reserved := []string{"10.0.0.1/32"}
		tenantRanges := []AllocatedRange{{Start: "10.0.0.2", End: "10.0.0.2"}}

		ranges, total, err := ComputeUnmanagedRanges(cidr, reserved, tenantRanges)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(ranges) != 0 {
			t.Errorf("expected 0 unmanaged ranges, got %d: %+v", len(ranges), ranges)
		}
		if total != 0 {
			t.Errorf("total = %d, want 0", total)
		}
	})

	t.Run("no reserved no tenant", func(t *testing.T) {
		// Everything except gateway and broadcast is unmanaged.
		cidr := "10.0.0.0/29" // 8 IPs: .0-.7
		ranges, total, err := ComputeUnmanagedRanges(cidr, nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// .0 is gateway, .7 is broadcast, .1-.6 is unmanaged
		if len(ranges) != 1 {
			t.Fatalf("expected 1 unmanaged range, got %d: %+v", len(ranges), ranges)
		}
		if ranges[0].Start != "10.0.0.1" || ranges[0].End != "10.0.0.6" {
			t.Errorf("range = %s-%s, want 10.0.0.1-10.0.0.6", ranges[0].Start, ranges[0].End)
		}
		if total != 6 {
			t.Errorf("total = %d, want 6", total)
		}
	})
}
