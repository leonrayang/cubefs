// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package fs

import (
	"strconv"
	"testing"
	"time"
)

func TestNegativeDentryCache_PutAndGet(t *testing.T) {
	ndc := NewNegativeDentryCache()

	// Test Put and Get
	ndc.Put("testfile.txt")
	if !ndc.Get("testfile.txt") {
		t.Error("Expected cache hit for testfile.txt")
	}

	// Test non-existent entry
	if ndc.Get("nonexistent.txt") {
		t.Error("Expected cache miss for nonexistent.txt")
	}
}

func TestNegativeDentryCache_Delete(t *testing.T) {
	ndc := NewNegativeDentryCache()

	// Put an entry
	ndc.Put("testfile.txt")
	if !ndc.Get("testfile.txt") {
		t.Error("Expected cache hit before delete")
	}

	// Delete the entry
	ndc.Delete("testfile.txt")
	if ndc.Get("testfile.txt") {
		t.Error("Expected cache miss after delete")
	}
}

func TestNegativeDentryCache_Clear(t *testing.T) {
	ndc := NewNegativeDentryCache()

	// Put multiple entries
	ndc.Put("file1.txt")
	ndc.Put("file2.txt")
	ndc.Put("file3.txt")

	// Verify they exist
	if !ndc.Get("file1.txt") || !ndc.Get("file2.txt") || !ndc.Get("file3.txt") {
		t.Error("Expected all entries to be cached")
	}

	// Clear all
	ndc.Clear()

	// Verify all are gone
	if ndc.Get("file1.txt") || ndc.Get("file2.txt") || ndc.Get("file3.txt") {
		t.Error("Expected all entries to be cleared")
	}
}

func TestNegativeDentryCache_Expiration(t *testing.T) {
	ndc := NewNegativeDentryCache()

	// Put an entry
	ndc.Put("testfile.txt")
	if !ndc.Get("testfile.txt") {
		t.Error("Expected cache hit immediately after Put")
	}

	// Wait for cache to expire (NegativeDentryValidDuration is 200ms)
	time.Sleep(NegativeDentryValidDuration + 50*time.Millisecond)

	// Should be expired now
	if ndc.Get("testfile.txt") {
		t.Error("Expected cache miss after expiration")
	}
}

func TestNegativeDentryCache_NilSafety(t *testing.T) {
	var ndc *NegativeDentryCache

	// All operations should handle nil gracefully
	ndc.Put("test.txt")
	if ndc.Get("test.txt") {
		t.Error("Get on nil cache should return false")
	}
	ndc.Delete("test.txt")
	ndc.Clear()
}

func TestNegativeDentryCache_ConcurrentAccess(t *testing.T) {
	ndc := NewNegativeDentryCache()

	// Test concurrent Put operations
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			ndc.Put("file" + strconv.Itoa(id))
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all entries exist
	for i := 0; i < 10; i++ {
		if !ndc.Get("file" + strconv.Itoa(i)) {
			t.Errorf("Expected cache hit for file%d", i)
		}
	}
}
