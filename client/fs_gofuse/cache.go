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

package main

import (
	"container/list"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	// MinInodeCacheEvictNum is used in the foreground eviction.
	// When clearing the inodes from the cache, it stops as soon as 10 inodes have been evicted.
	MinInodeCacheEvictNum = 10
	// MaxInodeCacheEvictNum is used in the back ground. We can evict 200000 inodes at max.
	MaxInodeCacheEvictNum = 200000

	// MinDentryCacheEvictNum is used in the foreground eviction.
	// When clearing the inodes from the cache, it stops as soon as 10 inodes have been evicted.
	MinDentryCacheEvictNum = 10
	// MaxDentryCacheEvictNum is used in the back ground. We can evict 200000 inodes at max.
	MaxDentryCacheEvictNum = 200000

	BgEvictionInterval = 2 * time.Minute

	// Cache configuration constants
	DefaultInodeExpiration = 120 * time.Second
	MaxInodeCache          = 10000000 // in terms of the number of items
	DefaultMaxInodeCache   = 2000000

	// the expiration duration of the dentry in the cache (used internally)
	DentryValidDuration = 5 * time.Second
	DefaultReaddirLimit = 1024

	// The following two are used in the FUSE cache
	// every time the lookup will be performed on the fly, and the result will not be cached
	LookupValidDuration = 5 * time.Second
	// the expiration duration of the attributes in the FUSE cache
	AttrValidDuration = 30 * time.Second

	DisableMetaCache = true
)

// InodeCache defines the structure of the inode cache.
type InodeCache struct {
	sync.RWMutex
	cache       map[uint64]*list.Element
	lruList     *list.List
	expiration  time.Duration
	maxElements int
}

// NewInodeCache returns a new inode cache.
func NewInodeCache(exp time.Duration, maxElements int) *InodeCache {
	ic := &InodeCache{
		cache:       make(map[uint64]*list.Element),
		lruList:     list.New(),
		expiration:  exp,
		maxElements: maxElements,
	}
	go ic.backgroundEviction()
	return ic
}

// Put puts the given inode info into the inode cache.
func (ic *InodeCache) Put(info *proto.InodeInfo) {
	ic.Lock()
	old, ok := ic.cache[info.Inode]
	if ok {
		ic.lruList.Remove(old)
		delete(ic.cache, info.Inode)
	}

	if ic.lruList.Len() >= ic.maxElements {
		ic.evict(true)
	}

	inodeSetExpiration(info, ic.expiration)
	element := ic.lruList.PushFront(info)
	ic.cache[info.Inode] = element
	ic.Unlock()
	log.LogDebugf("InodeCache put inode: inode(%v) expire(%v)",
		info.Inode, info.Expiration())
}

// Get returns the inode info based on the given inode number.
func (ic *InodeCache) Get(ino uint64) *proto.InodeInfo {
	ic.RLock()
	element, ok := ic.cache[ino]
	if !ok {
		ic.RUnlock()
		log.LogDebugf("Inode Cache not found %v", ino)
		return nil
	}

	info := element.Value.(*proto.InodeInfo)
	if inodeExpired(info) && DisableMetaCache {
		ic.RUnlock()
		log.LogDebugf("Inode Cache %v expired", ino)
		return nil
	}
	ic.RUnlock()

	if info != nil {
		log.LogDebugf("Inode Cache found ino(%v) storageClass(%v)",
			ino, info.StorageClass)
	}
	return info
}

// Delete deletes the inode info based on the given inode number.
func (ic *InodeCache) Delete(ino uint64) {
	log.LogDebugf("InodeCache Delete: ino(%v)", ino)
	ic.Lock()
	element, ok := ic.cache[ino]
	if ok {
		ic.lruList.Remove(element)
		delete(ic.cache, ino)
	}
	ic.Unlock()
}

// evict evicts the inode cache.
func (ic *InodeCache) evict(foreground bool) {
	if foreground {
		for i := 0; i < MinInodeCacheEvictNum; i++ {
			element := ic.lruList.Back()
			if element == nil {
				break
			}
			info := element.Value.(*proto.InodeInfo)
			log.LogDebugf("InodeCache check inode(%v)", info.Inode)
			if inodeExpired(info) {
				log.LogDebugf("InodeCache check inode(%v) expired(%v)",
					info.Inode, info.Expiration())
				ic.lruList.Remove(element)
				delete(ic.cache, info.Inode)
				log.LogDebugf("InodeCache remove inode(%v)", info.Inode)
			} else {
				break
			}
		}
	} else {
		for i := 0; i < MaxInodeCacheEvictNum; i++ {
			element := ic.lruList.Back()
			if element == nil {
				break
			}
			info := element.Value.(*proto.InodeInfo)
			if inodeExpired(info) {
				log.LogDebugf("InodeCache check inode(%v) expired(%v)",
					info.Inode, info.Expiration())
				ic.lruList.Remove(element)
				delete(ic.cache, info.Inode)
				log.LogDebugf("InodeCache remove inode(%v)", info.Inode)
			} else {
				break
			}
		}
	}
}

// backgroundEviction performs background eviction.
func (ic *InodeCache) backgroundEviction() {
	ticker := time.NewTicker(BgEvictionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			start := time.Now()
			ic.evict(false)
			elapsed := time.Since(start)
			if ic.lruList.Len() > 0 {
				log.LogInfof("InodeCache: start BG evict")
			} else {
				log.LogInfof("InodeCache: no need to do BG evict")
			}
			log.LogInfof("InodeCache: total inode cache(%d), cost(%d)ns", ic.lruList.Len(), elapsed.Nanoseconds())
		}
	}
}

// Dcache defines the structure of the dentry cache.
type Dcache struct {
	sync.RWMutex
	cache       map[string]*list.Element
	lruList     *list.List
	expiration  time.Duration
	maxElements int
}

// NewDcache returns a new dentry cache.
func NewDcache(exp time.Duration, maxElements int) *Dcache {
	dc := &Dcache{
		cache:       make(map[string]*list.Element),
		lruList:     list.New(),
		expiration:  exp,
		maxElements: maxElements,
	}
	go dc.backgroundEviction()
	return dc
}

// Put puts the given dentry info into the dentry cache.
func (dc *Dcache) Put(info *proto.DentryInfo) {
	dc.Lock()
	old, ok := dc.cache[info.Name]
	if ok {
		dc.lruList.Remove(old)
		delete(dc.cache, info.Name)
	}

	if dc.lruList.Len() >= dc.maxElements {
		dc.evict(true)
	}

	dentrySetExpiration(info, dc.expiration)
	element := dc.lruList.PushFront(info)
	dc.cache[info.Name] = element
	dc.Unlock()
	// log.LogDebugf("Dcache put inode: inode(%v)", info.Inode)
}

// Get returns the dentry info based on the given name.
func (dc *Dcache) Get(name string) *proto.DentryInfo {
	dc.RLock()
	element, ok := dc.cache[name]
	if !ok {
		dc.RUnlock()
		return nil
	}

	info := element.Value.(*proto.DentryInfo)
	if dentryExpired(info) && DisableMetaCache {
		dc.RUnlock()
		// log.LogDebugf("Dcache GetConnect expired: now(%v) inode(%v), expired(%d)", time.Now().Format(LogTimeFormat), info.Inode, info.Expiration())
		return nil
	}
	dc.RUnlock()
	return info
}

// Delete deletes the dentry info based on the given name.
func (dc *Dcache) Delete(name string) {
	// log.LogDebugf("Dcache Delete: ino(%v)", ino)
	dc.Lock()
	element, ok := dc.cache[name]
	if ok {
		dc.lruList.Remove(element)
		delete(dc.cache, name)
	}
	dc.Unlock()
}

// evict evicts the dentry cache.
func (dc *Dcache) evict(foreground bool) {
	if foreground {
		for i := 0; i < MinDentryCacheEvictNum; i++ {
			element := dc.lruList.Back()
			if element == nil {
				break
			}
			info := element.Value.(*proto.DentryInfo)
			if dentryExpired(info) {
				dc.lruList.Remove(element)
				delete(dc.cache, info.Name)
			} else {
				break
			}
		}
	} else {
		for i := 0; i < MaxDentryCacheEvictNum; i++ {
			element := dc.lruList.Back()
			if element == nil {
				break
			}
			info := element.Value.(*proto.DentryInfo)
			if dentryExpired(info) {
				dc.lruList.Remove(element)
				delete(dc.cache, info.Name)
			} else {
				break
			}
		}
	}
}

// backgroundEviction performs background eviction.
func (dc *Dcache) backgroundEviction() {
	ticker := time.NewTicker(BgEvictionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			start := time.Now()
			dc.evict(false)
			elapsed := time.Since(start)
			if dc.lruList.Len() > 0 {
				log.LogInfof("Dcache: start BG evict")
			} else {
				log.LogInfof("Dcache: no need to do BG evict")
			}
			log.LogInfof("Dcache: total dentry cache(%d), cost(%d)ns", dc.lruList.Len(), elapsed.Nanoseconds())
		}
	}
}

// CacheManager manages all caches for the client
type CacheManager struct {
	ic          *InodeCache
	dc          *Dcache
	nodeCache   map[uint64]interface{}
	nodeCacheMu sync.RWMutex
}

// NewCacheManager creates a new cache manager
func NewCacheManager(inodeExpiration time.Duration, maxInodeCache int) *CacheManager {
	return &CacheManager{
		ic:        NewInodeCache(inodeExpiration, maxInodeCache),
		dc:        NewDcache(inodeExpiration, maxInodeCache),
		nodeCache: make(map[uint64]interface{}),
	}
}

// GetInodeCache returns the inode cache
func (cm *CacheManager) GetInodeCache() *InodeCache {
	return cm.ic
}

// GetDentryCache returns the dentry cache
func (cm *CacheManager) GetDentryCache() DentryCacheInterface {
	return cm.dc
}

// GetNodeCache returns the node cache
func (cm *CacheManager) GetNodeCache() map[uint64]interface{} {
	return cm.nodeCache
}

// GetNodeCacheMu returns the node cache mutex
func (cm *CacheManager) GetNodeCacheMu() *sync.RWMutex {
	return &cm.nodeCacheMu
}

// PutNode puts a node into the node cache
func (cm *CacheManager) PutNode(ino uint64, node interface{}) {
	cm.nodeCacheMu.Lock()
	defer cm.nodeCacheMu.Unlock()
	cm.nodeCache[ino] = node
}

// GetNode gets a node from the node cache
func (cm *CacheManager) GetNode(ino uint64) (interface{}, bool) {
	cm.nodeCacheMu.RLock()
	defer cm.nodeCacheMu.RUnlock()
	node, ok := cm.nodeCache[ino]
	return node, ok
}

// DeleteNode deletes a node from the node cache
func (cm *CacheManager) DeleteNode(ino uint64) {
	cm.nodeCacheMu.Lock()
	defer cm.nodeCacheMu.Unlock()
	delete(cm.nodeCache, ino)
}

// Helper functions for cache expiration
func inodeExpired(info *proto.InodeInfo) bool {
	return time.Now().UnixNano() > info.Expiration()
}

func inodeSetExpiration(info *proto.InodeInfo, t time.Duration) {
	info.SetExpiration(time.Now().Add(t).UnixNano())
}

func dentryExpired(info *proto.DentryInfo) bool {
	return time.Now().UnixNano() > info.Expiration()
}

func dentrySetExpiration(info *proto.DentryInfo, t time.Duration) {
	info.SetExpiration(time.Now().Add(t).UnixNano())
}
