package cubefssdk

import (
	"container/list"
	"github.com/cubefs/cubefs/util/log"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
)

const (
	// MinInodeCacheEvictNum is used in the foreground eviction.
	// When clearing the inodes from the cache, it stops as soon as 10 inodes have been evicted.
	MinInodeCacheEvictNum = 10
	// MaxInodeCacheEvictNum is used in the back ground. We can evict 200000 inodes at max.
	MaxInodeCacheEvictNum = 200000

	BgEvictionInterval = 2 * time.Minute

	DefaultInodeExpiration = 120 * time.Second
	DefaultMaxInodeCache   = 10000000
)

// InodeCache defines the structure of the inode cache.
type InodeCache struct {
	sync.RWMutex
	cache       map[string]*list.Element
	lruList     *list.List
	expiration  time.Duration
	maxElements int
	logger      *zap.Logger
}

type PathInodeCache struct {
	infoCache *proto.InodeInfo
	filePath  string
}

// NewInodeCache returns a new inode cache.
func NewInodeCache(logger *zap.Logger) *InodeCache {
	ic := &InodeCache{
		cache:       make(map[string]*list.Element),
		lruList:     list.New(),
		expiration:  DefaultInodeExpiration,
		maxElements: DefaultMaxInodeCache,
		logger:      logger,
	}
	go ic.backgroundEviction()
	return ic
}

// Put puts the given inode info into the inode cache.
func (ic *InodeCache) Put(filePath string, info *proto.InodeInfo) {
	ic.Lock()
	old, ok := ic.cache[filePath]
	if ok {
		ic.lruList.Remove(old)
		delete(ic.cache, filePath)
	}

	if ic.lruList.Len() >= ic.maxElements {
		ic.evict(true)
	}

	inodeSetExpiration(info, ic.expiration)
	cache := &PathInodeCache{infoCache: info, filePath: filePath}
	element := ic.lruList.PushFront(cache)
	ic.cache[filePath] = element
	ic.Unlock()
	// log.LogDebugf("InodeCache put inode: inode(%v)", info.Inode)
}

// Get returns the inode info based on the given inode number.
func (ic *InodeCache) Get(filePath string) *proto.InodeInfo {
	ic.RLock()
	element, ok := ic.cache[filePath]
	if !ok {
		ic.RUnlock()
		return nil
	}

	cache := element.Value.(*PathInodeCache)
	if inodeExpired(cache.infoCache) {
		ic.RUnlock()
		// log.LogDebugf("InodeCache GetConnect expired: now(%v) inode(%v), expired(%d)", time.Now().Format(LogTimeFormat), info.Inode, info.Expiration())
		return nil
	}
	ic.RUnlock()
	return cache.infoCache
}

// Delete deletes the inode info based on the given inode number.
func (ic *InodeCache) Delete(filePath string) {
	// log.LogDebugf("InodeCache Delete: ino(%v)", ino)
	ic.Lock()
	element, ok := ic.cache[filePath]
	if ok {
		ic.lruList.Remove(element)
		delete(ic.cache, filePath)
	}
	ic.Unlock()
}

// Foreground eviction cares more about the speed.
// Background eviction evicts all expired items from the cache.
// The caller should grab the WRITE lock of the inode cache.
func (ic *InodeCache) evict(foreground bool) {
	var count int

	for i := 0; i < MinInodeCacheEvictNum; i++ {
		element := ic.lruList.Back()
		if element == nil {
			return
		}

		// For background eviction, if all expired items have been evicted, just return
		// But for foreground eviction, we need to evict at least MinInodeCacheEvictNum inodes.
		// The foreground eviction, does not need to care if the inode has expired or not.
		cache := element.Value.(*PathInodeCache)
		if !foreground && !inodeExpired(cache.infoCache) {
			return
		}

		// log.LogDebugf("InodeCache GetConnect expired: now(%v) inode(%v)", time.Now().Format(LogTimeFormat), info.Inode)
		ic.lruList.Remove(element)
		delete(ic.cache, cache.filePath)
		count++
	}

	// For background eviction, we need to continue evict all expired items from the cache
	if foreground {
		return
	}

	for i := 0; i < MaxInodeCacheEvictNum; i++ {
		element := ic.lruList.Back()
		if element == nil {
			break
		}
		cache := element.Value.(*PathInodeCache)
		if !inodeExpired(cache.infoCache) {
			break
		}
		// log.LogDebugf("InodeCache GetConnect expired: now(%v) inode(%v)", time.Now().Format(LogTimeFormat), info.Inode)
		ic.lruList.Remove(element)
		delete(ic.cache, cache.filePath)
		count++
	}
}

func (ic *InodeCache) backgroundEviction() {
	t := time.NewTicker(BgEvictionInterval)
	defer t.Stop()

	for range t.C {
		start := time.Now()
		ic.Lock()
		ic.evict(false)
		ic.Unlock()
		elapsed := time.Since(start)
		log.LogInfof("InodeCache: total inode cache(%d), cost(%d)ns", ic.lruList.Len(), elapsed.Nanoseconds())
	}
}

func inodeExpired(info *proto.InodeInfo) bool {
	return time.Now().UnixNano() > info.Expiration()
}

func inodeSetExpiration(info *proto.InodeInfo, t time.Duration) {
	info.SetExpiration(time.Now().Add(t).UnixNano())
}
