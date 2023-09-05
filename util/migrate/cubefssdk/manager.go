package cubefssdk

import (
	"fmt"
	"github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
	"sync"
)

type SdkManager struct {
	sdkCacheLk sync.RWMutex
	sdkCache   map[string]*CubeFSSdk
	logger     *zap.Logger
}

func NewCubeFSSdkManager(logger *zap.Logger) *SdkManager {
	return &SdkManager{
		logger:   logger,
		sdkCache: make(map[string]*CubeFSSdk),
	}
}

func (manager *SdkManager) GetCubeFSSdk(volName, endpoint string) (sdk *CubeFSSdk, err error) {
	manager.sdkCacheLk.RLock()
	if sdk, ok := manager.sdkCache[generateCubeFSSdkKey(volName, endpoint)]; ok {
		manager.sdkCacheLk.RUnlock()
		return sdk, nil
	}
	manager.sdkCacheLk.RUnlock()

	sdk, err = newCubeFSSdk(volName, endpoint, false, manager.logger)
	if err != nil {
		return nil, err
	}

	manager.sdkCacheLk.Lock()
	manager.sdkCache[generateCubeFSSdkKey(volName, endpoint)] = sdk
	manager.sdkCacheLk.Unlock()
	return sdk, nil
}

func (manager *SdkManager) GetStreamerLen() (infos []proto.StreamerInfo, total int) {
	manager.sdkCacheLk.RLock()
	defer manager.sdkCacheLk.RUnlock()

	for key, sdk := range manager.sdkCache {
		array, cnt := sdk.GetStreamerLen()
		total += cnt
		info := proto.StreamerInfo{Info: key, Count: cnt, Inodes: array}
		infos = append(infos, info)
	}
	return
}

func generateCubeFSSdkKey(volName, endpoint string) string {
	return fmt.Sprintf("%v_%v", endpoint, volName)
}
