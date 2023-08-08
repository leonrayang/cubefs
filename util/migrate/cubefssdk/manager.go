package cubefssdk

import (
	"fmt"
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

	manager.sdkCacheLk.Lock()
	defer manager.sdkCacheLk.Unlock()

	if sdk, ok := manager.sdkCache[generateCubeFSSdkKey(volName, endpoint)]; ok {
		return sdk, nil
	}

	sdk, err = newCubeFSSdk(volName, endpoint, true, manager.logger)
	if err != nil {
		return nil, err
	}
	manager.sdkCache[generateCubeFSSdkKey(volName, endpoint)] = sdk
	return sdk, nil
}

func generateCubeFSSdkKey(volName, endpoint string) string {
	return fmt.Sprintf("%v_%v", endpoint, volName)
}
