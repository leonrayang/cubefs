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
	manager.sdkCacheLk.Lock()
	defer manager.sdkCacheLk.Unlock()
	key := generateCubeFSSdkKey(volName, endpoint)
	if sdk, ok := manager.sdkCache[key]; ok {
		return sdk, nil
	}
	sdk, err = newCubeFSSdk(volName, endpoint, true, manager.logger)
	if err != nil {
		return nil, err
	}
	manager.sdkCache[key] = sdk
	return sdk, nil
}

func generateCubeFSSdkKey(volName, endpoint string) string {
	return fmt.Sprintf("%v_%v", endpoint, volName)
}
