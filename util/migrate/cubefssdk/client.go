package cubefssdk

import (
	"go.uber.org/zap"
	"strings"
)

type CubeFSSdk struct {
	mwApi   *MetaApi
	ecApi   *DataApi
	logger  *zap.Logger
	volName string
	ic      *InodeCache
}

func newCubeFSSdk(volName, endpoint string, enableSummary bool, logger *zap.Logger) (*CubeFSSdk, error) {
	sdk := &CubeFSSdk{
		logger:  logger,
		volName: volName,
		ic:      NewInodeCache(logger),
	}
	var err error
	//log.InitLog("/home/service/var/logs/sdk", "sdk", log.ErrorLevel, nil)
	if endpoint == "10.236.0.150:17010,10.236.0.151:17010,10.236.0.152:17010" {
		endpoint = "cfs-bhw-starfire-ssd.oppo.local"
	}
	if endpoint == "10.177.27.240:17010,10.177.27.242:17010,10.177.27.241:17010" {
		endpoint = "cfs.dg-test.wanyol.com"
	}

	if endpoint == "10.224.52.159:17010,10.48.82.229:17010,10.51.164.13:17010" {
		endpoint = "cfs-bhw-starfire-ssd2.oppo.local"
	}

	if strings.Contains(endpoint, ",") {
		endpoint = strings.Split(endpoint, ",")[0]
	}
	sdk.mwApi, err = NewMetaApi(volName, endpoint, enableSummary, logger)
	if err != nil {
		return nil, err
	}
	sdk.ecApi, err = NewDataApi(volName, endpoint, sdk.mwApi.mw, logger)
	if err != nil {
		return nil, err
	}
	return sdk, nil
}
