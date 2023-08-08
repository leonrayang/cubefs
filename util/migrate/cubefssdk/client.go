package cubefssdk

import (
	"go.uber.org/zap"
)

type CubeFSSdk struct {
	mwApi   *MetaApi
	ecApi   *DataApi
	logger  *zap.Logger
	volName string
	//ic      *InodeCache
}

func newCubeFSSdk(volName, endpoint string, enableSummary bool, logger *zap.Logger) (*CubeFSSdk, error) {
	sdk := &CubeFSSdk{
		logger:  logger,
		volName: volName,
		//ic:      NewInodeCache(logger),
	}
	var err error

	if endpoint == "10.236.0.150:17010,10.236.0.151:17010,10.236.0.152:17010" {
		endpoint = "cfs-bhw-starfire-ssd.oppo.local"
	}
	if endpoint == "10.177.27.240:17010,10.177.27.242:17010,10.177.27.241:17010" {
		endpoint = "cfs.dg-test.wanyol.com"
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
