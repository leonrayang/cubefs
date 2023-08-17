package cubefssdk

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"go.uber.org/zap"
	"os"
	gopath "path"
	"syscall"
)

var masterMap map[string][]string = map[string][]string{
	"aiv":        {"10.90.73.53:17010", "10.90.73.54:17010", "10.90.73.55:17010"},
	"ml-aiv":     {"10.81.71.143:17010", "10.84.104.118:17010", "10.90.231.19:17010"},
	"ml-mariana": {"10.81.71.143:17010", "10.84.104.118:17010", "10.90.231.19:17010"},
	"bhw":        {"10.236.0.150:17010", "10.236.0.151:17010", "10.236.0.152:17010 "},
}

type MetaApi struct {
	mw *meta.MetaWrapper
}

func NewMetaApi(volName, endpoint string, enableSummary bool, logger *zap.Logger) (*MetaApi, error) {
	var (
		mw      *meta.MetaWrapper
		err     error
		masters []string
	)
	logger.Info("NewMetaApi", zap.Any("volName", volName), zap.Any("endpoint", endpoint))
	masters = append(masters, endpoint)
	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        volName,
		Masters:       masters,
		ValidateOwner: false,
		EnableSummary: enableSummary,
	}); err != nil {
		logger.Error("newClient NewMetaWrapper failed", zap.Any("volName", volName),
			zap.Any("masters", masters), zap.Any("err", err))
		return nil, err
	}
	metaAPI := &MetaApi{
		mw: mw,
	}
	return metaAPI, nil
}

// 可以利用缓存提速
func (sdk *CubeFSSdk) PathIsExist(path string) bool {
	_, err := sdk.mwApi.mw.LookupPath(path)
	if err != nil {
		if err != syscall.EEXIST {
			return false
		} else {
			return true
		}
	}
	return true
}

func (sdk *CubeFSSdk) PathIsExistWithIno(parentID uint64, filePath string) (uint64, bool) {
	ino, _, err := sdk.mwApi.mw.Lookup_ll(parentID, gopath.Base(filePath))
	if err != nil {
		if err != syscall.EEXIST {
			return 0, false
		} else {
			return 0, true
		}
	}
	return ino, true
}

func (sdk *CubeFSSdk) DeleteFile(parentID uint64, fileName string) error {
	_, err := sdk.mwApi.mw.Delete_ll(parentID, fileName, false)
	return err
}

func (sdk *CubeFSSdk) LookupPath(path string) (*proto.InodeInfo, error) {
	ino, err := sdk.mwApi.mw.LookupPath(path)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("lookupPath path %v from vol[%v] failed:%v", path, sdk.volName, err.Error()))
	}

	info, err := sdk.mwApi.mw.InodeGet_ll(ino)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("InodeGet_ll path %v fromvol[%v] failed:%v", path, sdk.volName, err.Error()))
	}

	return info, nil
}

func (sdk *CubeFSSdk) getInodeInfo(ino uint64) (*proto.InodeInfo, error) {
	info, err := sdk.mwApi.mw.InodeGet_ll(ino)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("InodeGet_ll get %v fromvol[%v] failed:%v", ino, sdk.volName, err.Error()))
	}

	return info, nil
}

func (sdk *CubeFSSdk) LookupFileWithParentCache(filePath string) (info *proto.InodeInfo, err error) {
	parentDir := gopath.Dir(filePath)
	parentInfo, err := sdk.LookupPathWithCache(parentDir)
	if err != nil {
		return nil, err
	}
	//查找子项
	ino, _, err := sdk.mwApi.mw.Lookup_ll(parentInfo.Inode, gopath.Base(filePath))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("lookupPath path %v from vol[%v] failed:%v", filePath, sdk.volName, err.Error()))
	}
	info, err = sdk.mwApi.mw.InodeGet_ll(ino)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("InodeGet_ll path %v fromvol[%v] failed:%v", filePath, sdk.volName, err.Error()))
	}
	return info, nil
}

func (sdk *CubeFSSdk) LookupPathWithCache(filePath string) (info *proto.InodeInfo, err error) {
	//可能目录的全路径有缓存
	info = sdk.ic.Get(filePath)
	if info != nil {
		return info, nil
	}
	var ino uint64
	//可能文件路径的父目录有缓存,先查找ino号
	parentInfo := sdk.ic.Get(gopath.Dir(filePath))
	if parentInfo != nil {
		ino, _, err = sdk.mwApi.mw.Lookup_ll(parentInfo.Inode, gopath.Base(filePath))
		if err != nil {
			return nil, errors.New(fmt.Sprintf("lookupPath path %v from vol[%v] failed:%v", filePath, sdk.volName, err.Error()))
		}
	} else {
		ino, err = sdk.mwApi.mw.LookupPath(filePath)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("lookupPath path %v from vol[%v] failed:%v", filePath, sdk.volName, err.Error()))
		}
	}
	//通过ino号查找Inode信息
	info, err = sdk.mwApi.mw.InodeGet_ll(ino)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("InodeGet_ll path %v fromvol[%v] failed:%v", filePath, sdk.volName, err.Error()))
	}
	sdk.ic.Put(filePath, info)
	return info, nil
}

func (sdk *CubeFSSdk) Move(srcPath, dstPath string) error {
	srcPathInfo, err := sdk.LookupPath(srcPath)
	logger := sdk.logger
	if err != nil {
		logger.Error("lookupPath src failed", zap.Any("srcPath", srcPath), zap.Any("err", err))
		return err
	}
	dstPathInfo, err := sdk.LookupPath(dstPath)
	if err != nil {
		logger.Error("lookupPath dst failed", zap.Any("dstPath", dstPath), zap.Any("err", err))
		return err
	}
	srcDirPath, srcName := gopath.Split(srcPath)
	srcDirInfo, err := sdk.LookupPath(srcDirPath)
	if err != nil {
		logger.Error("lookupPath src failed", zap.Any("srcDirPath", srcDirPath), zap.Any("err", err))
		return err
	}
	//如果是文件移动到目录下 或者文件夹移动到文件夹下
	if (proto.IsRegular(srcPathInfo.Mode) && proto.IsDir(dstPathInfo.Mode)) ||
		(proto.IsDir(srcPathInfo.Mode) && proto.IsDir(dstPathInfo.Mode)) {
		err = sdk.mwApi.mw.Rename_ll(srcDirInfo.Inode, srcName, dstPathInfo.Inode, srcName, true)
		return err
	}
	return errors.New(fmt.Sprintf("Unsupport move type src %v, dst %v", srcPathInfo.Mode, dstPathInfo.Mode))
}

func (sdk *CubeFSSdk) IsFileOrSymlink(path string) (bool, error) {
	info, err := sdk.LookupPath(path)
	logger := sdk.logger
	if err != nil {
		logger.Error("lookupPath src failed", zap.Any("srcPath", path), zap.Any("err", err))
		return false, err
	}
	return proto.IsRegular(info.Mode) || proto.IsSymlink(info.Mode), nil
}

func (sdk *CubeFSSdk) IsDir(path string) (bool, error) {
	info, err := sdk.LookupPath(path)
	logger := sdk.logger
	if err != nil {
		logger.Error("lookupPath src failed", zap.Any("srcPath", path), zap.Any("err", err))
		return false, err
	}
	return proto.IsDir(info.Mode), nil
}

func (sdk *CubeFSSdk) IsSymlink(path string) (bool, error) {
	info, err := sdk.LookupPath(path)
	logger := sdk.logger
	if err != nil {
		logger.Error("lookupPath src failed", zap.Any("srcPath", path), zap.Any("err", err))
		return false, err
	}
	return proto.IsSymlink(info.Mode), nil
}

func (sdk *CubeFSSdk) CreateFile(pino uint64, name string, mode, uid, gid uint32) (*proto.InodeInfo, error) {
	fuseMode := mode & 0777
	return sdk.mwApi.mw.Create_ll(pino, name, fuseMode, uid, gid, nil)
}

func (sdk *CubeFSSdk) CreateDirectory(pino uint64, name string, mode, uid, gid uint32) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0777
	fuseMode |= uint32(os.ModeDir)
	return sdk.mwApi.mw.Create_ll(pino, name, fuseMode, uid, gid, nil)
}

func (sdk *CubeFSSdk) CreateSymlink(pino uint64, name string, mode, uid, gid uint32, target []byte) (info *proto.InodeInfo, err error) {
	fuseMode := mode & 0777
	fuseMode |= uint32(os.ModeSymlink)
	return sdk.mwApi.mw.Create_ll(pino, name, fuseMode, uid, gid, target)
}

func (sdk *CubeFSSdk) GetFileSize(path string) (uint64, error) {
	info, err := sdk.LookupPath(path)
	if err != nil {
		sdk.logger.Error("lookupPath src failed", zap.Any("srcPath", path), zap.Any("err", err))
		return 0, err
	}
	return info.Size, nil
}

func (sdk *CubeFSSdk) GetDirSize(path string, goroutineNum int) (uint64, error) {
	info, err := sdk.LookupPath(path)
	if err != nil {
		sdk.logger.Error("lookupPath path failed", zap.Any("path", path), zap.Any("err", err))
		return 0, err
	}
	summaryInfo, err := sdk.mwApi.mw.GetSummary_ll(info.Inode, int32(goroutineNum))
	if err != nil {
		sdk.logger.Error("GetSummary_ll path failed", zap.Any("path", path), zap.Any("err", err))
		return 0, errors.New(fmt.Sprintf("GetSummary_ll path failed: %s[%s]", path, err.Error()))
	}
	sdk.logger.Debug("GetDirSize", zap.Any("path", path), zap.Any("summaryInfo", summaryInfo))
	return uint64(summaryInfo.Fbytes), nil
}

func (sdk *CubeFSSdk) ReadDir(path string) ([]proto.Dentry, error) {
	info, err := sdk.LookupPath(path)
	if err != nil {
		sdk.logger.Error("lookupPath path failed", zap.Any("path", path), zap.Any("err", err))
		return nil, err
	}
	return sdk.mwApi.mw.ReadDir_ll(info.Inode)
}

func (sdk *CubeFSSdk) IsDirByDentry(entry proto.Dentry) bool {
	return proto.IsDir(entry.Type)
}

func (sdk *CubeFSSdk) Truncate(parentIno uint64, ino uint64) error {
	return sdk.ecApi.ec.Truncate(sdk.mwApi.mw, parentIno, ino, 0)
}

//func (sdk *CubeFSSdk) MkDirAll(path string) error {
//	dirPath := path.Clean(path)
//	if dirPath == "/" {
//		return nil
//	}
//	subs := strings.Split(dirPath, "/")
//	for _, sub := range subs {
//
//	}
//}
