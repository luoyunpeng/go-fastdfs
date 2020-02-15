package model

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	mapSet "github.com/deckarep/golang-set"
	"github.com/gin-gonic/gin"
	"github.com/luoyunpeng/go-fastdfs/internal/config"
	"github.com/luoyunpeng/go-fastdfs/pkg"
	log "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	levelDBUtil "github.com/syndtr/goleveldb/leveldb/util"
)

type FileInfo struct {
	Name      string   `json:"name"`
	ReName    string   `json:"rename"`
	Path      string   `json:"path"`
	Md5       string   `json:"md5"`
	Size      int64    `json:"size"`
	Peers     []string `json:"peers"`
	Scene     string   `json:"scene"`
	TimeStamp int64    `json:"timeStamp"`
	OffSet    int64    `json:"offset"`
	Retry     int
	Op        string
}

func LoadFileInfoByDate(date string, filename string, levelDB *leveldb.DB) (mapSet.Set, error) {
	var fileInfos mapSet.Set
	fileInfos = mapSet.NewSet()
	keyPrefix := "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, filename)

	iter := levelDB.NewIterator(levelDBUtil.BytesPrefix([]byte(keyPrefix)), nil)
	for iter.Next() {
		var fileInfo FileInfo
		if err := config.Json.Unmarshal(iter.Value(), &fileInfo); err != nil {
			continue
		}
		fileInfos.Add(&fileInfo)
	}
	iter.Release()

	return fileInfos, nil
}

// Read:
func RepairFileInfoFromFile(conf *config.Config) {
	var (
		pathPrefix string
		err        error
		fi         os.FileInfo
	)

	if conf.LockMap().IsLock("RepairFileInfoFromFile") {
		log.Warn("Lock RepairFileInfoFromFile")
		return
	}

	conf.LockMap().LockKey("RepairFileInfoFromFile")
	defer conf.LockMap().UnLockKey("RepairFileInfoFromFile")

	handleFunc := func(filePath string, f os.FileInfo, err error) error {
		var (
			files    []os.FileInfo
			fi       os.FileInfo
			fileInfo FileInfo
			pathMd5  string
		)
		if f.IsDir() {
			files, err = ioutil.ReadDir(filePath)

			if err != nil {
				log.Error(err)
				return err
			}
			for _, fi = range files {
				if fi.IsDir() || fi.Size() == 0 {
					continue
				}

				filePath = strings.Replace(filePath, "\\", "/", -1)

				if pathPrefix != "" {
					filePath = strings.Replace(filePath, pathPrefix, conf.StoreDir(), 1)
				}
				if strings.HasPrefix(filePath, conf.StoreDir()+"/"+conf.LargeDir()) {
					log.Info(fmt.Sprintf("ignore small file file %s", filePath+"/"+fi.Name()))
					continue
				}

				pathMd5 = pkg.MD5(filePath + "/" + fi.Name())
				//if finfo, _ := conf.GetFileInfoFromLevelDB(pathMd5); finfo != nil && finfo.Md5 != "" {
				//	log.Info(fmt.Sprintf("exist ignore file %s", file_path+"/"+fi.Name()))
				//	continue
				//}
				//sum, err = util.GetFileSumByName(file_path+"/"+fi.Name(), config.CommonConfig.FileSumArithmetic)

				fileInfo = FileInfo{
					Size:      fi.Size(),
					Name:      fi.Name(),
					Path:      filePath,
					Md5:       pathMd5,
					TimeStamp: fi.ModTime().Unix(),
					Peers:     []string{conf.Addr()},
					OffSet:    -2,
				}

				log.Info(filePath, "/", fi.Name())
				queueToPeers <- fileInfo
				//conf.postFileToPeer(&fileInfo)
				_, _ = SaveFileInfoToLevelDB(fileInfo.Md5, &fileInfo, conf.LevelDB(), conf)
				//conf.SaveFileMd5Log(&fileInfo, FileMd5Name)
			}
		}

		return nil
	}

	pathname := conf.StoreDir()
	pathPrefix, err = os.Readlink(pathname)
	if err == nil {
		//link
		pathname = pathPrefix
		if strings.HasSuffix(pathPrefix, "/") {
			//bugfix fullpath
			pathPrefix = pathPrefix[0 : len(pathPrefix)-1]
		}
	}
	fi, err = os.Stat(pathname)
	if err != nil {
		log.Error(err)
	}
	if fi.IsDir() {
		_ = filepath.Walk(pathname, handleFunc)
	}

	log.Info("RepairFileInfoFromFile is finish.")
}

func GetFilePathByInfo(fileInfo *FileInfo, withDocker bool) string {
	_ = withDocker
	fileName := fileInfo.Name
	if fileInfo.ReName != "" {
		fileName = fileInfo.ReName
	}

	return fileInfo.Path + "/" + fileName
}

func CheckFileExistByInfo(md5s string, fileInfo *FileInfo, conf *config.Config) bool {
	_ = md5s
	if fileInfo == nil {
		return false
	}

	if fileInfo.OffSet >= 0 {
		//small file
		if info, err := GetFileInfoFromLevelDB(fileInfo.Md5, conf); err == nil && info.Md5 == fileInfo.Md5 {
			return true
		}

		return false
	}

	fullPath := GetFilePathByInfo(fileInfo, true)
	fi, err := os.Stat(fullPath)
	if err != nil {
		return false
	}

	if fi.Size() == fileInfo.Size {
		return true
	}

	return false
}

func SaveFileInfoToLevelDB(key string, fileInfo *FileInfo, db *leveldb.DB, conf *config.Config) (*FileInfo, error) {
	if fileInfo == nil || db == nil {
		return nil, errors.New("fileInfo is null or db is null")
	}

	data, err := config.Json.Marshal(fileInfo)
	if err != nil {
		return fileInfo, err
	}

	if err = db.Put([]byte(key), data, nil); err != nil {
		return fileInfo, err
	}

	if db == conf.LevelDB() { //search slow ,write fast, double write logDB
		logDate := pkg.GetDayFromTimeStamp(fileInfo.TimeStamp)
		logKey := fmt.Sprintf("%s_%s_%s", logDate, conf.FileMd5Name(), fileInfo.Md5)
		_ = conf.LevelDB().Put([]byte(logKey), data, nil)
	}

	return fileInfo, nil
}

func SyncFileInfo(path string, router *gin.RouterGroup, conf *config.Config) {
	router.PUT(path, func(ctx *gin.Context) {
		fileInfo := FileInfo{}

		r := ctx.Request
		if !IsPeer(r, conf) {
			ctx.JSON(http.StatusForbidden, GetClusterNotPermitMessage(r))
			return
		}

		fileInfoStr := r.FormValue("fileInfo")
		if err := config.Json.Unmarshal([]byte(fileInfoStr), &fileInfo); err != nil {
			ctx.JSON(http.StatusNotFound, GetClusterNotPermitMessage(r))
			log.Error(err)
			return
		}

		if fileInfo.OffSet == -2 {
			// optimize migrate
			_, _ = SaveFileInfoToLevelDB(fileInfo.Md5, &fileInfo, conf.LevelDB(), conf)
		} else {
			SaveFileMd5Log(&fileInfo, conf.Md5QueueFile(), conf)
		}

		AppendToDownloadQueue(&fileInfo, conf)
		fileName := fileInfo.Name
		if fileInfo.ReName != "" {
			fileName = fileInfo.ReName
		}
		filePath := strings.Replace(fileInfo.Path, conf.StoreDir()+"/", "", 1)
		downloadUrl := fmt.Sprintf("http://%s/%s", r.Host, filePath+"/"+fileName)
		log.Info("SyncFileInfo: ", downloadUrl)

		ctx.JSON(http.StatusOK, downloadUrl)
	})
}

func GetFileInfo(path string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(path, func(ctx *gin.Context) {
		var result JsonResult

		r := ctx.Request
		md5sum := r.FormValue("md5")
		filePath := r.FormValue("path")
		result.Status = "fail"
		if !IsPeer(r, conf) {
			ctx.JSON(http.StatusNotFound, GetClusterNotPermitMessage(r))
			return
		}
		md5sum = r.FormValue("md5")
		if filePath != "" {
			filePath = strings.Replace(filePath, "/"+conf.FileDownloadPathPrefix(), conf.StoreDirName()+"/", 1)
			md5sum = pkg.MD5(filePath)
		}
		fileInfo, err := GetFileInfoFromLevelDB(md5sum, conf)
		if err != nil {
			log.Error(err)
			result.Message = err.Error()
			ctx.JSON(http.StatusNotFound, result)
			return
		}

		result.Status = "ok"
		result.Data = fileInfo
		ctx.JSON(http.StatusOK, result)
	})
}
