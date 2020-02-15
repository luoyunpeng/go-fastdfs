package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/astaxie/beego/httplib"
	mapSet "github.com/deckarep/golang-set"
	"github.com/gin-gonic/gin"
	"github.com/luoyunpeng/go-fastdfs/internal/config"
	"github.com/luoyunpeng/go-fastdfs/internal/model"
	"github.com/luoyunpeng/go-fastdfs/pkg"
	log "github.com/sirupsen/logrus"
)

//Read: GetMd5File download file 'data/files.md5'?
/*func GetMd5File(ctx *gin.Context) {
	var date string
	r := ctx.Request

	if !model.IsPeer(r) {
		return
	}
	filePath := config.DataDir + "/" + date + "/" + config.FileMd5Name
	if !pkg.FileExists(filePath) {
		ctx.JSON(http.StatusNotFound, filePath+"does not exist")
		return
	}

	ctx.File(filePath)
}*/

// RemoveEmptyDir remove empty dir
func RemoveEmptyDir(path string, router *gin.RouterGroup, conf *config.Config) {
	router.DELETE(path, func(ctx *gin.Context) {
		r := ctx.Request
		if model.IsPeer(r, conf) {
			pkg.RemoveEmptyDir(conf.DataDir())
			pkg.RemoveEmptyDir(conf.StoreDir())
			ctx.JSON(http.StatusOK, "")
			return
		}

		ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
	})
}

//ListDir list all file in given dir
func ListDir(path string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(path, func(ctx *gin.Context) {
		var filesResult []model.FileInfoResult

		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		dir := ctx.Query("dir")
		dir = strings.Replace(dir, ".", "", -1)
		if tmpDir, err := os.Readlink(dir); err == nil {
			dir = tmpDir
		}
		filesInfo, err := ioutil.ReadDir(conf.StoreDir() + "/" + dir)
		if err != nil {
			log.Error(err)

			ctx.JSON(http.StatusInternalServerError, err.Error())
			return
		}

		for _, f := range filesInfo {
			fi := model.FileInfoResult{
				Name:    f.Name(),
				Size:    f.Size(),
				IsDir:   f.IsDir(),
				ModTime: f.ModTime().Unix(),
				Path:    dir,
				Md5:     pkg.MD5(strings.Replace(conf.StoreDir()+"/"+dir+"/"+f.Name(), "//", "/", -1)),
			}
			filesResult = append(filesResult, fi)
		}

		ctx.JSON(http.StatusOK, filesResult)
	})
}

// Report
func Report(path string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(path, func(ctx *gin.Context) {
		var (
			data           []byte
			reportFileName string
			result         model.JsonResult
			html           string
			err            error
		)

		r := ctx.Request
		result.Status = "ok"
		if model.IsPeer(r, conf) {
			reportFileName = conf.StaticDir() + "/report.html"
			if pkg.Exist(reportFileName) {
				if data, err = pkg.ReadFile(reportFileName); err != nil {
					log.Error(err)
					result.Message = err.Error()
					ctx.JSON(http.StatusNotFound, result)
					return
				}

				html = string(data)
				html = strings.Replace(html, "{group}", "", 1)

				ctx.HTML(http.StatusOK, "report.html", html)
				return
			}

			ctx.JSON(http.StatusNotFound, fmt.Sprintf("%s is not found", reportFileName))
			return
		}

		ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
	})
}

// Index point to upload page
func Index(uri string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(uri, func(ctx *gin.Context) {
		if conf.EnableWebUpload() {
			ctx.HTML(http.StatusOK, "upload.tmpl", gin.H{"title": "Main website"})
			//ctx.Data(http.StatusOK, "text/html", []byte(config.DefaultUploadPage))
		}

		ctx.JSON(http.StatusNotFound, "web upload deny")
	})
}

// GetMd5sForWeb
func GetMd5sForWeb(path string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(path, func(ctx *gin.Context) {
		var (
			date   string
			err    error
			result mapSet.Set
			lines  []string
			md5s   []interface{}
		)

		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		date = ctx.Query("date")
		if result, err = model.GetMd5sByDate(date, conf.FileMd5Name(), conf); err != nil {
			log.Error(err)
			ctx.JSON(http.StatusNotFound, err.Error())
			return
		}

		md5s = result.ToSlice()
		for _, line := range md5s {
			if line != nil && line != "" {
				lines = append(lines, line.(string))
			}
		}

		ctx.JSON(http.StatusOK, strings.Join(lines, ","))
	})
}

//
func Download(uri string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(uri, func(ctx *gin.Context) {
		var fileInfo os.FileInfo
		var err error

		reqURI := ctx.Request.RequestURI
		// if params is not enough then redirect to upload
		if pkg.CheckUploadURIInvalid(reqURI) {
			log.Warnf("RequestURI-%s is invalid, redirect to index", reqURI)
			ctx.JSON(http.StatusBadRequest, "RequestURI is invalid")
			return
		}

		if ok, err := model.CheckDownloadAuth(ctx, conf); !ok {
			log.Error(err)
			ctx.JSON(http.StatusUnauthorized, "not Permitted")
			return
		}

		fullPath, smallPath := model.GetFilePathFromRequest(ctx, conf)
		if smallPath == "" {
			if fileInfo, err = os.Stat(fullPath); err != nil {
				model.DownloadNotFound(ctx, conf)
				return
			}

			if !conf.ShowDir() && fileInfo.IsDir() {
				ctx.JSON(http.StatusNotAcceptable, "list dir deny")
				return
			}

			_, _ = model.DownloadNormalFileByURI(ctx, conf)
			return
		}

		if ok, _ := model.DownloadSmallFileByURI(ctx, conf); !ok {
			model.DownloadNotFound(ctx, conf)
		}
	})
}

func CheckFileExist(reqPath string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(reqPath, func(ctx *gin.Context) {
		md5sum := ctx.Query("md5")
		fPath := ctx.Query("path")

		if fileInfo, err := model.GetFileInfoFromLevelDB(md5sum, conf); fileInfo != nil {
			if fileInfo.OffSet != -1 { // TODO: what does offset mean? -1 means deleted?
				ctx.JSON(http.StatusOK, fileInfo)
				return
			}

			fPath = conf.StoreDir() + "/" + fileInfo.Path + "/" + fileInfo.Name
			if fileInfo.ReName != "" {
				fPath = conf.StoreDir() + "/" + fileInfo.Path + "/" + fileInfo.ReName
			}

			if pkg.Exist(fPath) {
				ctx.JSON(http.StatusOK, fileInfo)
				return
			}

			if fileInfo.OffSet == -1 {
				err = model.RemoveKeyFromLevelDB(md5sum, conf.LevelDB()) // when file delete,delete from leveldb
				if err != nil {
					log.Warnf("delete %s from levelDB error: ", md5sum, err)
				}
			}

			ctx.JSON(http.StatusNotFound, "no such file"+fileInfo.Path+"/"+fileInfo.Name)
			return
		}

		if fPath != "" {
			absPath := conf.StoreDir() + "/" + fPath
			fileInfo, err := os.Stat(absPath)
			if err == nil {
				sum := pkg.MD5(fPath)
				//if config.CommonConfig.EnableDistinctFile {
				//	sum, err = pkg.GetFileSumByName(fpath, config.CommonConfig.FileSumArithmetic)
				//	if err != nil {
				//		log.Error(err)
				//	}
				//}
				fileInfo := &model.FileInfo{
					Path:      path.Dir(fPath),
					Name:      path.Base(fPath),
					Size:      fileInfo.Size(),
					Md5:       sum,
					Peers:     []string{conf.Addr()},
					OffSet:    -1, //very important
					TimeStamp: fileInfo.ModTime().Unix(),
				}

				ctx.JSON(http.StatusOK, fileInfo)
				return
			}
		}

		ctx.JSON(http.StatusNotFound, "please check file path or md5 value")
	})
}

func CheckFilesExist(path string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(path, func(ctx *gin.Context) {
		var fileInfos []*model.FileInfo

		md5sum := ctx.Query("md5s")
		md5s := strings.Split(strings.Trim(md5sum, " "), ",")
		if len(md5s) == 0 {
			ctx.JSON(http.StatusNotFound, "at lease one md5 must given")
			return
		}

		for _, m := range md5s {
			if fileInfo, _ := model.GetFileInfoFromLevelDB(m, conf); fileInfo != nil {
				if fileInfo.OffSet != -1 {
					fileInfos = append(fileInfos, fileInfo)

					continue
				}

				filePath := conf.StoreDir() + "/" + fileInfo.Path + "/" + fileInfo.Name
				if fileInfo.ReName != "" {
					filePath = conf.StoreDir() + "/" + fileInfo.Path + "/" + fileInfo.ReName
				}

				if pkg.Exist(filePath) {
					fileInfos = append(fileInfos, fileInfo)
					continue
				}

				if fileInfo.OffSet == -1 {
					err := model.RemoveKeyFromLevelDB(md5sum, conf.LevelDB()) // when file delete,delete from leveldb
					if err != nil {
						log.Warnf("delete %s from levelDB error: ", md5sum, err)
					}
				}
			}
		}

		if len(fileInfos) == 0 {
			ctx.JSON(http.StatusNotFound, "no such file")
			return
		}

		ctx.JSON(http.StatusOK, fileInfos)
	})
}

// Upload
func Upload(relativePath string, router *gin.RouterGroup, conf *config.Config) {
	router.POST(relativePath, func(ctx *gin.Context) {
		if conf.ReadOnly() {
			ctx.JSON(http.StatusForbidden, "file server read-only")
			return
		}

		if conf.AuthUrl() != "" {
			r := ctx.Request
			if !model.CheckAuth(r, conf) {
				log.Warn("auth fail", r.Form)
				ctx.JSON(http.StatusUnauthorized, "auth fail")
				return
			}
		}

		scene := ctx.PostForm("scene")
		if scene == "" {
			scene = conf.DefaultScene()
		}
		if _, err := pkg.CheckScene(scene, conf.Scenes()); err != nil {
			ctx.JSON(http.StatusBadRequest, err.Error())
			return
		}

		code := ctx.PostForm("code")
		//Read: default not enable google auth
		if conf.EnableGoogleAuth() && scene != "" {
			if secret, ok := conf.SceneMap().GetValue(scene); ok {
				if !model.VerifyGoogleCode(secret.(string), code, int64(conf.DownloadTokenExpire()/30)) {
					ctx.JSON(http.StatusUnauthorized, "invalid request,error google code")
					return
				}
			}
		}

		fileInfo := model.FileInfo{}
		customFileName := ctx.PostForm("filename")
		md5sum := ctx.PostForm("md5")
		fileInfo.Md5 = md5sum
		fileInfo.ReName = customFileName
		fileInfo.OffSet = -1
		fileInfo.TimeStamp = time.Now().Unix()
		fileInfo.Scene = scene

		if conf.EnableCustomPath() {
			fileInfo.Path = ctx.PostForm("path")
			fileInfo.Path = strings.Trim(fileInfo.Path, "/")
		}

		uploadFileHeader, err := ctx.FormFile("file")
		if err != nil {
			ctx.JSON(http.StatusBadRequest, err.Error())
			return
		}

		if uploadFileHeader.Size <= 0 {
			log.Error("file size is zero")
			ctx.JSON(http.StatusNotFound, "file size is zero")
			return
		}

		fileInfo.Size = uploadFileHeader.Size
		fileInfo.Name = filepath.Base(uploadFileHeader.Filename)
		if conf.RenameFile() && fileInfo.ReName == "" {
			fileInfo.ReName = pkg.MD5(pkg.GetUUID()) + path.Ext(fileInfo.Name)
		}
		// if file path not given, create the upload dir by current date
		uploadDir := path.Join(conf.StoreDir(), fileInfo.Scene, conf.PeerId(), pkg.FormatTimeByHour(time.Now()))
		if fileInfo.Path != "" {
			uploadDir = strings.Split(fileInfo.Path, conf.StoreDir())[0]
			uploadDir = path.Join(conf.StoreDir(), fileInfo.Path)
		}

		// create the upload dir, no need to check if it exists
		if err := pkg.CreateDirectories(uploadDir, 0777); err != nil {
			log.Errorf("create upload dir error: %v", err)
			ctx.JSON(http.StatusInternalServerError, err.Error())
			return
		}

		uploadFile := uploadDir + "/" + fileInfo.Name
		if fileInfo.ReName != "" {
			uploadFile = uploadDir + "/" + fileInfo.ReName
		}

		tmpFolder := path.Join(conf.StoreDir(), "_tmp", pkg.Today())
		if err := pkg.CreateDirectories(tmpFolder, 0777); err != nil {
			ctx.JSON(http.StatusInternalServerError, err.Error())
			return
		}
		defer func() {
			_ = os.Remove(tmpFolder)
		}()

		// will remove after copy to target path
		tmpFileName := tmpFolder + "/" + pkg.GetUUID()
		if err := ctx.SaveUploadedFile(uploadFileHeader, tmpFileName); err != nil {
			log.Error(err)
			ctx.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		tmpFile, err := os.Open(tmpFileName)
		if err != nil {
			log.Error(err)
			ctx.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer func() {
			_ = tmpFile.Close()
		}()

		// md5 code or sha1 code
		if conf.EnableDistinctFile() {
			fileInfo.Md5 = pkg.GetFileSum(tmpFile, conf.FileSumArithmetic())
		} else {
			// TODO: bug fix file-count stat
			fileInfo.Md5 = pkg.MD5(model.GetFilePathByInfo(&fileInfo, false))
		}

		// o size file, or something else
		if fileInfo.Md5 == "" {
			log.Warn("the upload file Md5 is nil")
			ctx.JSON(http.StatusBadRequest, "the upload file Md5 is nil, please check your file")
			return
		}

		// the given sum(md5 or sha1) is not same as the real
		if md5sum != "" && fileInfo.Md5 != md5sum {
			log.Warn(" fileInfo.Md5 and md5sum !=")
		}

		fileInfo.Peers = append(fileInfo.Peers, conf.Addr())

		if conf.EnableDistinctFile() {
			queryFileInfo, _ := model.GetFileInfoFromLevelDB(fileInfo.Md5, conf)
			if queryFileInfo != nil && queryFileInfo.Md5 != "" && queryFileInfo.Path == fileInfo.Path {
				fileResult := model.BuildFileResult(queryFileInfo, ctx.Request.Host, conf)
				_ = os.Remove(tmpFileName)
				ctx.JSON(http.StatusOK, fileResult.Url)
				return
			}
		}

		if conf.EnableMergeSmallFile() && fileInfo.Size < int64(conf.SmallFileSize()) {
			if err := model.SaveSmallFile(&fileInfo, conf); err != nil {
				log.Error(err)
				ctx.JSON(http.StatusNotFound, err.Error())
				return
			}
		}

		if _, err := pkg.CopyfileRemove(tmpFileName, uploadFile); err != nil {
			log.Error(err)
			ctx.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		model.SaveFileMd5Log(&fileInfo, conf.FileMd5(), conf) //maybe slow
		go model.PostFileToPeer(&fileInfo, conf)

		// TODO: use upload worker
		// model.InternalUpload(ctx, conf, fileInfo)
		conf.RtMap().AddCountInt64(conf.UploadCounterKey(), ctx.Request.ContentLength)
		if v, ok := conf.RtMap().GetValue(conf.UploadCounterKey()); ok {
			if v.(int64) > 1*1024*1024*1024 {
				var _v int64
				conf.RtMap().Put(conf.UploadCounterKey(), _v)
				debug.FreeOSMemory()
			}
		}

		log.Info(fmt.Sprintf("upload: %s", uploadFile))
		ctx.JSON(http.StatusOK, model.BuildFileResult(&fileInfo, ctx.Request.Host, conf))
	})
}

func RemoveFile(path string, router *gin.RouterGroup, conf *config.Config) {
	router.DELETE(path, func(ctx *gin.Context) {
		var (
			err      error
			md5sum   string
			fileInfo *model.FileInfo
			fPath    string
			delUrl   string
			result   model.JsonResult
			inner    string
			name     string
		)

		r := ctx.Request
		md5sum = ctx.Query("md5")
		fPath = ctx.Query("path")
		inner = ctx.Query("inner")
		result.Status = "fail"
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		if conf.AuthUrl() != "" && !model.CheckAuth(r, conf) {
			ctx.JSON(http.StatusUnauthorized, "Unauthorized")
			return
		}

		if fPath != "" && md5sum == "" {
			fPath = strings.Replace(fPath, conf.FileDownloadPathPrefix(), "/", 1)
			md5sum = pkg.MD5(fPath)
		}

		if inner != "1" {
			for _, peer := range conf.Peers() {
				if peer == conf.Addr() {
					continue
				}

				delFile := func(peer string, md5sum string, fileInfo *model.FileInfo) {
					delUrl = peer + "/" + conf.FileDownloadPathPrefix()
					req := httplib.Delete(delUrl)
					req.Param("md5", md5sum)
					req.Param("inner", "1")
					req.SetTimeout(time.Second*5, time.Second*10)

					if _, err = req.String(); err != nil {
						log.Error(err)
					}
				}

				go delFile(peer, md5sum, fileInfo)
			}
		}

		if len(md5sum) < 32 {
			result.Message = "md5 invalid"
			ctx.JSON(http.StatusBadRequest, result)
			return
		}

		if fileInfo, err = model.GetFileInfoFromLevelDB(md5sum, conf); err != nil {
			result.Message = err.Error()
			ctx.JSON(http.StatusNotFound, result)
			return
		}

		if fileInfo.OffSet >= 0 {
			result.Message = "small file delete not support"

			ctx.JSON(http.StatusNotFound, result)
			return
		}

		name = fileInfo.Name
		if fileInfo.ReName != "" {
			name = fileInfo.ReName
		}
		fPath = conf.StoreDir() + "/" + fileInfo.Path + "/" + name
		if fileInfo.Path != "" && pkg.FileExists(fPath) {
			model.SaveFileMd5Log(fileInfo, conf.RemoveMd5File(), conf)
			if err = os.Remove(fPath); err != nil {
				result.Message = err.Error()

				ctx.JSON(http.StatusNotFound, result)
				return
			}

			result.Message = "remove success"
			result.Status = "ok"

			ctx.JSON(http.StatusOK, result)
			return
		}

		result.Message = "fail remove"
		ctx.JSON(http.StatusNotFound, result)
	})
}

func RepairFileInfo(path string, router *gin.RouterGroup, conf *config.Config) {
	router.PUT(path, func(ctx *gin.Context) {
		var result model.JsonResult

		if !model.IsPeer(ctx.Request, conf) {
			ctx.JSON(http.StatusNotFound, model.GetClusterNotPermitMessage(ctx.Request))
			return
		}

		if !conf.EnableMigrate() {
			ctx.JSON(http.StatusNotFound, "please set enable_migrate=true")
			return
		}

		result.Status = "ok"
		result.Message = "repair job start,don't try again,very danger "

		go model.RepairFileInfoFromFile(conf)

		ctx.JSON(http.StatusNotFound, result)
	})
}

// TODO
func Reload(path string, router *gin.RouterGroup, conf *config.Config) {
	router.PUT(path, func(ctx *gin.Context) {
		var (
			err     error
			data    []byte
			cfg     config.Config
			action  string
			cfgJson string
			result  model.JsonResult
		)

		r := ctx.Request
		result.Status = "fail"
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotFound, model.GetClusterNotPermitMessage(r))
			return
		}

		cfgJson = ctx.PostForm("cfg")
		action = ctx.PostForm("action")
		_ = cfgJson
		if action == "get" {
			result.Data = conf
			result.Status = "ok"
			ctx.JSON(http.StatusNotFound, result)
			return
		}

		if action == "set" {
			if cfgJson == "" {
				result.Message = "(error)parameter cfg(json) require"
				ctx.JSON(http.StatusNotFound, result)
				return
			}

			if err = config.Json.Unmarshal([]byte(cfgJson), &cfg); err != nil {
				log.Error(err)
				result.Message = err.Error()
				ctx.JSON(http.StatusNotFound, result)
				return
			}

			result.Status = "ok"
			cfgJson = pkg.JsonEncodePretty(cfg)
			pkg.WriteFile(config.DefaultConfigFile, cfgJson)

			ctx.JSON(http.StatusOK, result)
			return
		}

		if action == "reload" {
			if data, err = ioutil.ReadFile(config.DefaultConfigFile); err != nil {
				result.Message = err.Error()
				ctx.JSON(http.StatusNotFound, result)
				return
			}

			if err = config.Json.Unmarshal(data, &cfg); err != nil {
				result.Message = err.Error()
				ctx.JSON(http.StatusNotFound, result)
				return
			}

			//config.ParseConfig(config.DefaultConfigFile)
			model.InitComponent(true, conf)
			result.Status = "ok"

			ctx.JSON(http.StatusOK, result)
			return
		}

		ctx.JSON(http.StatusNotFound, "(error)action support set(json) get reload")
	})
}

func BackUp(path string, router *gin.RouterGroup, conf *config.Config) {
	router.POST(path, func(ctx *gin.Context) {
		var (
			err    error
			date   string
			result model.JsonResult
			inner  string
			url    string
		)

		r := ctx.Request
		result.Status = "ok"
		date = ctx.Query("date")
		inner = ctx.Query("inner")
		if date == "" {
			date = pkg.Today()
		}

		if model.IsPeer(r, conf) {
			if inner != "1" {
				for _, peer := range conf.Peers() {
					backUp := func(peer string, date string) {
						url = peer + model.GetRequestURI("backup")
						req := httplib.Post(url)
						req.Param("date", date)
						req.Param("inner", "1")
						req.SetTimeout(time.Second*5, time.Second*600)
						if _, err = req.String(); err != nil {
							log.Error(err)
						}
					}

					go backUp(peer, date)
				}
			}

			go model.BackUpMetaDataByDate(date, conf)

			result.Message = "back job start..."
			ctx.JSON(http.StatusOK, result)
			return
		}

		result.Message = model.GetClusterNotPermitMessage(r)
		ctx.JSON(http.StatusNotAcceptable, result)
	})
}

// Notice: performance is poor,just for low capacity,but low memory ,
//if you want to high performance,use searchMap for search,but memory ....
func Search(path string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(path, func(ctx *gin.Context) {
		var (
			result    model.JsonResult
			err       error
			kw        string
			count     int
			fileInfos []model.FileInfo
			md5s      []string
		)
		r := ctx.Request
		kw = ctx.Query("kw")
		if !model.IsPeer(r, conf) {
			result.Message = model.GetClusterNotPermitMessage(r)
			ctx.JSON(http.StatusNotAcceptable, result)
			return
		}

		iter := conf.LevelDB().NewIterator(nil, nil)
		for iter.Next() {
			var fileInfo model.FileInfo
			value := iter.Value()
			if err = config.Json.Unmarshal(value, &fileInfo); err != nil {
				log.Error(err)
				continue
			}

			if strings.Contains(fileInfo.Name, kw) && !pkg.Contains(md5s, fileInfo.Md5) {
				count = count + 1
				fileInfos = append(fileInfos, fileInfo)
				md5s = append(md5s, fileInfo.Md5)
			}
			if count >= 100 {
				break
			}
		}

		iter.Release()
		err = iter.Error()
		if err != nil {
			log.Error()
		}

		//fileInfos=svr.SearchDict(kw) // serch file from map for huge capacity
		result.Status = "ok"
		result.Data = fileInfos
		ctx.JSON(http.StatusOK, result)
	})
}

func Repair(path string, router *gin.RouterGroup, conf *config.Config) {
	router.POST(path, func(ctx *gin.Context) {
		var (
			forceRepair bool
			result      model.JsonResult
		)

		r := ctx.Request
		result.Status = "ok"
		force := ctx.PostForm("force")
		if force == "1" {
			forceRepair = true
		}

		if model.IsPeer(r, conf) {
			go model.AutoRepair(forceRepair, conf)

			result.Message = "repair job start..."
			ctx.JSON(http.StatusOK, result)
			return
		}

		result.Message = model.GetClusterNotPermitMessage(r)
		ctx.JSON(http.StatusNotAcceptable, result)
	})
}
