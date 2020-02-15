package model

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"time"

	"github.com/astaxie/beego/httplib"
	mapSet "github.com/deckarep/golang-set"
	"github.com/gin-gonic/gin"
	"github.com/luoyunpeng/go-fastdfs/internal/config"
	"github.com/luoyunpeng/go-fastdfs/pkg"
	log "github.com/sirupsen/logrus"
)

var (
	//TODO: queueToPeers will receive all file-info that will be sent to all peers
	queueToPeers chan FileInfo
	//TODO: queueFromPeers will receive all file-info that will be wrote to local peer
	queueFromPeers chan FileInfo
)

func init() {
	queueToPeers = make(chan FileInfo, 100)
	queueFromPeers = make(chan FileInfo, 100)
}

// IsPeer check the host that create the request is in the peers
func IsPeer(r *http.Request, conf *config.Config) bool {
	var (
		ip     string
		peer   string
		inside bool
	)

	//return true
	ip = pkg.GetClientIp(r)
	realIp := os.Getenv("GO_FASTDFS_IP")
	if realIp == "" {
		realIp = pkg.GetPublicIP()
	}
	if ip == "127.0.0.1" || ip == realIp {
		return true
	}

	if pkg.Contains(conf.AdminIps(), ip) {
		return true
	}

	ip = "http://" + ip
	inside = false
	for _, peer = range conf.Peers() {
		if strings.HasPrefix(peer, ip) {
			inside = true
			break
		}
	}

	return inside
}

// CheckClusterStatus
func CheckClusterStatus(conf *config.Config) {
	check := func() {
		defer func() {
			if re := recover(); re != nil {
				buffer := debug.Stack()
				log.Error("CheckClusterStatus")
				log.Error(re)
				log.Error(string(buffer))
			}
		}()

		var (
			status  JsonResult
			err     error
			subject string
			body    string
			req     *httplib.BeegoHTTPRequest
		)

		for _, peer := range conf.Peers() {
			req = httplib.Get(peer + GetRequestURI("status"))
			req.SetTimeout(time.Second*5, time.Second*5)
			err = req.ToJSON(&status)
			if err != nil || status.Status != "ok" {
				for _, to := range conf.AlarmReceivers() {
					subject = "fastdfs server error"
					if err != nil {
						body = fmt.Sprintf("%s\nserver:%s\nerror:\n%s", subject, peer, err.Error())
					} else {
						body = fmt.Sprintf("%s\nserver:%s\n", subject, peer)
					}
					if err = SendMail(to, subject, body, "text", conf); err != nil {
						log.Error(err)
					}
				}

				if conf.AlarmUrl() != "" {
					req = httplib.Post(conf.AlarmUrl())
					req.SetTimeout(time.Second*10, time.Second*10)
					req.Param("message", body)
					req.Param("subject", subject)
					if _, err = req.String(); err != nil {
						log.Error(err)
					}
				}
			}
		}
	}

	go func() {
		for {
			time.Sleep(time.Minute * 10)
			check()
		}
	}()
}

// GetClusterNotPermitMessage
func GetClusterNotPermitMessage(r *http.Request) string {
	return fmt.Sprintf(config.MessageClusterIp, pkg.GetClientIp(r))
}

// checkPeerFileExist
func checkPeerFileExist(peer string, md5sum string, fPath string) (*FileInfo, error) {
	var fileInfo FileInfo

	req := httplib.Post(fmt.Sprintf("%s%s?md5=%s", peer, GetRequestURI("check_file_exist"), md5sum))
	req.Param("path", fPath)
	req.Param("md5", md5sum)
	req.SetTimeout(time.Second*5, time.Second*10)
	if err := req.ToJSON(&fileInfo); err != nil {
		return &FileInfo{}, err
	}

	if fileInfo.Md5 == "" {
		return &fileInfo, errors.New("not found")
	}

	return &fileInfo, nil
}

func DownloadFromPeer(peer string, fileInfo *FileInfo, conf *config.Config) {
	var (
		err         error
		filename    string
		fpath       string
		fpathTmp    string
		fi          os.FileInfo
		sum         string
		data        []byte
		downloadUrl string
	)

	if conf.ReadOnly() {
		log.Warn("ReadOnly", fileInfo)
		return
	}

	if conf.RetryCount() > 0 && fileInfo.Retry >= conf.RetryCount() {
		log.Error("DownloadFromPeer Error ", fileInfo)
		return
	} else {
		fileInfo.Retry = fileInfo.Retry + 1
	}
	filename = fileInfo.Name
	if fileInfo.Name != "" {
		filename = fileInfo.Name
	}

	if fileInfo.OffSet != -2 && conf.EnableDistinctFile() && CheckFileExistByInfo(fileInfo.Md5, fileInfo, conf) {
		// ignore migrate file
		log.Info(fmt.Sprintf("DownloadFromPeer file Exist, path:%s", fileInfo.Path+"/"+fileInfo.Name))
		return
	}

	if (!conf.EnableDistinctFile() || fileInfo.OffSet == -2) && pkg.FileExists(GetFilePathByInfo(fileInfo, true)) {
		// ignore migrate file
		if fi, err = os.Stat(GetFilePathByInfo(fileInfo, true)); err == nil {
			if fi.ModTime().Unix() > fileInfo.TimeStamp {
				log.Info(fmt.Sprintf("ignore file sync path:%s", GetFilePathByInfo(fileInfo, false)))
				fileInfo.TimeStamp = fi.ModTime().Unix()
				PostFileToPeer(fileInfo, conf) // keep newer

				return
			}

			_ = os.Remove(GetFilePathByInfo(fileInfo, true))
		}
	}

	if _, err = os.Stat(fileInfo.Path); err != nil {
		_ = os.MkdirAll(fileInfo.Path, 0775)
	}
	//fmt.Println("downloadFromPeer",fileInfo)
	p := strings.Replace(fileInfo.Path, conf.StoreDir()+"/", "", 1)
	//filename=util.UrlEncode(filename)
	downloadUrl = peer + "/" + p + "/" + filename
	log.Info("DownloadFromPeer: ", downloadUrl)
	fpath = fileInfo.Path + "/" + filename
	fpathTmp = fileInfo.Path + "/" + fmt.Sprintf("%s_%s", "tmp_", filename)
	timeout := fileInfo.Size/1024/1024/1 + 30
	if conf.SyncTimeout() > 0 {
		timeout = conf.SyncTimeout()
	}

	conf.LockMap().LockKey(fpath)
	defer conf.LockMap().UnLockKey(fpath)

	downloadKey := fmt.Sprintf("downloading_%d_%s", time.Now().Unix(), fpath)
	_ = conf.LevelDB().Put([]byte(downloadKey), []byte(""), nil)
	defer func() {
		_ = conf.LevelDB().Delete([]byte(downloadKey), nil)
	}()

	if fileInfo.OffSet == -2 {
		//migrate file
		if fi, err = os.Stat(fpath); err == nil && fi.Size() == fileInfo.Size {
			//prevent double download
			_, _ = SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, conf.LevelDB(), conf)
			//log.Info(fmt.Sprintf("file '%s' has download", fpath))
			return
		}

		req := httplib.Get(downloadUrl)
		req.SetTimeout(time.Second*30, time.Second*time.Duration(timeout))
		if err = req.ToFile(fpathTmp); err != nil {
			AppendToDownloadQueue(fileInfo, conf) //retry
			_ = os.Remove(fpathTmp)
			log.Error(err, fpathTmp)
			return
		}

		if os.Rename(fpathTmp, fpath) == nil {
			//svr.SaveFileMd5Log(fileInfo, FileMd5Name)
			_, _ = SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, conf.LevelDB(), conf)
		}

		return
	}
	req := httplib.Get(downloadUrl)
	req.SetTimeout(time.Second*30, time.Second*time.Duration(timeout))
	if fileInfo.OffSet >= 0 {
		//small file download
		data, err = req.Bytes()
		if err != nil {
			AppendToDownloadQueue(fileInfo, conf) //retry
			log.Error(err)
			return
		}

		data2 := make([]byte, len(data)+1)
		data2[0] = '1'
		for i, v := range data {
			data2[i+1] = v
		}
		data = data2
		if int64(len(data)) != fileInfo.Size {
			log.Warn("file size is error")
			return
		}

		fpath = strings.Split(fpath, ",")[0]
		err = pkg.WriteFileByOffSet(fpath, fileInfo.OffSet, data)
		if err != nil {
			log.Warn(err)
			return
		}

		SaveFileMd5Log(fileInfo, conf.FileMd5(), conf)
		return
	}
	if err = req.ToFile(fpathTmp); err != nil {
		AppendToDownloadQueue(fileInfo, conf) //retry
		_ = os.Remove(fpathTmp)
		log.Error(err)
		return
	}

	if fi, err = os.Stat(fpathTmp); err != nil {
		_ = os.Remove(fpathTmp)
		return
	}
	_ = sum
	//if config.CommonConfig.EnableDistinctFile {
	//	//DistinctFile
	//	if sum, err = util.GetFileSumByName(fpathTmp, config.CommonConfig.FileSumArithmetic); err != nil {
	//		log.Error(err)
	//		return
	//	}
	//} else {
	//	//DistinctFile By path
	//	sum = util.MD5(svr.GetFilePathByInfo(fileInfo, false))
	//}
	if fi.Size() != fileInfo.Size { //  maybe has bug remove || sum != fileMd5
		log.Error("file sum check error")
		_ = os.Remove(fpathTmp)
		return
	}

	if os.Rename(fpathTmp, fpath) == nil {
		SaveFileMd5Log(fileInfo, conf.FileMd5(), conf)
	}
}

func CheckFileAndSendToPeer(date string, filename string, isForceUpload bool, conf *config.Config) {
	var (
		md5set mapSet.Set
		err    error
		md5s   []interface{}
	)
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("CheckFileAndSendToPeer")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()

	if md5set, err = GetMd5sByDate(date, filename, conf); err != nil {
		log.Error(err)
		return
	}

	md5s = md5set.ToSlice()
	for _, md := range md5s {
		if md == nil {
			continue
		}

		if fileInfo, _ := GetFileInfoFromLevelDB(md.(string), conf); fileInfo != nil && fileInfo.Md5 != "" {
			if isForceUpload {
				fileInfo.Peers = []string{}
			}
			if len(fileInfo.Peers) > len(conf.Peers()) {
				continue
			}

			if !pkg.Contains(fileInfo.Peers, conf.Addr()) {
				fileInfo.Peers = append(fileInfo.Peers, conf.Addr()) // peer is null
			}
			if filename == conf.Md5QueueFile() {
				AppendToDownloadQueue(fileInfo, conf)
				continue
			}

			AppendToQueue(fileInfo, conf)
		}
	}
}

func PostFileToPeer(fileInfo *FileInfo, conf *config.Config) {
	for _, peer := range conf.Peers() {
		if pkg.Contains(fileInfo.Peers, peer) {
			continue
		}

		fileName := fileInfo.Name
		if fileInfo.ReName != "" {
			fileName = fileInfo.ReName
			if fileInfo.OffSet != -1 {
				fileName = strings.Split(fileInfo.ReName, ",")[0]
			}
		}

		fPath := path.Join(conf.StoreDir(), fileInfo.Path, fileName)
		if !pkg.FileExists(fPath) {
			log.Warn(fmt.Sprintf("file '%s' not found", fPath))
			continue
		}

		if fileInfo.Size == 0 {
			if fi, err := os.Stat(fPath); err != nil {
				log.Error(err)
			} else {
				fileInfo.Size = fi.Size()
			}
		}

		if fileInfo.OffSet != -2 && conf.EnableDistinctFile() {
			//not migrate file should check or update file
			// where not EnableDistinctFile should check
			if info, err := checkPeerFileExist(peer, fileInfo.Md5, ""); info != nil && info.Md5 != "" {
				fileInfo.Peers = append(fileInfo.Peers, peer)
				if _, err = SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, conf.LevelDB(), conf); err != nil {
					log.Error(err)
				}

				continue
			}
		}

		postURL := peer + "/" + GetRequestURI("syncfile_info")
		b := httplib.Post(postURL)
		b.SetTimeout(time.Second*30, time.Second*30)
		data, err := config.Json.Marshal(fileInfo)
		if err != nil {
			log.Error(err)
			return
		}

		b.Param("fileInfo", string(data))
		result, err := b.String()
		if err != nil {
			if fileInfo.Retry <= conf.RetryCount() {
				fileInfo.Retry = fileInfo.Retry + 1
				AppendToQueue(fileInfo, conf)
			}
			log.Error(err, fmt.Sprintf(" path:%s", fileInfo.Path+"/"+fileInfo.Name))
		}

		if !strings.HasPrefix(result, "http://") || err != nil {
			log.Error(err)
			SaveFileMd5Log(fileInfo, conf.Md5ErrorFile(), conf)
		}
		if strings.HasPrefix(result, "http://") {
			log.Info(result)
			if !pkg.Contains(fileInfo.Peers, peer) {
				fileInfo.Peers = append(fileInfo.Peers, peer)
				if _, err = SaveFileInfoToLevelDB(fileInfo.Md5, fileInfo, conf.LevelDB(), conf); err != nil {
					log.Error(err)
				}
			}
		}
	}
}

func Sync(relativePath string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(relativePath, func(ctx *gin.Context) {
		var result JsonResult

		r := ctx.Request
		result.Status = "fail"
		if !IsPeer(r, conf) {
			result.Message = "client must be in cluster"
			ctx.JSON(http.StatusNotFound, result)
			return
		}

		date := ""
		force := ""
		inner := ""
		isForceUpload := false
		force = ctx.Query("force")
		date = ctx.Query("date")
		inner = ctx.Query("inner")
		if force == "1" {
			isForceUpload = true
		}
		if inner != "1" {
			for _, peer := range conf.Peers() {
				req := httplib.Post(peer + GetRequestURI("sync"))
				req.Param("force", force)
				req.Param("inner", "1")
				req.Param("date", date)
				if _, err := req.String(); err != nil {
					log.Error(err)
				}
			}
		}
		if date == "" {
			result.Message = "require paramete date &force , ?date=20181230"
			ctx.JSON(http.StatusNotFound, result)
			return
		}

		date = strings.Replace(date, ".", "", -1)
		if isForceUpload {
			go CheckFileAndSendToPeer(date, conf.FileMd5Name(), isForceUpload, conf)
		} else {
			go CheckFileAndSendToPeer(date, conf.Md5ErrorFile(), isForceUpload, conf)
		}

		result.Status = "ok"
		result.Message = "job is running"
		ctx.JSON(http.StatusOK, result)
	})
}

func ConsumerPostToPeer(conf *config.Config) {
	ConsumerFunc := func() {
		for fileInfo := range queueToPeers {
			PostFileToPeer(&fileInfo, conf)
		}
	}

	for i := 0; i < conf.SyncWorker(); i++ {
		go ConsumerFunc()
	}
}
