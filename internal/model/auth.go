package model

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/astaxie/beego/httplib"
	"github.com/gin-gonic/gin"
	"github.com/luoyunpeng/go-fastdfs/internal/config"
	"github.com/luoyunpeng/go-fastdfs/pkg"
	log "github.com/sirupsen/logrus"
	"github.com/sjqzhang/googleAuthenticator"
)

// CheckAuth
func CheckAuth(r *http.Request, conf *config.Config) bool {
	var (
		err        error
		req        *httplib.BeegoHTTPRequest
		result     string
		jsonResult JsonResult
	)
	if err = r.ParseForm(); err != nil {
		log.Error(err)
		return false
	}

	req = httplib.Post(conf.AuthUrl())
	req.SetTimeout(time.Second*10, time.Second*10)
	req.Param("__path__", r.URL.Path)
	req.Param("__query__", r.URL.RawQuery)
	for k := range r.Form {
		req.Param(k, r.FormValue(k))
	}
	for k, v := range r.Header {
		req.Header(k, v[0])
	}
	result, err = req.String()
	result = strings.TrimSpace(result)
	if strings.HasPrefix(result, "{") && strings.HasSuffix(result, "}") {
		if err = config.Json.Unmarshal([]byte(result), &jsonResult); err != nil {
			log.Error(err)
			return false
		}

		if jsonResult.Data != "ok" {
			log.Warn(result)
			return false
		}

	} else {
		if result != "ok" {
			log.Warn(result)
			return false
		}
	}

	return true
}

func VerifyGoogleCode(secret string, code string, discrepancy int64) bool {
	var gAuth *googleAuthenticator.GAuth

	gAuth = googleAuthenticator.NewGAuth()
	if ok, err := gAuth.VerifyCode(secret, code, discrepancy); ok {
		return ok
	} else {
		log.Error(err)

		return ok
	}
}

func CheckDownloadAuth(ctx *gin.Context, conf *config.Config) (bool, error) {
	r := ctx.Request

	if conf.EnableDownloadAuth() && conf.AuthUrl() != "" && !IsPeer(r, conf) && !CheckAuth(r, conf) {
		return false, errors.New("auth fail")
	}

	if conf.DownloadUseToken() && !IsPeer(r, conf) {
		token := ctx.Query("token")
		timestamp := ctx.Query("timestamp")
		if token == "" || timestamp == "" {
			return false, errors.New("invalid request")
		}

		maxTimestamp := time.Now().Add(time.Second *
			time.Duration(conf.DownloadTokenExpire())).Unix()
		minTimestamp := time.Now().Add(-time.Second *
			time.Duration(conf.DownloadTokenExpire())).Unix()
		ts, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil {
			return false, errors.New("invalid timestamp")
		}

		if ts > maxTimestamp || ts < minTimestamp {
			return false, errors.New("timestamp expire")
		}

		fullPath, smallPath := GetFilePathFromRequest(ctx.Request.RequestURI, conf)

		pathMd5 := pkg.MD5(fullPath)
		if smallPath != "" {
			pathMd5 = pkg.MD5(smallPath)
		}
		if fileInfo, err := GetFileInfoFromLevelDB(pathMd5, conf); err != nil {
			// TODO
		} else {
			if !(pkg.MD5(fileInfo.Md5+timestamp) == token) {
				return false, errors.New("invalid token")
			}

			return false, nil
		}
	}

	if conf.EnableGoogleAuth() && !IsPeer(r, conf) {
		fullPath := r.RequestURI[2:len(r.RequestURI)]
		fullPath = strings.Split(fullPath, "?")[0] // just path
		scene := strings.Split(fullPath, "/")[0]
		code := r.FormValue("code")
		if secret, ok := conf.SceneMap().GetValue(scene); ok {
			if !VerifyGoogleCode(secret.(string), code, int64(conf.DownloadTokenExpire()/30)) {
				return false, errors.New("invalid google code")
			}
		}
	}

	return true, nil
}
