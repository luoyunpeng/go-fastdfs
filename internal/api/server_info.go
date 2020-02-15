package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/luoyunpeng/go-fastdfs/internal/config"
	"github.com/luoyunpeng/go-fastdfs/internal/model"
)

// Addr return current host addr
func Addr(relativePath string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(relativePath, func(ctx *gin.Context) {
		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		ctx.JSON(http.StatusOK, conf.Addr())
	})
}

// Peers return all peer
func Peers(relativePath string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(relativePath, func(ctx *gin.Context) {
		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		ctx.JSON(http.StatusOK, conf.Peers())
	})
}

// PeerID return current peer id
func PeerID(relativePath string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(relativePath, func(ctx *gin.Context) {
		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		ctx.JSON(http.StatusOK, conf.PeerId())
	})
}

// PeerID return current peer id
func StatMap(relativePath string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(relativePath, func(ctx *gin.Context) {
		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		conf.StatMap().Get()

		ctx.JSON(http.StatusOK, conf.StatMap().Get())
	})
}

// PeerID return current peer id
func SumMap(relativePath string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(relativePath, func(ctx *gin.Context) {
		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		conf.StatMap().Get()

		ctx.JSON(http.StatusOK, conf.SumMap().Get())
	})
}

// PeerID return current peer id
func RtMap(relativePath string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(relativePath, func(ctx *gin.Context) {
		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		conf.StatMap().Get()

		ctx.JSON(http.StatusOK, conf.RtMap().Get())
	})
}

// PeerID return current peer id
func SceneMap(relativePath string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(relativePath, func(ctx *gin.Context) {
		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		conf.StatMap().Get()

		ctx.JSON(http.StatusOK, conf.SceneMap().Get())
	})
}

// PeerID return current peer id
func SearchMap(relativePath string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(relativePath, func(ctx *gin.Context) {
		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		conf.StatMap().Get()

		ctx.JSON(http.StatusOK, conf.SearchMap().Get())
	})
}
