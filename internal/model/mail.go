package model

import (
	"net/smtp"
	"strings"

	"github.com/luoyunpeng/go-fastdfs/internal/config"
)

func SendMail(to, subject, body, mailType string) error {
	host := config.CommonConfig.Mail.Host
	user := config.CommonConfig.Mail.User
	password := config.CommonConfig.Mail.Password
	hp := strings.Split(host, ":")
	auth := smtp.PlainAuth("", user, password, hp[0])
	var contentType string
	if mailType == "html" {
		contentType = "Content-Type: text/" + mailType + "; charset=UTF-8"
	} else {
		contentType = "Content-Type: text/plain" + "; charset=UTF-8"
	}
	msg := []byte("To: " + to + "\r\nFrom: " + user + ">\r\nSubject: " + "\r\n" + contentType + "\r\n\r\n" + body)
	sendTo := strings.Split(to, ";")
	err := smtp.SendMail(host, auth, user, sendTo, msg)
	return err
}
