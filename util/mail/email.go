package mail

import (
	"fmt"
	"gopkg.in/gomail.v2"
	"time"
)

type Config struct {
	MailSvr  string   `json:"svr"`
	Port     int      `json:"port"`
	From     string   `json:"from"`
	To       []string `json:"to"`
	Cc       []string `json:"cc"`
	Password string   `json:"pwd"`
}

func SendMail(content string) error {
	m := gomail.NewMessage()
	m.SetBody("text/html",content)

	mailHeader := map[string][]string{
		"From":    {"storage@epush.oppo.com"},
		"To":      {"chi.he@oppo.com"},
		"Subject": {fmt.Sprintf("新的迁移任务完成%s", time.Now().Format("2006-01-02"))},
	}
	dialer := gomail.NewDialer("smtphz.qiye.163.com", 25, "storage@epush.oppo.com", "j4-dwfb%KDvS")
	m.SetHeaders(mailHeader)
	return dialer.DialAndSend(m)
}
