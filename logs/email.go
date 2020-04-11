package logs

import (
	"crypto/tls"
	"fmt"
	"github.com/go-gomail/gomail"
	"log"
	"net"
	"net/smtp"
	"strings"
	"time"
)

type EmailLog struct {
	Enable       bool     `conf:"enable"`         //配置文件中,是否启用
	Level        logLevel `conf:"run_level"`      //配置文件中,email日志等级.
	Host         string   `conf:"host"`           // 邮箱服务器地址，如smtp.163.com
	Port         int      `conf:"port"`           //邮箱服务器端口，如25,465
	SendEmail    string   `conf:"send_email"`     //发件人邮箱地址
	SendPasswd   string   `conf:"send_passwd"`    //发件人邮箱密码（明文形式）
	Recipient    string   `conf:"recipient"`      // 接收者.如有多个,则以英文逗号(,)隔开
	EmailCC      string   `conf:"emailCC"`        // 抄送者.如有多个,则以英文逗号(,)隔开
	MaxEmailChan int      `conf:"max_email_chan"` //异步发送邮件,最大缓存邮件数
	Mail         *gomail.Message //发邮件对象
	EmailChan    chan *emailMsg //邮件日志存放管道
}
//邮件消息对象
type emailMsg struct {
	subject string //主题
	body    string //内容
}

func NewEmailLog(enable bool, levelStr, host string, port int, sendEmail, sendPasswd, recipient, emailCC string, maxEmailChan int) *EmailLog {
	level, err := GetLevel(levelStr)
	if err != nil {
		fmt.Println("GetLevel() failed, err:",err)
	}
	if len(recipient) == 0 {
		return nil
	}
	var (
		recipientList = []string{}
		emailCCList   = []string{}
	)
	emailLog := &EmailLog{
		Enable:     enable,
		Level:      level,
		Host:       host,
		Port:       port,
		SendEmail:  sendEmail,
		SendPasswd: sendPasswd,
		Recipient:  recipient,
		EmailCC:    emailCC,
		Mail:       gomail.NewMessage(),
		EmailChan:  make(chan *emailMsg, maxEmailChan),
	}

	// 多个收件人
	if strings.Contains(recipient, ",") {
		for _, tmp := range strings.Split(recipient, ",") {
			recipientList = append(recipientList, strings.TrimSpace(tmp))
		}
	} else {
		emailCCList = append(emailCCList, strings.TrimSpace(recipient))
	}

	emailLog.Mail.SetHeader("To", recipientList...)

	//抄送人
	if len(emailLog.EmailCC) != 0 {
		if strings.Contains(emailLog.EmailCC, ",") {
			for _, tmp := range strings.Split(emailLog.EmailCC, ",") {
				emailCCList = append(emailCCList, strings.TrimSpace(tmp))
			}
		} else {
			emailCCList = append(emailCCList, strings.TrimSpace(emailLog.EmailCC))
		}
		emailLog.Mail.SetHeader("Cc", emailCCList...)
	}

	// 发件人
	emailLog.Mail.SetAddressHeader("From", emailLog.SendEmail, "")

	emailLog.initEmailLog()
	return emailLog
}

func (e *EmailLog) initEmailLog() {
	//后台异步写日志
	go e.backOutputEmail()

}

// SendEmail body支持html格式字符串
func (e *EmailLog) SendEmailLog(subject, body string) {
	// 主题
	e.Mail.SetHeader("Subject", subject)

	// 正文
	e.Mail.SetBody("text/html", body)
	//附件
	//m.Attach("/home/Alex/lolcat.jpg")

	//阿里邮箱 pass填密码，qq邮箱和163填授权码
	d := gomail.NewDialer(e.Host, e.Port, e.SendEmail, e.SendPasswd)
	//d := gomail.NewPlainDialer("smtp.qq.com", 587, "11111111@qq.com", "授权码")
	//d.TLSConfig = &tls.Config{InsecureSkipVerify: true}

	// 发送
	err := d.DialAndSend(e.Mail)
	if err != nil {
		fmt.Println("send email faild,err :", err)
	}
	time.Sleep(time.Millisecond * 500)
}

func (e *EmailLog) outputEmail(level logLevel, format string, a ...interface{}) {
	if e.isEnable(level) && e.Enable == true {
		msg := fmt.Sprintf(format, a...)
		time := time.Now().Format("2006-01-02 15:04:05")
		funcName, fileName, line := getCallInfo(5)
		msgAll := fmt.Sprintf("[%s] [%s] [%s][%s:%d] %s\n", time, GetLevelStr(level), fileName, funcName, line, msg)
		emsg := emailMsg{
			subject: GetLevelStr(level) + " [" + time + "]",
			body:    msgAll,
		}
		select {
		case e.EmailChan <- &emsg:
		default:
			fmt.Println("log output email,channel blocked")
		}

	}
}

func (e *EmailLog) backOutputEmail() {
	for {
		msg := &emailMsg{}
		select {
		case msg = <-e.EmailChan:
			e.SendEmailLog(msg.subject, msg.body)
			time.Sleep(time.Millisecond * 500)
		default:
			time.Sleep(time.Millisecond * 500)
			continue
		}
	}
}
func (e *EmailLog) isEnable(level logLevel) bool {
	return level >= e.Level
}
func (e *EmailLog) Unknown(format string, a ...interface{}) {
	e.outputEmail(UNKNOWN, format, a...)
}
func (e *EmailLog) Debug(format string, a ...interface{}) {
	e.outputEmail(DEBUG, format, a...)
}
func (e *EmailLog) Trace(format string, a ...interface{}) {
	e.outputEmail(TRACE, format, a...)
}
func (e *EmailLog) Info(format string, a ...interface{}) {
	e.outputEmail(INFO, format, a...)
}
func (e *EmailLog) Warning(format string, a ...interface{}) {
	e.outputEmail(WARNING, format, a...)
}
func (e *EmailLog) Error(format string, a ...interface{}) {
	e.outputEmail(ERROR, format, a...)
}
func (e *EmailLog) Fatal(format string, a ...interface{}) {
	e.outputEmail(FATAL, format, a...)
}
func (e *EmailLog) Close() {

}

//以下是安全传输层协议（TLS）,来传输邮件。
func test() {
	host := "smtp.163.com"
	port := 465
	email := "fgfdh@163.com"
	password := "afgfd3456"
	toEmail := "4543589@qq.com"
	header := make(map[string]string)
	header["From"] = "test" + "<" + email + ">"
	header["To"] = toEmail
	header["Subject"] = "邮件标题"
	header["Content-Type"] = "text/html; charset=UTF-8"
	body := `[2020-02-08 19:46:28] [ERROR] [main.go][main:101] InsertRow,插入错误: err: InsertDbRow(),插入行到数据库失败.`
	message := ""
	for k, v := range header {
		message += fmt.Sprintf("%s: %s\r\n", k, v)
	}
	message += "\r\n" + body
	auth := smtp.PlainAuth(
		"",
		email,
		password,
		host,
	)
	err := SendMailUsingTLS(
		fmt.Sprintf("%s:%d", host, port),
		auth,
		email,
		[]string{toEmail},
		[]byte(message),
	)
	if err != nil {
		panic(err)
	}
}

//return a smtp client
func Dial(addr string) (*smtp.Client, error) {
	conn, err := tls.Dial("tcp", addr, nil)
	if err != nil {
		log.Println("Dialing Error:", err)
		return nil, err
	}
	//分解主机端口字符串
	host, _, _ := net.SplitHostPort(addr)
	return smtp.NewClient(conn, host)
}

//参考net/smtp的func SendMail()
//使用net.Dial连接tls(ssl)端口时,smtp.NewClient()会卡住且不提示err
//len(to)>1时,to[1]开始提示是密送
func SendMailUsingTLS(addr string, auth smtp.Auth, from string,
	to []string, msg []byte) (err error) {
	//create smtp client
	c, err := Dial(addr)
	if err != nil {
		log.Println("Create smpt client error:", err)
		return err
	}
	defer c.Close()
	if auth != nil {
		if ok, _ := c.Extension("AUTH"); ok {
			if err = c.Auth(auth); err != nil {
				log.Println("Error during AUTH", err)
				return err
			}
		}
	}
	if err = c.Mail(from); err != nil {
		return err
	}
	for _, addr := range to {
		if err = c.Rcpt(addr); err != nil {
			return err
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	_, err = w.Write(msg)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	return c.Quit()
}
