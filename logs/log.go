package logs

import (
	"dbcache/conf"
	"errors"
	"fmt"
	"path"
	"runtime"
	"strings"
)

type logLevel uint16 //日志级别

//从上往下,日志级别越高
const (
	UNKNOWN logLevel = iota
	DEBUG
	TRACE
	INFO
	WARNING
	ERROR
	FATAL
)

var (
	Flog     *FileLog      //文件日志对象
	Slog     *StdoutLog    //标准输出对象
	Elog     *EmailLog     //email对象
	sLogConf = StdoutLog{} //标准输出配置信息
	fLogConf = FileLog{}   //文件配置信息
	elogConf = EmailLog{}  //email配置信息
)

//初始化日志
func InitLog() {
	//读取配置文件中，标准屏幕输出配置
	err := conf.ParseConf(conf.CONFIG_FILE, &sLogConf)
	if err != nil {
		fmt.Println("init log error:", err)
	}
	//读取配置文件中，文件日志配置
	err = conf.ParseConf(conf.CONFIG_FILE, &fLogConf)
	if err != nil {
		fmt.Println("init log error:", err)
	}
	//读取配置文件中，邮件日志配置
	err = conf.ParseConf(conf.CONFIG_FILE, &elogConf)
	if err != nil {
		fmt.Printf("ConnectDB(),read email config file failed, err:%s\n", err)
	}
	//标准输出日志对象
	Slog = NewStdoutLog(sLogConf.Enable, GetLevelStr(sLogConf.Level))
	//文件日志对象
	Flog = NewFileLog(fLogConf.Enable, GetLevelStr(fLogConf.Level), fLogConf.FilePath, fLogConf.FileName,
		fLogConf.MaxFileSize*1024*1024, fLogConf.MaxLogChan)
	//邮件输出日志对象
	Elog = NewEmailLog(elogConf.Enable, GetLevelStr(elogConf.Level), elogConf.Host, elogConf.Port, elogConf.SendEmail,
		elogConf.SendPasswd, elogConf.Recipient, elogConf.EmailCC, elogConf.MaxEmailChan)
}
func Debug(out, format string, a ...interface{}) {
	outLog(out, "Debug", format, a...)
}
func Trace(out, format string, a ...interface{}) {
	outLog(out, "Trace", format, a...)
}
func Info(out, format string, a ...interface{}) {
	outLog(out, "Info", format, a...)
}
func Warning(out, format string, a ...interface{}) {
	outLog(out, "Warning", format, a...)
}
func Error(out, format string, a ...interface{}) {
	outLog(out, "Error", format, a...)
}
func Fatal(out, format string, a ...interface{}) {
	outLog(out, "Fatal", format, a...)
}
func Close() {
	Flog.Close()
}

//A和ALL,则文件,终端,邮件都输出,FILE和F只输出到文件,STDOUT和S只输出到终端.EMAIL和E只输出到邮件
//注意:配置文件中是否开启 enable = true,运行等级run_level.都会影响输出.
//等级说明:1 DEBUG,2 TRACE,3 INFO,4 WARNING,5 ERROR,6 FATAL

func outLog(out, logLevel, format string, a ...interface{}) {
	out = strings.TrimSpace(out)
	out = strings.ToUpper(out)
	if len(out) == 0 {
		fmt.Println(`The out parameter Invalid,must be type "file"(f) or "stdout"(s) or "all"(a)`)
	}
	logLevel = strings.ToUpper(logLevel)
	if !(out == "FILE" || out == "F" || out == "STD" || out == "S" || out == "ALL" || out == "A" || out == "EMAIL" || out == "E") {
		fmt.Println(`The out parameter Invalid,must be type "file"(f) or "stdout"(s) or "email"(e) or "all"(a)`)
		return
	}

	if out == "FILE" || out == "F" || out == "A" || out == "ALL" {
		switch logLevel {
		case "DEBUG":
			Flog.Debug(format, a...)
		case "TRACE":
			Flog.Trace(format, a...)
		case "INFO":
			Flog.Info(format, a...)
		case "WARNING":
			Flog.Warning(format, a...)
		case "ERROR":
			Flog.Error(format, a...)
		case "FATAL":
			Flog.Fatal(format, a...)
		default:
			Flog.Unknown(format, a...)
		}
	}
	if out == "STD" || out == "S" || out == "A" || out == "ALL" {
		switch logLevel {
		case "DEBUG":
			Slog.Debug(format, a...)
		case "TRACE":
			Slog.Trace(format, a...)
		case "INFO":
			Slog.Info(format, a...)
		case "WARNING":
			Slog.Warning(format, a...)
		case "ERROR":
			Slog.Error(format, a...)
		case "FATAL":
			Slog.Fatal(format, a...)
		default:
			Slog.Unknown(format, a...)
		}
	}
	if out == "EMAIL" || out == "E" || out == "A" || out == "ALL" {
		switch logLevel {
		case "DEBUG":
			Elog.Debug(format, a...)
		case "TRACE":
			Elog.Trace(format, a...)
		case "INFO":
			Elog.Info(format, a...)
		case "WARNING":
			Elog.Warning(format, a...)
		case "ERROR":
			Elog.Error(format, a...)
		case "FATAL":
			Elog.Fatal(format, a...)
		default:
			Elog.Unknown(format, a...)
		}
	}
}
//获取等级
func GetLevel(levelStr string) (level logLevel, err error) {
	levelStr = strings.ToUpper(levelStr)
	switch levelStr {
	case "DEBUG":
		return DEBUG, nil
	case "TRACE":
		return TRACE, nil
	case "INFO":
		return INFO, nil
	case "WARNING":
		return WARNING, nil
	case "ERROR":
		return ERROR, nil
	case "FATAL":
		return FATAL, nil
	default:
		err = errors.New("Invalid log level")
		return UNKNOWN, err
	}
}
//获取等级的字符串
func GetLevelStr(level logLevel) (levelStr string) {
	switch level {
	case DEBUG:
		return "DEBUG"
	case TRACE:
		return "TRACE"
	case INFO:
		return "INFO"
	case WARNING:
		return "WARNING"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

//得到程序调用日志时的函数名,文件名和行号.
func getCallInfo(skip int) (funcName, fileName string, line int) {
	pc, file, line, ok := runtime.Caller(skip)
	if !ok {
		fmt.Println("getCallInfo() failed")
		return
	}
	funcName = runtime.FuncForPC(pc).Name()
	funcName = strings.Split(funcName, ".")[1]
	fileName = path.Base(file)
	return
}
