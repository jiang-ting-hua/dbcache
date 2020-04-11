package logs

import (
	"fmt"
	"os"
	"path"
	"time"
)

//输出到文件.
type FileLog struct {
	Enable      bool         `conf:"enable"`        //配置文件中,是否启用
	Level       logLevel     `conf:"run_level"`     //配置文件中,file日志等级.
	FilePath    string       `conf:"file_path"`     //配置文件中,文件路径.
	FileName    string       `conf:"file_name"`     //配置文件中,文件名.
	MaxFileSize int64        `conf:"max_file_size"` //配置文件中,日志文件最大大小.
	MaxLogChan  int          `conf:"max_log_chan"`  //配置文件中,日志存放管道最大数
	FileObj     *os.File     //标准日志文件对象.
	ErrFileObj  *os.File     //单独错误日志文件对象.
	LogChan     chan *logMsg //日志存放管道
}

type logMsg struct {
	level     logLevel
	msg       string
	funcName  string
	fileName  string
	timestamp string
	line      int
}

func NewFileLog(enable bool, levelStr, filePath, fileName string, maxFileSize int64, maxLogChan int) (fileLog *FileLog) {
	level, err := GetLevel(levelStr)
	if err != nil {
		fmt.Println("GetLevel() failed, err:",err)
	}

	fileLog = &FileLog{
		Enable:      enable,
		Level:       level,
		FilePath:    filePath,
		FileName:    fileName,
		MaxFileSize: maxFileSize,
		MaxLogChan:  maxLogChan,
		LogChan:     make(chan *logMsg, maxLogChan),
	}
	err = fileLog.initFileLog()
	if err != nil {
		panic(err)
	}
	return fileLog
}
func (f *FileLog) initFileLog() (err error) {
	fullFileName := path.Join(f.FilePath, f.FileName)
	fileObj, err := os.OpenFile(fullFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		err = fmt.Errorf("open log file failed, file name:%s, err:%v\n", fullFileName, err)
		return err
	}
	errFileObj, err := os.OpenFile(fullFileName+".err", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		err = fmt.Errorf("open error log file failed, file name:%s, err:%v\n", fullFileName+".err", err)
		return err
	}
	f.FileObj = fileObj
	f.ErrFileObj = errFileObj
	//后台异步写日志
	go f.backOutputFile()

	return nil
}
func (f *FileLog) Close() {
	f.FileObj.Close()
	f.ErrFileObj.Close()
}

func (f *FileLog) isEnable(level logLevel) bool {
	return level >= f.Level
}
func (f *FileLog) checkSize(file *os.File) bool {
	fileInfo, err := file.Stat()
	if err != nil {
		err = fmt.Errorf("get file info failed, file name:%s, err:%v\n", file.Name(), err)
		return false
	}
	//如果文件大于等于日志最大值,返回真.
	return fileInfo.Size() >= f.MaxFileSize
}
func (f *FileLog) splitFile(file *os.File) (newFile *os.File, err error) {
	//获取原文件的信息
	//old := file.Name()
	info, err := file.Stat()
	if err != nil {
		err = fmt.Errorf("get log file info failed, err:%v\n", err)
		return nil, err
	}
	oldFileName := info.Name()
	time := time.Now().Format("20060102_150405")
	logFileName := path.Join(f.FilePath, oldFileName)
	newPath := fmt.Sprintf("%s_%s.bak", logFileName, time)

	//关闭当前文件
	file.Close()
	//原文件改名
	os.Rename(logFileName, newPath)
	//再次打开新的日志文件
	fileObj, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		err = fmt.Errorf("again open log file failed, file name:%s, err:%v\n", logFileName, err)
		return nil, err
	}
	return fileObj, nil
}

//输出日志
func (f *FileLog) outputFile(level logLevel, format string, a ...interface{}) {
	if f.isEnable(level) && f.Enable == true {
		msg := fmt.Sprintf(format, a...)
		time := time.Now().Format("2006-01-02 15:04:05")
		funcName, fileName, line := getCallInfo(5)

		logTmp := &logMsg{
			level:     level,
			msg:       msg,
			funcName:  funcName,
			fileName:  fileName,
			timestamp: time,
			line:      line,
		}
		select {
		case f.LogChan <- logTmp:
		default:
			fmt.Println("log output File,channel blocked")
		}
	}
}

//后台异步写文件
func (f *FileLog) backOutputFile() {
	for {
		if f.checkSize(f.FileObj) {
			newFile, err := f.splitFile(f.FileObj)
			if err != nil {
				fmt.Println(err)
			}
			f.FileObj = newFile
		}

		logTmp := &logMsg{}
		select {
		case logTmp = <-f.LogChan:
		default:
			time.Sleep(time.Millisecond * 500)
			continue
		}

		msgInfo := fmt.Sprintf("[%s] [%s] [%s][%s:%d] %s\n", logTmp.timestamp, GetLevelStr(logTmp.level), logTmp.fileName, logTmp.funcName, logTmp.line, logTmp.msg)
		fmt.Fprintf(f.FileObj, msgInfo)
		time.Sleep(time.Millisecond * 500)
		//如果日志等级大于ERROR,另外在err错误日志再记录一遍.
		if logTmp.level >= ERROR {
			if f.checkSize(f.ErrFileObj) {
				newFile, err := f.splitFile(f.ErrFileObj)
				if err != nil {
					fmt.Println(err)
				}
				f.ErrFileObj = newFile
			}
			msgInfo = fmt.Sprintf("[%s] [%s] [%s][%s:%d] %s\n", logTmp.timestamp, GetLevelStr(logTmp.level), logTmp.fileName, logTmp.funcName, logTmp.line, logTmp.msg)
			fmt.Fprintf(f.ErrFileObj, msgInfo)
			time.Sleep(time.Millisecond * 500)
		}
	}
}

func (f *FileLog) Unknown(format string, a ...interface{}) {
	f.outputFile(UNKNOWN, format, a...)
}
func (f *FileLog) Debug(format string, a ...interface{}) {
	f.outputFile(DEBUG, format, a...)
}
func (f *FileLog) Trace(format string, a ...interface{}) {
	f.outputFile(TRACE, format, a...)
}
func (f *FileLog) Info(format string, a ...interface{}) {
	f.outputFile(INFO, format, a...)
}
func (f *FileLog) Warning(format string, a ...interface{}) {
	f.outputFile(WARNING, format, a...)
}
func (f *FileLog) Error(format string, a ...interface{}) {
	f.outputFile(ERROR, format, a...)
}
func (f *FileLog) Fatal(format string, a ...interface{}) {
	f.outputFile(FATAL, format, a...)
}
