package logs

import (
	"fmt"
	"time"
)

//输出到标准控制台.
type StdoutLog struct {
	Enable bool     `conf:"enable"`    //配置文件中,是否启用
	Level  logLevel `conf:"run_level"` //配置文件中,标准输出日志等级.
}

func NewStdoutLog(enable bool, levelStr string) *StdoutLog {
	level, err := GetLevel(levelStr)
	if err != nil {
		fmt.Println("GetLevel() failed, err:",err)
	}
	return &StdoutLog{Enable: enable, Level: level}
}

func (c *StdoutLog) outputStdout(level logLevel, format string, a ...interface{}) {

	if c.isEnable(level) && c.Enable == true {
		msg := fmt.Sprintf(format, a...)
		time := time.Now().Format("2006-01-02 15:04:05")
		funcName, fileName, line := getCallInfo(5)
		fmt.Printf("[%s] [%s] [%s][%s:%d] %s\n", time, GetLevelStr(level), fileName, funcName, line, msg)
	}
}
func (c *StdoutLog) isEnable(level logLevel) bool {
	return level >= c.Level
}
func (c *StdoutLog) Unknown(format string, a ...interface{}) {
	c.outputStdout(UNKNOWN, format, a...)
}
func (c *StdoutLog) Debug(format string, a ...interface{}) {
	c.outputStdout(DEBUG, format, a...)
}
func (c *StdoutLog) Trace(format string, a ...interface{}) {
	c.outputStdout(TRACE, format, a...)
}
func (c *StdoutLog) Info(format string, a ...interface{}) {
	c.outputStdout(INFO, format, a...)
}
func (c *StdoutLog) Warning(format string, a ...interface{}) {
	c.outputStdout(WARNING, format, a...)
}
func (c *StdoutLog) Error(format string, a ...interface{}) {
	c.outputStdout(ERROR, format, a...)
}
func (c *StdoutLog) Fatal(format string, a ...interface{}) {
	c.outputStdout(FATAL, format, a...)
}
func (c *StdoutLog) Close() {
}
