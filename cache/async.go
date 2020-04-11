package cache

import (
	"database/sql"
	"dbcache/conf"
	"dbcache/logs"
	"fmt"
	"os"
	"path"
	"time"
)

type DataSync struct {
	DataSyncConf       conf.DataSync  //[配置文件cache.conf]保存数据库数据同步.是实时还是异步.
	AsyncSqlchan       chan *AsyncSql //异步数据库同步管道
	AsyncFileObj       *os.File       //异步数据库同步,保存需要更新的SQL语句文件对象.
	AsyncFailedFileObj *os.File       //异步数据库同步,保存失败的需要更新的SQL语句文件对象.
}

//异步更新数据库
type AsyncSql struct {
	exeSql    string //异步数据更新SQL语句
	timestamp string //执行语句的时间
	isFinish  bool   //是否完成.
}

func NewDataSync() *DataSync {
	return &DataSync{
		DataSyncConf:       conf.DataSync{},
		AsyncSqlchan:       nil,
		AsyncFileObj:       nil,
		AsyncFailedFileObj: nil,
	}
}

//初始化异步同步信息.
func InitAsync(db *sql.DB,d *DataSync,tableName string) (err error) {
	//读取配置文件,数据库异步同步数据的信息.
	err = conf.ParseConf(conf.TABLES_CONF, &d.DataSyncConf)
	if err != nil {
		return err
	}
	//初始化文件对象
	d.AsyncFileObj, err = os.OpenFile(d.DataSyncConf.AsyncFilePath+tableName+"_"+d.DataSyncConf.AsyncFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		err = fmt.Errorf("open async sql file failed, file name:%s, err:%v\n", d.DataSyncConf.AsyncFilePath+tableName+"_"+d.DataSyncConf.AsyncFileName, err)
		return err
	}
	d.AsyncFailedFileObj, err = os.OpenFile(d.DataSyncConf.AsyncFilePath+tableName+"_"+d.DataSyncConf.AsyncFailedFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		err = fmt.Errorf("open async sql failed file failed, file name:%s, err:%v\n", d.DataSyncConf.AsyncFilePath+tableName+"_"+d.DataSyncConf.AsyncFailedFileName, err)
		return err
	}
	//初始化管道.
	d.AsyncSqlchan = make(chan *AsyncSql, d.DataSyncConf.AsyncMaxChan)
	//后台异步同步数据
	go d.backSyncSql(db)

	return nil
}

//后台同步数据库
func (d *DataSync) backSyncSql(db *sql.DB) {
	for {
		//检查文件容量大小
		if d.checkFileSize(d.AsyncFileObj) {
			newFile, err := d.splitFile(d.AsyncFileObj)
			if err != nil {
				logs.Error("a", "backSyncSql(),splitFile() faild. err: %s", err)
			}
			d.AsyncFileObj = newFile
		}
		//检查文件容量大小
		if d.checkFileSize(d.AsyncFailedFileObj) {
			newFile, err := d.splitFile(d.AsyncFailedFileObj)
			if err != nil {
				logs.Error("a", "backSyncSql(),splitFile() faild. err: %s", err)
			}
			d.AsyncFailedFileObj = newFile
		}

		//从管道中取出要执行的sql
		sqlTmp := &AsyncSql{}
		select {
		case sqlTmp = <-d.AsyncSqlchan:
		default:
			time.Sleep(time.Millisecond * 500)
			continue
		}

		//检查是否完成.
		if sqlTmp.isFinish == true {
			continue
		}
		//需执行的sql,首先保存于文件
		sqlMsg := fmt.Sprintf("/* %s */  %s;\n", sqlTmp.timestamp, sqlTmp.exeSql)
		fmt.Fprintf(d.AsyncFileObj, sqlMsg)

		//执行SQL语句
		_, err := db.Exec(sqlTmp.exeSql)
		if err != nil {
			//执行失败,休眠一会再次执行
			time.Sleep(time.Millisecond * 500)
			_, err := db.Exec(sqlTmp.exeSql)
			if err != nil {
				sqlMsg := fmt.Sprintf("/* [%s][%s] */  %s;\n", sqlTmp.timestamp, err, sqlTmp.exeSql)
				fmt.Fprintf(d.AsyncFailedFileObj, sqlMsg)
				logs.Error("a", "backSyncSql(),DbConn.Exec(%s) faild. err: %s", sqlTmp.exeSql, err)
				sqlTmp.isFinish = false
			}
		}
		sqlTmp.isFinish = true
	}
}

//检查文件大小
func (d *DataSync) checkFileSize(file *os.File) bool {
	fileInfo, err := file.Stat()
	if err != nil {
		err = fmt.Errorf("get async Sql file info failed, file name:%s, err:%v\n", file.Name(), err)
		return false
	}
	//如果文件大于等于异步保存SQL文件最大值,返回真.
	return fileInfo.Size() >= d.DataSyncConf.MaxAsyncFileSize*1024*1024
}

//分割文件
func (d *DataSync) splitFile(file *os.File) (newFile *os.File, err error) {
	//获取原文件的信息
	info, err := file.Stat()
	if err != nil {
		err = fmt.Errorf("get async Sql file info failed, err:%v\n", err)
		return nil, err
	}
	oldFileName := info.Name()
	currentTime := time.Now().Format("20060102_150405")
	logFileName := path.Join(d.DataSyncConf.AsyncFilePath, oldFileName)
	newPath := fmt.Sprintf("%s_%s.bak", logFileName, currentTime)

	//关闭当前文件
	file.Close()
	//原文件改名
	os.Rename(logFileName, newPath)
	//再次打开新的日志文件
	fileObj, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		err = fmt.Errorf("again open Async sql file failed, file name:%s, err:%v\n", logFileName, err)
		return nil, err
	}
	return fileObj, nil
}

//发送要执行的sql语句到管道.
func (d *DataSync) sendToAsyncChan(exeSql string) {
	sqlTmp := &AsyncSql{
		exeSql:    exeSql,
		timestamp: time.Now().Format("2006-01-02 15-04-05"),
		isFinish:  false,
	}
	select {
	case d.AsyncSqlchan <- sqlTmp:
	default:
		fmt.Println("Async sql output File,channel blocked")
	}
}
