package db

import (
	"database/sql"
	"dbcache/conf"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

const (
	PING_TIME = 600 //数据库ping的时间间隔(秒)
)

var (
	DbConfig = conf.DbConfig{} //[配置文件config.conf]保存数据库配置信息
)

//连接数据库
func initDB() (db *sql.DB, err error) {

	//读取数据库登录配置文件,保存于结构conf.DbConfig{}中.
	err = conf.ParseConf(conf.CONFIG_FILE, &DbConfig)
	if err != nil {
		err = fmt.Errorf("initDB(),Read the database login configuration file [%s] faild, err: %s", conf.CONFIG_FILE, err)
		return nil, err
	}

	dataSource := DbConfig.User + ":" + DbConfig.Pwd + "@tcp(" + DbConfig.Ip + ":" + DbConfig.Port + ")/" + DbConfig.DatabaseName + "?" + DbConfig.ConnectInfo
	db, err = sql.Open("mysql", dataSource)
	if err != nil {
		err = fmt.Errorf("initDB(),Database connection failed, err: %s", err)
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		err = fmt.Errorf("initDB(),Database connection failed, err: %s", err)
		return nil, err
	}
	return db, err
}

func ConnectDB() (db *sql.DB, err error) {
	db, err = initDB()
	if err != nil {
		err = fmt.Errorf("ConnectDB() err:  %s", err)
		return nil, err
	}
	// Connection pool and timeouts
	//DbConn.SetMaxOpenConns(100)                  // 最大打开连接数,默认值为0表示不限制。
	//DbConn.SetMaxIdleConns(5)                  // 最大空闲连接数,默认的最大空闲连接数是2
	//DbConn.SetConnMaxLifetime(time.Second * 60)    // 设置一个连接可以重用的最大时间量。连接过期时间如不设置 连接会被一直保持
	go dbPing(db)
	return db, err
}

func dbPing(db *sql.DB) {
	for {
		time.Sleep(time.Second * time.Duration(PING_TIME))
		err := db.Ping()
		if err != nil {
			for {
				time.Sleep(time.Second * 60)
				db, err = initDB()
				if err == nil {
					break
				}
			}
		}
	}
}
