package conf

import "strings"

//数据库数据同步.是实时还是异步.
type DataSync struct {
	RealTime            bool   `conf:"real_time"`              //是否实时同步更新,true:实时更新,false:异步更新.
	AsyncMaxChan        int    `conf:"async_max_chan"`         //异步更新管道,最大缓存数.
	AsyncFilePath       string `conf:"async_file_path"`        //异步保存需要更新的SQL语句文件路径
	AsyncFileName       string `conf:"async_file_name"`        //异步保存需要更新的SQL语句文件名
	AsyncFailedFileName string `conf:"async_failed_file_name"` //异步保存失败的需要更新的SQL语句文件名
	MaxAsyncFileSize    int64  `conf:"max_async_file_size"`    //异步保存需要更新的SQL语句文件和失败,单个文件最大大小
}

type Table interface {
	GetPkey() string
	GetTableName() string
	GetWhere() string
	GetOrther() string
	GetColumn() string
	GetColumns() []string
	PkeyIsIncrement() bool
	GetCacheType()string
}

//注意,配置文件cache.conf中的组名[Table]和结构名称一样.

//数据库表Users
type Users struct {
	TableName         string `conf:"table_name"`          //缓存的表名
	Columns           string `conf:"columns"`             //缓存的多列,以分号隔开
	Pkey              string `conf:"pkey"`                //缓存表的主键
	Where             string `conf:"where"`               //缓存表,取数据时,加的where条件.
	Orther            string `conf:"orther"`              //缓存表,取数据时,按条件排序.
	PkeyAutoIncrement bool   `conf:"pkey_auto_increment"` //缓存表,主键是否为自增列
	CacheType         string   `conf:"cache_type"`          //缓存表,主键是否为自增列
}

func (u *Users) GetPkey() (Pkey string) {
	return u.Pkey
}
func (u *Users) GetTableName() (tableName string) {
	return u.TableName
}
func (u *Users) GetWhere() (where string) {
	return u.Where
}
func (u *Users) GetOrther() (where string) {
	return u.Orther
}
func (u *Users) GetColumn() (Columns string) {
	return u.Columns
}
func (u *Users) GetColumns() (Columns []string) {
	if u.Columns == "" {
		return nil
	}
	Columns = strings.Split(u.Columns, ",")
	for i, _ := range Columns {
		Columns[i] = strings.TrimSpace(Columns[i])
	}
	return Columns
}
func (u *Users) PkeyIsIncrement() (isIncrement bool) {
	return u.PkeyAutoIncrement
}
func (u *Users) GetCacheType()(cacheType string) {
	return u.CacheType
}
//数据库表Goods
type Goods struct {
	TableName         string `conf:"table_name"`          //缓存的表名
	Columns           string `conf:"columns"`             //缓存的多列,以分号隔开
	Pkey              string `conf:"pkey"`                //缓存表的主键
	Where             string `conf:"where"`               //缓存表,取数据时,加的where条件.
	Orther            string `conf:"orther"`              //缓存表,取数据时,按条件排序.
	PkeyAutoIncrement bool   `conf:"pkey_auto_increment"` //缓存表,主键是否为自增列
	CacheType         string   `conf:"cache_type"`          //缓存表,主键是否为自增列
}

func (u *Goods) GetPkey() (Pkey string) {
	return u.Pkey
}
func (u *Goods) GetTableName() (tableName string) {
	return u.TableName
}
func (u *Goods) GetWhere() (where string) {
	return u.Where
}
func (u *Goods) GetOrther() (where string) {
	return u.Orther
}
func (u *Goods) GetColumn() (Columns string) {
	return u.Columns
}
func (u *Goods) GetColumns() (Columns []string) {
	if u.Columns == "" {
		return nil
	}
	Columns = strings.Split(u.Columns, ",")
	for i, _ := range Columns {
		Columns[i] = strings.TrimSpace(Columns[i])
	}
	return Columns
}
func (u *Goods) PkeyIsIncrement() (isIncrement bool) {
	return u.PkeyAutoIncrement
}
func (u *Goods) GetCacheType()(cacheType string) {
	return u.CacheType
}