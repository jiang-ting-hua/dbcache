package conf

import "strings"

//数据库异步同步.
type DataAsync struct {
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
	GetCacheType() string
	GetIsRealtime() bool
}

//数据库表Users,注意:表名和配置文件cache.conf中的组名一样.
type Users struct {
	TableName         string `conf:"table_name"`          //缓存的表名
	Columns           string `conf:"columns"`             //缓存的多列,以分号隔开
	Pkey              string `conf:"pkey"`                //缓存表的主键
	Where             string `conf:"where"`               //缓存表,取数据时,加的where条件.
	Orther            string `conf:"orther"`              //缓存表,取数据时,按条件排序.
	PkeyAutoIncrement bool   `conf:"pkey_auto_increment"` //缓存表,主键是否为自增列
	CacheType         string `conf:"cache_type"`          //缓存表,主键是否为自增列
	IsRealtime        bool   `conf:"is_realtime"`         //是否实时同步更新,true:实时更新,false:异步更新.
}

func (u *Users) GetPkey() string       { return u.Pkey }
func (u *Users) GetTableName() string  { return u.TableName }
func (u *Users) GetWhere() string      { return u.Where }
func (u *Users) GetOrther() string     { return u.Orther }
func (u *Users) GetColumn() string     { return u.Columns }
func (u *Users) PkeyIsIncrement() bool { return u.PkeyAutoIncrement }
func (u *Users) GetCacheType() string  { return u.CacheType }
func (u *Users) GetIsRealtime() bool   { return u.IsRealtime }
func (u *Users) GetColumns() (columns []string) {
	if u.Columns == "" {
		return nil
	}
	columns = strings.Split(u.Columns, ",")
	for i, _ := range columns {
		columns[i] = strings.TrimSpace(columns[i])
	}
	return columns
}

//数据库表Goods, 注意:表名和配置文件cache.conf中的组名一样.
type Goods struct {
	TableName         string `conf:"table_name"`          //缓存的表名
	Columns           string `conf:"columns"`             //缓存的多列,以分号隔开
	Pkey              string `conf:"pkey"`                //缓存表的主键
	Where             string `conf:"where"`               //缓存表,取数据时,加的where条件.
	Orther            string `conf:"orther"`              //缓存表,取数据时,按条件排序.
	PkeyAutoIncrement bool   `conf:"pkey_auto_increment"` //缓存表,主键是否为自增列
	CacheType         string `conf:"cache_type"`          //缓存表,主键是否为自增列
	IsRealtime        bool   `conf:"is_realtime"`         //是否实时同步更新,true:实时更新,false:异步更新.
}

func (u *Goods) GetPkey() string       { return u.Pkey }
func (u *Goods) GetTableName() string  { return u.TableName }
func (u *Goods) GetWhere() string      { return u.Where }
func (u *Goods) GetOrther() string     { return u.Orther }
func (u *Goods) GetColumn() string     { return u.Columns }
func (u *Goods) PkeyIsIncrement() bool { return u.PkeyAutoIncrement }
func (u *Goods) GetCacheType() string  { return u.CacheType }
func (u *Goods) GetIsRealtime() bool   { return u.IsRealtime }
func (u *Goods) GetColumns() (columns []string) {
	if u.Columns == "" {
		return nil
	}
	columns = strings.Split(u.Columns, ",")
	for i, _ := range columns {
		columns[i] = strings.TrimSpace(columns[i])
	}
	return columns
}
