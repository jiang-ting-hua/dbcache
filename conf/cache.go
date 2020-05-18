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
//缓存的表配置
type CacheTable struct {
	TableName         string `conf:"table_name"`          //缓存的表名
	Columns           string `conf:"columns"`             //缓存的多列,以分号隔开
	Pkey              string `conf:"pkey"`                //缓存表的主键
	Where             string `conf:"where"`               //缓存表取数据时,加的where条件.
	Orther            string `conf:"orther"`              //缓存表取数据时,按排序条件.在运行中,插入数据也是按此排序.
	PkeyAutoIncrement bool   `conf:"pkey_auto_increment"` //缓存表主键是否为自增列
	CacheType         string `conf:"cache_type"`          //用于分页查询,缓存类型:一.slice切片(按orther里排序),二.sliceNotDel切片(不删除,只记录,速度最快,但后插入数据未排序),三.link链表(按orther里排序)
	IsRealtime        bool   `conf:"is_realtime"`         //缓存表是否实时同步更新,true:实时更新,false:异步更新.
	IsWaitResult      bool   `conf:"is_wait_result"`      //缓存表在异步更新时,是否等待返回结果(上面条件是is_realtime = false时)
}

func (c *CacheTable) GetPkey() string                      { return c.Pkey }
func (c *CacheTable) GetTableName() string                 { return c.TableName }
func (c *CacheTable) GetWhere() string                     { return c.Where }
func (c *CacheTable) GetOrther() string                    { return c.Orther }
func (c *CacheTable) GetColumn() string                    { return c.Columns }
func (c *CacheTable) PkeyIsIncrement() bool                { return c.PkeyAutoIncrement }
func (c *CacheTable) GetCacheType() string                 { return c.CacheType }
func (c *CacheTable) GetIsRealtime() bool                  { return c.IsRealtime }
func (c *CacheTable) GetColumns() (columns []string)       { return getColumns(c.Columns) }
func (c *CacheTable) GetSortColumn() (sortColumn string)   { return getSortColumn(c.Orther, c.Pkey) }
func (c *CacheTable) GetSortMode() (sortMode string)       { return getSortMode(c.Orther) }
func (c *CacheTable) GetIsWaitResult() (isWaitResult bool) { return c.IsWaitResult }

//根据以逗号分割的列字符串,转换为切片.
func getColumns(columnStr string) (columns []string) {
	if columnStr == "" {
		return nil
	}
	columns = strings.Split(columnStr, ",")
	for i, _ := range columns {
		columns[i] = strings.TrimSpace(columns[i])
	}
	return columns
}

//获取排序字段和排序方式
func getSortColumn(orther string, pkey string) (sortColumn string) {
	if orther == "" {
		return pkey
	}
	slices := strings.Split(orther, " ")
	if len(slices) == 0 {
		return pkey
	}
	var isOrder, isBy bool
	for i, v := range slices {
		v = strings.ToLower(strings.TrimSpace(v))
		if v == "order" {
			isOrder = true
		}
		if v == "by" {
			isBy = true
		}
		if i > 0 {
			if slices[i-1] == "by" {
				sortColumn = v
			}
		}
	}
	if sortColumn == "" {
		return pkey
	}
	if isOrder && isBy {
		return sortColumn
	} else {
		return pkey
	}
}

//获取排序字段和排序方式
func getSortMode(orther string) (sortMode string) {
	if orther == "" {
		return ""
	}
	slices := strings.Split(orther, " ")
	if len(slices) == 0 {
		return ""
	}
	var isOrder, isBy, isDesc, isAsc bool
	for _, v := range slices {
		v = strings.ToLower(strings.TrimSpace(v))
		if v == "order" {
			isOrder = true
		}
		if v == "by" {
			isBy = true
		}
		if v == "asc" {
			isAsc = true
		}
		if v == "desc" {
			isDesc = true
		}

	}
	if isOrder && isBy && isAsc {
		return "asc"
	}
	if isOrder && isBy && isDesc {
		return "desc"
	}
	if isOrder && isBy && !isAsc && !isDesc {
		return "asc"
	}
	return ""
}
