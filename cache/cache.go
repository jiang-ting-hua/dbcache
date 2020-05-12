package cache

import (
	"database/sql"
	"dbcache/conf"
	"dbcache/logs"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"math"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	CacheObj = map[string]*DBcache{} //缓存表对象,用于rpc,grpc
)

//数据库缓存表中列的信息
type columnInfo struct {
	columnName       string       //列名
	scanType         reflect.Type //扫描类型.ScanType返回一个适合使用Rows.Scan进行扫描的Go类型,	如果驱动程序不支持此属性，则ScanType将返回空接口的类型。
	databaseTypeName string       //数据库类型
	isDecimalSize    bool         //是否是浮点数据
	precision        int64        //精度
	scale            int64        //小数的刻度
	isLength         bool         //(查询类型的长度,是不是OK,如果是true,length才有值,才有意义）.是否支持是文本和二进制字段类型,如果列类型不是可变长度，例如int，或者不支持
	length           int64        //文本和二进制字段类型的长度.最大Max Int64
	isNullable       bool         //(查询是否为空,是不是OK,如是是true,nullable才有值,才有意义）.如果驱动程序不支持此属性，将为false。
	nullable         bool         //是否可以为空
}

type DBcache struct {
	//基础配置信息
	DbConn      *sql.DB                //数据库对象
	TableConfig conf.Table             //[配置文件cache.conf]保存缓存数据表信息
	ColumnInfo  map[string]*columnInfo //数据库缓存表中列的信息
	CacheType   string                 //用于分页查询,缓存类型:一.slice切片,二.sliceNotDel切片(不删除,只记录),三.link链表
	dataAsync   *DataAsync             //异步同步数据库对象

	//map数据缓存对象[主缓存对象]
	DbCache sync.Map //用来缓存的表数据
	//链表缓存对象
	LinkDbCache LinkCache //链表保存缓存数据[用于页面分页显示](链表,插入和删除快,但查找,修改没切片数据快.适用于插入删除多.数据量大)
	//切片缓存对象(切片数组优点,因为内存是连续的,查找,修改快,但是插入和删除慢.适用于查询多.数据量少)
	SliceDbCache []*SliceCache //用来根据行号查询缓存,[用于页面分页显示]
	DelRowNum    map[int]bool  //(缓存是切片SliceDbCache,并且缓存类型是[sliceNotDel])保存已删除行的行号,当有删除行时,只是把删除的行号保存.未进行切片的删除,因为切片的删除会影响性能.但是这样的缺点是未排序.
	RowCount     int64         //总行数
	RwMutex      sync.RWMutex  //读写锁
}

//切片缓存数据
type SliceCache struct {
	Pkey       string    //主键值
	SortColumn string    //排序列值
	SortMode   string    //排列方式
	RowMap     *sync.Map //数据库中行的数据
}

func NewDBcache(db *sql.DB, table conf.Table) *DBcache {
	return &DBcache{
		DbConn:       db,
		TableConfig:  table,
		ColumnInfo:   nil,
		CacheType:    "link",
		dataAsync:    NewDatAsync(),
		DbCache:      sync.Map{},
		LinkDbCache:  NewLinkCache(),
		SliceDbCache: nil,
		DelRowNum:    make(map[int]bool),
		RowCount:     0,
		RwMutex:      sync.RWMutex{},
	}
}

//初始化缓存信息
func InitCache(db *sql.DB, table conf.Table) (dbCache *DBcache, err error) {
	dbCache = NewDBcache(db, table)
	//读取配置文件,初始化配置信息
	err = conf.ParseConf(conf.TABLES_CONF, dbCache.TableConfig)
	if err != nil {
		return nil, err
	}
	//根据配置文件生成select查询语句
	var selectSql string
	var countSql string
	if dbCache.TableConfig.GetWhere() == "" {
		selectSql = "select " + dbCache.TableConfig.GetColumn() +
			" from " + dbCache.TableConfig.GetTableName() + " " +
			dbCache.TableConfig.GetOrther()
		countSql = "select count(1) from " + dbCache.TableConfig.GetTableName() + " " +
			dbCache.TableConfig.GetOrther()
	} else {
		selectSql = "select " + dbCache.TableConfig.GetColumn() +
			" from " + dbCache.TableConfig.GetTableName() +
			" where " + dbCache.TableConfig.GetWhere() + " " +
			dbCache.TableConfig.GetOrther()
		countSql = "select count(1) from " + dbCache.TableConfig.GetTableName() +
			" where " + dbCache.TableConfig.GetWhere() + " " +
			dbCache.TableConfig.GetOrther()
	}
	//查询总行数
	var count int64
	row := db.QueryRow(countSql)
	err = row.Scan(&count)
	if err != nil {
		if err == sql.ErrNoRows {
			err = fmt.Errorf("InitCache(),Query the total number of rows, the result is empty, err: %s", err)
			return nil, err
		} else {
			err = fmt.Errorf("InitCache(),The query for total rows failed, err: %s", err)
			return nil, err
		}
	}

	//判断用于分页查询的缓存类型.
	if dbCache.TableConfig.GetCacheType() == "slice" || dbCache.TableConfig.GetCacheType() == "sliceNotDel" {
		dbCache.RowCount = count
		dbCache.SliceDbCache = make([]*SliceCache, count, count+count/2)
	}
	// 执行select查询,检索缓存数据
	rows, err := db.Query(selectSql)
	if err != nil {
		err = fmt.Errorf("InitCache(),Failed to retrieve data, err: %s", err)
		return nil, err
	}
	defer rows.Close()
	// 得到列名
	columns, err := rows.Columns()
	if err != nil {
		err = fmt.Errorf("InitCache(),Failed to get column names, err: %s", err)
		return nil, err
	}
	// 得到列类型信息
	types, err := rows.ColumnTypes()
	if err != nil {
		err = fmt.Errorf("InitCache(),Failed to get the column type, err:%s", err)
		return nil, err
	}
	//保存数据库中列的信息
	dbCache.ColumnInfo = make(map[string]*columnInfo, len(columns))
	for _, c := range types {
		name := c.Name()
		precision, scale, isDecimalSize := c.DecimalSize()
		length, isLength := c.Length()
		nullable, isNullable := c.Nullable()

		column := columnInfo{
			columnName:       name,
			scanType:         c.ScanType(),
			databaseTypeName: c.DatabaseTypeName(),
			isDecimalSize:    isDecimalSize,
			precision:        precision,
			scale:            scale,
			isLength:         isLength,
			length:           length,
			isNullable:       isNullable,
			nullable:         nullable,
		}
		dbCache.ColumnInfo[name] = &column
	}

	//每行的数据,每列保存在[]sql.RawBytes字节切片
	values := make([]sql.RawBytes, len(columns))
	//用接口来保存数据。
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	var rowNum int64
	sortColumn := dbCache.TableConfig.GetSortColumn()
	sortMode := dbCache.TableConfig.GetSortMode()
	if sortMode == "" {
		sortMode = "asc"
	}
	// 按行取数据
	for rows.Next() {
		// 从数据中获得行数据,保存在scanArgs的MAP数据中
		err = rows.Scan(scanArgs...)
		if err != nil {
			err = fmt.Errorf("InitCache(),从数据中获得行数据失败, err: %s", err)
			return nil, err
		}
		// 现在正式从读到数据中,开始处理.
		var value string
		//将各列的数据存储在RowMap
		//var RowMap sync.Map
		RowMap := new(sync.Map)
		PkeyValue := ""
		sortColumnValue := ""
		for i, columnValue := range values {
			//columnType := GetColumnType(columns[i])
			//如果数据库类型是字符串
			//switch columnType {
			//case "DATATIME":
			//    //2020-02-15T010:24:45+08:00
			//	string(columnValue)
			//case :
			//default:
			//	value = string(columnValue)
			//}

			// 检查值是否为零（空值）,不为空转换为字符串
			if columnValue == nil {
				value = "NULL"
			} else {
				value = string(columnValue)
			}
			//取出主键值
			if dbCache.TableConfig.GetPkey() == columns[i] {
				PkeyValue = value
			}
			//取出排序列值
			if sortColumn == columns[i] {
				sortColumnValue = value
			}
			RowMap.Store(columns[i], value)
		}
		//主缓存
		dbCache.DbCache.Store(PkeyValue, *RowMap)

		//判断用于分页查询的缓存类型.
		switch dbCache.TableConfig.GetCacheType() {
		case "slice": //数据保存于切片
			SliceData := &SliceCache{
				Pkey:       PkeyValue,
				SortColumn: sortColumnValue,
				SortMode:   sortMode,
				RowMap:     RowMap,
			}
			dbCache.SliceDbCache[rowNum] = SliceData
		case "sliceNotDel": //数据保存于切片,但删除记录未真的删除,只是记录.
			SliceData := &SliceCache{
				Pkey:       PkeyValue,
				SortColumn: sortColumnValue,
				SortMode:   sortMode,
				RowMap:     RowMap,
			}
			dbCache.SliceDbCache[rowNum] = SliceData
			go dbCache.backCheckDelRowRecord() //后台检查删除记录是否达到需要重新初始化
		case "link": //数据保存于链表
			node := &Node{
				rowNum:     rowNum,
				pkey:       PkeyValue,
				sortColumn: sortColumnValue,
				row:        RowMap,
				pre:        nil,
				next:       nil,
			}
			dbCache.LinkDbCache.InsertTail(node)
		}
		rowNum++ //行计数.
	}
	if err = rows.Err(); err != nil {
		err = fmt.Errorf("InitCache(),从数据库中读取数据失败: %s", err)
		return nil, err
	}
	dbCache.RowCount = rowNum
	//判断是实时更新,还是后台异步同步数据库数据.
	if dbCache.TableConfig.GetIsRealtime() == false {
		err := dbCache.dataAsync.InitAsync(db, dbCache.TableConfig.GetTableName())
		if err != nil {
			err = fmt.Errorf("InitAsync(),初始化异步同步数据库失败: %s", err)
			return nil, err
		}
	}

	//判断配置表中,是否指定了排序方式,如果没指定,则按主键升序排序
	switch dbCache.TableConfig.GetCacheType() {
	case "slice", "sliceNotDel": //数据保存于切片
		if dbCache.TableConfig.GetSortMode() == "" { //如果未排序,按升序排序
			dbCache.SliceDbCache = QuickSortGoAsc(dbCache.SliceDbCache)
		}
	}
	//用于rpc和grpc,保存缓存表对象.
	CacheObj[dbCache.TableConfig.GetTableName()] = dbCache
	return dbCache, nil
}

//根据主键,获取该行的数据.
func (d *DBcache) GetRow(Pkey string) (result map[string]string, err error) {
	result = map[string]string{}
	v, ok := d.DbCache.Load(Pkey)
	if ok {
		rowMap := v.(sync.Map)
		rowMap.Range(func(column, value interface{}) bool {
			result[column.(string)] = value.(string)
			return true
		})
	} else {
		err = fmt.Errorf("GetRow(),数据未找到")
		return result, err
	}
	return result, nil
}

//根据主键,获取一列的数据.
func (d *DBcache) GetColumn(Pkey string, column string) (result string, err error) {
	//先得到该行的数据(MAP类型)
	results, err := d.GetRow(Pkey)
	if err != nil {
		err = fmt.Errorf("GetColumn(),数据未找到: %s", err)
		return "", err
	}
	//再根据该行(MAP类型),取得该列的数据.
	result, ok := results[column]
	if !ok {
		err = fmt.Errorf("GetColumn(),数据未找到: %s", err)
		return "", err
	}
	return result, err
}

//根据主键值,删除该行数据.
func (d *DBcache) DelRow(Pkey string) (n int64, err error) {
	//删除数据库对应主键的行
	n, err = d.DelDbRow(Pkey)
	if err != nil {
		err = fmt.Errorf("DelRow(),err : %s", err)
		return 0, err
	}
	//删除缓存
	d.DbCache.Delete(Pkey)

	//删除用于分页缓存中的数据
	switch d.TableConfig.GetCacheType() {
	case "slice": //缓存数据保存于切片
		d.DelSliceDbCache(Pkey)
	case "sliceNotDel": //缓存数据保存于切片,但不真的删除,只记录行号
		d.DelSliceDbCacheRecord(Pkey)
	case "link": //缓存数据保存于链表
		ok := d.LinkDbCache.DeleteNodePkey(Pkey)
		if ok {
			atomic.AddInt64(&d.RowCount, -1)
		}
	}
	return n, err
}

//根据主键值,删除数据库中该行数据.
func (d *DBcache) DelDbRow(key string) (n int64, err error) {
	//删除数据库中对应的行.通过主键查找.
	columnType := d.GetColumnType(key)
	var sqlString string
	if strings.Contains(columnType, "INT") || columnType == "FLOAT" || columnType == "DOUBLE" || columnType == "DECIMAL" {
		sqlString = "DELETE from " + d.TableConfig.GetTableName() + " where " + d.TableConfig.GetPkey() + "=" + key
	} else {
		sqlString = "DELETE from " + d.TableConfig.GetTableName() + " where " + d.TableConfig.GetPkey() + "='" + key + "'"
	}
	//判断是实时更新,还是异步更新
	if d.TableConfig.GetIsRealtime() == true {
		rs, err := d.DbConn.Exec(sqlString)
		if err != nil {
			err = fmt.Errorf("DelDbRow(),删除行数据失败,行主键(%s),err : %s", key, err)
			return 0, err
		}
		n, err = rs.RowsAffected()
		if err != nil {
			err = fmt.Errorf("DelDbRow(),获取受影响的行失败, err : %s", err)
			return 0, err
		}
		return n, err
	} else {
		//异步更新数据库时,是否需要等待返回执行结果.
		if d.TableConfig.GetIsWaitResult() {
			result := make(chan *WaitResult, 1)
			d.dataAsync.sendToAsyncChanResult(true, result, sqlString)
			waitResult := &WaitResult{}
			waitResult = <-result
			if waitResult.err != nil {
				err = fmt.Errorf("DelDbRow(),删除行数据失败,行主键(%s),err : %s", key, err)
				return 0, err
			}
			return n, err
		} else {
			//不返回结果.
			d.dataAsync.sendToAsyncChan(sqlString)
		}
	}
	return 0, nil
}

//缓存类型是slice切片
//(该函数仅于分页查询),删除切片中的数据
func (d *DBcache) DelSliceDbCache(pkeyValue string) {
	//检查当保存删除的行容量,重新初始化SliceDbCache
	if len(d.SliceDbCache) == 0 {
		return
	}
	//根据主键,取出行号.再进行删除.
	n := d.GetRowNum(pkeyValue)
	if n != -1 {
		d.RwMutex.Lock()
		d.SliceDbCache = append(d.SliceDbCache[:n], d.SliceDbCache[n+1:]...)
		d.RwMutex.Unlock()
		atomic.AddInt64(&d.RowCount, -1)
	}
}

//缓存类型是slice切片
//(该函数仅于分页查询),获取用于分页缓存中数据的行号
func (d *DBcache) GetRowNum(pkeyValue string) (i int) {
	pkeyValue = strings.TrimSpace(pkeyValue)
	//如果是按主键列排序,则使用二分查找
	if d.TableConfig.GetSortColumn() == d.TableConfig.GetPkey() {
		switch d.TableConfig.GetSortMode() {
		case "asc":
			return BinarySearchAsc(d.SliceDbCache, pkeyValue)
		case "desc":
			return BinarySearchDesc(d.SliceDbCache, pkeyValue)
		default:
			return BinarySearchAsc(d.SliceDbCache, pkeyValue)
		}
	} else {
		for i, sliceData := range d.SliceDbCache {
			if sliceData.Pkey == pkeyValue {
				return i
			}
		}
	}
	return -1
}

//后台检查当保存删除的行容量,重新初始化SliceDbCache
func (d *DBcache) backCheckDelRowRecord() {
	for {
		time.Sleep(time.Second * 3600) //一小时检查一次.
		hour := time.Now().Hour()
		//晚上1点到5点之间检查和重新初始化
		if hour >= 1 && hour <= 5 {
			if len(d.DelRowNum) > 10000 || len(d.DelRowNum) > len(d.SliceDbCache)/3 {
				d.RwMutex.Lock()
				size := len(d.SliceDbCache)
				d.SliceDbCache = nil
				d.DelRowNum = nil
				d.SliceDbCache = make([]*SliceCache, 0, size)
				d.DelRowNum = make(map[int]bool, size)
				pkey := d.TableConfig.GetPkey()
				sortColumn := d.TableConfig.GetSortColumn()
				sortMode := d.TableConfig.GetSortMode()
				d.DbCache.Range(func(k, v interface{}) bool {
					rowMap := v.(sync.Map)
					pkeyValue := k.(string)
					var sortColumnValue string
					//如果主键是排序列
					if pkey == sortColumn {
						sortColumnValue = pkeyValue
					} else { //如果不是主键为排序列,则还要在rowMap中取排序列的值
						Value, ok := rowMap.Load(sortColumn)
						if ok {
							sortColumnValue = Value.(string)
						} else {
							sortColumnValue = pkeyValue
						}
					}
					SliceData := &SliceCache{
						Pkey:       pkeyValue,
						SortColumn: sortColumnValue,
						SortMode:   sortMode,
						RowMap:     &rowMap,
					}
					d.SliceDbCache = append(d.SliceDbCache, SliceData)
					return true
				})
				//排序.
				switch sortMode{
				case "asc":
					d.SliceDbCache = QuickSortGoAsc(d.SliceDbCache)
				case "desc":
					d.SliceDbCache = QuickSortGoDesc(d.SliceDbCache)
				default:
					d.SliceDbCache = QuickSortGoAsc(d.SliceDbCache)
				}
				d.RwMutex.Unlock()
				runtime.GC()
				debug.FreeOSMemory()
			}
		}
	}
}

//缓存类型是sliceNotDel切片(不删除,只记录)
// (该函数仅于分页查询),当有删除行时,只是把删除的行号保存.未进行切片的删除,因为切片的删除会影响性能.
func (d *DBcache) DelSliceDbCacheRecord(pkeyValue string) {
	if len(d.SliceDbCache) == 0 {
		return
	}

	n := d.GetRowNumRecord(pkeyValue)
	if n != -1 {
		d.RwMutex.RLock()
		d.DelRowNum[n] = true
		d.RwMutex.RUnlock()
		atomic.AddInt64(&d.RowCount, -1)
	}
}

//缓存类型是sliceNotDel切片(不删除,只记录)
//(该函数仅于分页查询),获取用于分页缓存中数据的行号
func (d *DBcache) GetRowNumRecord(pkeyValue string) (i int) {
	pkeyValue = strings.TrimSpace(pkeyValue)
	i = -1
	//如果是按主键列排序,则使用二分查找
	if d.TableConfig.GetSortColumn() == d.TableConfig.GetPkey() {
		switch d.TableConfig.GetSortMode() {
		case "asc":
			i = BinarySearchAsc(d.SliceDbCache, pkeyValue)
		case "desc":
			i = BinarySearchDesc(d.SliceDbCache, pkeyValue)
		default:
			i = BinarySearchAsc(d.SliceDbCache, pkeyValue)
		}
		//找到,还要在保存删除行号记录中查找
		if i != -1 {
			if d.DelRowNum[i] {
				return -1
			} else {
				return i
			}
		}
	} else {
		for i, sliceData := range d.SliceDbCache {
			if d.DelRowNum[i] {
				continue
			}
			if sliceData.Pkey == pkeyValue {
				return i
			}
		}
	}
	return -1
}

func (d *DBcache) GetRowNumRecord2(pkeyValue string) (i int) {
	i = d.GetRowNum(pkeyValue)
	if i == -1 {
		return -1
	} else {
		//找到,还要在保存删除行号记录中查找
		if d.DelRowNum[i] {
			return -1
		} else {
			return i
		}
	}
	return -1
}

//获取列在数据库中的类型.
func (d *DBcache) GetColumnType(column string) (columnType string) {
	col, ok := d.ColumnInfo[column]
	if ok {
		return col.databaseTypeName
	}
	return ""
}

//根据where条件中,and和or获得条件表达式
func (d *DBcache) GetWhereCondition(where string, isAnd, isOr bool) (whereCondition [][]string, err error) {
	//如果where是and组合条件
	if isAnd == true && isOr == false {
		whereCondition, err = d.GetCondition(where, "and")
		if err != nil {
			err = fmt.Errorf("GetWhere(),条件错误: %s ,err : %s", where, err)
			return nil, err
		}
	}

	//如果where是or组合条件
	if isAnd == false && isOr == true {
		whereCondition, err = d.GetCondition(where, "or")
		if err != nil {
			err = fmt.Errorf("GetWhere(),条件错误: %s ,err : %s", where, err)
			return nil, err
		}
	}

	//如果没and和or,只是单条件表达式
	if isAnd == false && isOr == false {
		whereCondition, err = d.GetCondition(where, "=")
		if err != nil {
			err = fmt.Errorf("GetWhere(),条件错误: %s , err : %s", where, err)
			return nil, err
		}
	}
	return
}

//根据where条件表达式,检查表达式是否成立,并得到数据.
func (d *DBcache) GetWhereValue(isAnd, isOr bool, whereCondition [][]string, rowMap *sync.Map, result *[]map[string]string) {
	row := map[string]string{}
	//从条件表达式中,获取各个表达式.并判断各表达式是否成立.
	for i, condition := range whereCondition {
		//根据条件表式中,column中的值,在该行缓存中查找该值.再做比较(=或!=)判断
		keyValue, ok := rowMap.Load(condition[0])
		if ok {
			//当条件表达式中是=时.
			if condition[1] == "=" {
				//判断条件表达中的值与缓存是否相等
				if condition[2] == keyValue.(string) {
					//相等,则在条件表达中,设置条件成立
					whereCondition[i][3] = "true"
				}
				//当条件表达式中是!=时.
			} else if condition[1] == "!=" {
				//判断条件表达中的值与缓存是否不相等
				if condition[2] != keyValue.(string) {
					//不相等,则在条件表达中,设置条件成立
					whereCondition[i][3] = "true"
				}
			}
		}
	}

	switch {
	//当条件表达式是and时.如果有一个条件不成立,则整个表达式不成立.不成立则true
	case isAnd == true && isOr == false:
		isMatch := false
		for i, condition := range whereCondition {
			if condition[3] == "false" {
				isMatch = true
			} else {
				whereCondition[i][3] = "false"
			}
		}
		//条件表达中只要有一个条件(不成立)则执行以下.
		if isMatch == false {
			//从该行数据(sny.map),保存在*row(map)中.
			rowMap.Range(func(k, v interface{}) bool {
				//(*row)[k.(string)] = v.(string)
				(row)[k.(string)] = v.(string)
				return true
			})
			//再把该行数据追加到结果切片
			*result = append(*result, row)
		}

	//当条件表达式是or时.只要有一个为真.则条件成立
	case isAnd == false && isOr == true:
		isMatch := false
		for i, condition := range whereCondition {
			if condition[3] == "true" {
				isMatch = true
				whereCondition[i][3] = "false"
			}
		}
		//条件表达中只要有一个条件(成立)则执行以下.
		if isMatch == true {
			rowMap.Range(func(k, v interface{}) bool {
				//(*row)[k.(string)] = v.(string)
				(row)[k.(string)] = v.(string)
				return true
			})
			*result = append(*result, row)
		}

	//当条件是=时.无and或or
	case isAnd == false && isOr == false:
		isMatch := false
		for i, condition := range whereCondition {
			if condition[3] == "true" {
				isMatch = true
				whereCondition[i][3] = "false"
			}
		}
		//判断是否条件成立
		if isMatch == true {
			rowMap.Range(func(k, v interface{}) bool {
				//(*row)[k.(string)] = v.(string)
				(row)[k.(string)] = v.(string)
				return true
			})
			*result = append(*result, row)
		}
	}
}

//根据where条件,获取多行数据.
func (d *DBcache) GetWhere(where string) (result []map[string]string, err error) {
	//存储各条件表达式
	whereCondition := [][]string{}

	where = strings.TrimSpace(where)
	if len(where) == 0 {
		return nil, fmt.Errorf("GetWhere(),where条件不能为空")
	}

	//检查是否包含and 或 or
	where = strings.ToLower(where)
	isAnd := strings.Contains(where, " and ")
	isOr := strings.Contains(where, " or ")
	//根据and 或 or得到表达式
	whereCondition, err = d.GetWhereCondition(where, isAnd, isOr)
	if err != nil {
		return nil, fmt.Errorf("GetWhere(), err: %s", err)
	}

	//从sync.map中取得每行数据,存储于result(MAP类型)
	d.DbCache.Range(func(k, v interface{}) bool {
		rowMap := v.(sync.Map)
		d.GetWhereValue(isAnd, isOr, whereCondition, &rowMap, &result)
		return true
	})
	return result, err
}

//获取and或or二边的条件表达式
func (d *DBcache) GetCondition(where string, operator string) (whereCondition [][]string, err error) {
	slices := make([]string, 0)
	whereCondition = [][]string{}
	switch {
	case operator == "=":
		//=表示,不包括and或or或逗号其它,只有一个key,value
		slices = append(slices, where)
	case operator == "and" || operator == "or":
		//分割and或or二边的条件表达式
		slices = d.SplitString(where, operator)
	case operator == ",":
		//分割以,分割条件的表达式
		slices = d.SplitString(where, operator)
	default:
		//以其它分割条件的表达式
		slices = d.SplitString(where, operator)
	}

	//再将每个条件表达式分解,例如user=xiaoming 分解成user,=,xiaoming,false
	//最后加的false,是在为每行,判断条件表达中的数据与缓存中,是不是相等或不相等,则判断该条件表达是否成立
	for _, condition := range slices {
		//如果条件是!=(不等于)
		if i := strings.Index(condition, "!="); i != -1 {
			result, err := d.SplitCondition(condition, "!=")
			if err != nil {
				err = fmt.Errorf("GetCondition(),条件错误: %s ,err: %s", condition, err)
				return nil, err
			}
			whereCondition = append(whereCondition, result)
			//如果条件是=(等于)
		} else if i := strings.Index(condition, "="); i != -1 {
			result, err := d.SplitCondition(condition, "=")
			if err != nil {
				err = fmt.Errorf("GetCondition(),条件错误: %s ,err: %s", condition, err)
				return nil, err
			}
			whereCondition = append(whereCondition, result)
		}
	}
	return whereCondition, err
}

//分解字符串保存于切片中,去除两边的空格.
func (d *DBcache) SplitString(str string, operator string) (slices []string) {
	slices = strings.Split(str, operator)
	for i, _ := range slices {
		slices[i] = strings.TrimSpace(slices[i])
	}
	return slices
}

//分割表达式
//以(and,or,逗号等分割)分解条件表达式,保存于切片,例如:user=xiaoming,pwd=1345534
//分解为每个单独的字符串,例:user=xiaoming,分解成三个,user,=,xiaoming
func (d *DBcache) SplitCondition(str string, operator string) (result []string, err error) {
	if i := strings.Index(str, operator); i != -1 {
		column := str[:i]
		value := str[i+1:]
		column = strings.TrimSpace(column)
		value = strings.TrimSpace(value)
		operator = strings.TrimSpace(operator)
		result = append(result, column)
		result = append(result, operator)
		result = append(result, value)
		result = append(result, "false")
		return result, nil
	} else {
		err = fmt.Errorf("SplitCondition(),字符串:%s 按(%s)分割失败.", str, operator)
		return nil, err
	}
}

//根据主键,更新一列的数据.
func (d *DBcache) UpdateColumn(Pkey string, column string, value string) (n int64, err error) {
	Columns := d.TableConfig.GetColumns()
	isExist := false
	for _, v := range Columns {
		if v == column {
			isExist = true
			break
		}
	}
	if isExist == false {
		err = fmt.Errorf("UpdateColumn(),该列未缓存.主键: %s,列名: %s", Pkey, column)
		return 0, err
	}

	v, ok := d.DbCache.Load(Pkey)
	if ok {
		//更新数据库
		i, err := d.UpdateDbcolumn(Pkey, column, value)
		if err != nil {
			err = fmt.Errorf("UpdateColumn():数据库列更新失败,%s", err)
			return 0, err
		}
		//更新缓存
		rowMap := v.(sync.Map)
		rowMap.Store(column, value)
		return i, nil
	} else {
		err = fmt.Errorf("UpdateColumn(),数据未找到,主键: %s ", Pkey)
		return 0, err
	}
	return 0, nil
}

//根据主键,更新数据库中一列.
func (d *DBcache) UpdateDbcolumn(Pkey string, column string, value string) (n int64, err error) {
	columnType := d.GetColumnType(column)
	if columnType == "" {
		columnType = "VARCHAR"
	}
	var sqlString string
	//如果在数据库中,列类型是整型,浮点型
	if strings.Contains(columnType, "INT") || columnType == "FLOAT" || columnType == "DOUBLE" || columnType == "DECIMAL" {
		sqlString = "UPDATE " + d.TableConfig.GetTableName() + " SET " + column + "=" + value + " WHERE " + d.TableConfig.GetPkey() + "='" + Pkey + "'"
	} else {
		sqlString = "UPDATE " + d.TableConfig.GetTableName() + " SET " + column + "='" + value + "' WHERE " + d.TableConfig.GetPkey() + "='" + Pkey + "'"
	}
	//判断是实时更新,还是异步更新
	if d.TableConfig.GetIsRealtime() == true {
		rs, err := d.DbConn.Exec(sqlString)
		if err != nil {
			err = fmt.Errorf("UpdateDbcolumn(),更新行数据失败,主键: %s 列名: %s 列值: %s ", Pkey, column, value)
			return 0, err
		}
		n, err = rs.RowsAffected()
		if err != nil {
			err = fmt.Errorf("UpdateDbcolumn(),获取受影响的行失败. err : %s", err)
			return 0, err
		}
		return n, err
	} else {
		//异步更新数据库时,是否需要等待返回执行结果.
		if d.TableConfig.GetIsWaitResult() {
			result := make(chan *WaitResult, 1)
			d.dataAsync.sendToAsyncChanResult(true, result, sqlString)
			waitResult := &WaitResult{}
			waitResult = <-result
			if waitResult.err != nil {
				err = fmt.Errorf("UpdateDbcolumn(),更新行数据失败,主键: %s 列名: %s 列值: %s ", Pkey, column, value)
				return 0, err
			}
			return waitResult.n, err
		} else {
			d.dataAsync.sendToAsyncChan(sqlString)
		}
	}
	return 0, nil
}

//根据主键,更新多列数据.
func (d *DBcache) UpdateColumns(Pkey string, where string) (n int64, err error) {
	whereCondition, err := d.GetCondition(where, ",")
	if err != nil {
		err = fmt.Errorf("UpdateColumns(),条件错误: %s. err: %s", where, err)
		return 0, err
	}
	columns := d.TableConfig.GetColumns()
	//检查该列是否存在于缓存表中.
	isExist := false
	for _, condition := range whereCondition {
		for _, column := range columns {
			if condition[0] == column {
				isExist = true
				break
			}
		}
	}
	if isExist == false {
		err = fmt.Errorf("UpdateColumns(),where条件中有列未缓存,%s", where)
		return 0, err
	}
	//更新缓存
	v, ok := d.DbCache.Load(Pkey)
	if ok {
		rowMap := v.(sync.Map)
		//更新数据库
		n, err = d.UpdateDbcolumns(Pkey, where)
		if err != nil {
			err = fmt.Errorf("UpdateColumns(), err:%s", err)
			return 0, err
		}

		//在sync.Map中更新值.
		for _, condition := range whereCondition {
			rowMap.Store(condition[0], condition[2])
		}

		return n, nil
	} else {
		err = fmt.Errorf("UpdateColumns(),数据未找到,主键: %s,条件: %s", Pkey, where)
		return 0, err
	}
	return 0, nil
}

//根据主键,更新数据库中多列.
func (d *DBcache) UpdateDbcolumns(Pkey string, condition string) (n int64, err error) {

	SqlStr := d.GetSqlStr(condition)
	sqlString := "UPDATE " + d.TableConfig.GetTableName() + " SET " + SqlStr + " WHERE " + d.TableConfig.GetPkey() + "='" + Pkey + "'"
	if d.TableConfig.GetIsRealtime() == true {
		rs, err := d.DbConn.Exec(sqlString)
		if err != nil {
			err = fmt.Errorf("UpdateDbcolumns(),更新行数据失败,行主键: %s, err: %s", Pkey, err)
			return 0, err
		}
		n, err = rs.RowsAffected()
		if err != nil {
			err = fmt.Errorf("UpdateDbcolumns(),获取受影响的行失败. err: %s", err)
			return 0, err
		}
		return n, err
	} else {
		//异步更新数据库时,是否需要等待返回执行结果.
		if d.TableConfig.GetIsWaitResult() {
			result := make(chan *WaitResult, 1)
			d.dataAsync.sendToAsyncChanResult(true, result, sqlString)
			waitResult := &WaitResult{}
			waitResult = <-result
			if waitResult.err != nil {
				err = fmt.Errorf("UpdateDbcolumns(),更新行数据失败,行主键: %s, err: %s", Pkey, err)
				return 0, err
			}
			return n, err
		} else {
			d.dataAsync.sendToAsyncChan(sqlString)
		}
	}
	return 0, nil
}

//根据表达式,得到SQL语句的字符串.
func (d *DBcache) GetSqlStr(condition string) (SqlStr string) {
	whereCondition, err := d.GetCondition(condition, ",")
	if err != nil {
		logs.Error("a", "GetSqlStr(),条件错误: %s, err: %s", condition, err)
		return ""
	}
	var columns string

	for _, condition := range whereCondition {
		//判断列是否是INT,FLOAT,DOUBLE,DECIMAL.
		columnType := d.GetColumnType(condition[0])
		if strings.Contains(columnType, "INT") || columnType == "FLOAT" || columnType == "DOUBLE" || columnType == "DECIMAL" {
			columns = columns + condition[0] + "=" + condition[2] + ","
		} else {
			columns = columns + condition[0] + "='" + condition[2] + "',"
		}
	}
	SqlStr = columns[:len(columns)-1]
	return SqlStr
}

//判断是否存在主键.
func (d *DBcache) isExistPkey(condition string) (isTrue bool) {
	whereCondition, err := d.GetCondition(condition, ",")
	if err != nil {
		logs.Info("a", "isExistPkey(),条件错误: %s, err: %s", condition, err)
		return false
	}
	Pkey := d.TableConfig.GetPkey()
	isTrue = false
	for _, condition := range whereCondition {
		if condition[0] == Pkey {
			isTrue = true
		}
	}
	return isTrue
}

//插入一行数据.
func (d *DBcache) InsertRow(condition string) (n int64, err error) {
	rowMap := new(sync.Map)
	sortColumn := d.TableConfig.GetSortColumn()
	sortMode := d.TableConfig.GetSortMode()
	whereCondition, err := d.GetCondition(condition, ",")
	if err != nil {
		err = fmt.Errorf("InsertRow(),获取条件错误. err: %v", err)
		return 0, err
	}
	//判断插入一行数据中，有没有主键．这只是需要主键．
	//如果是自增列，则不需要主键．
	Pkey := d.TableConfig.GetPkey()
	var PkeyValue string
	var sortColumnValue string
	isPkey := false
	for _, condition := range whereCondition {
		//判断该列在数据库中是否可为空
		colInfo, ok := d.ColumnInfo[condition[0]]
		if ok {
			if colInfo.isNullable == true && colInfo.nullable == false {
				if strings.TrimSpace(condition[2]) == "" {
					err = fmt.Errorf("InsertRow(),该列%s不能为空. err: %v", condition[0], err)
					return 0, err
				}
			}
		}

		//判断是否有主键
		if condition[0] == Pkey {
			PkeyValue = condition[2]
			isPkey = true
		}
		//取排序列的值
		if condition[0] == sortColumn {
			sortColumnValue = condition[2]
		}

		//将插入的行数据保存于map中
		rowMap.Store(condition[0], condition[2])
	}
	//不是自增列,必须要有主键.自增列可以不要
	if d.TableConfig.PkeyIsIncrement() == false && isPkey != true {
		err = fmt.Errorf("InsertRow(),插入行中,没有主键.条件: %s, 主键: %s, err: %v", condition, Pkey, err)
		return 0, err
	}
	//插入数据库
	i, err := d.InsertDbRow(condition)
	if err != nil {
		return 0, err
	}
	//插入缓存
	d.DbCache.Store(PkeyValue, *rowMap)

	//插入用于分页查询的缓存
	switch d.TableConfig.GetCacheType() {
	case "slice": //数据保存于切片.
		//插入用于分页查询缓存
		d.RwMutex.Lock()
		SliceData := &SliceCache{
			Pkey:       PkeyValue,
			SortColumn: sortColumnValue,
			SortMode:   sortMode,
			RowMap:     rowMap,
		}
		index := -1
		switch d.TableConfig.GetSortMode() {
		case "asc":
			index = BinarySearchAsc(d.SliceDbCache, sortColumnValue)
			if index == -1 {
				//当是升序排序时,如果没找到,判断是不是比第1个还小,如果还小,则插入到最前面
				if sortColumnValue < d.SliceDbCache[0].SortColumn {
					d.SliceDbCache = append(d.SliceDbCache[:0], append([]*SliceCache{SliceData}, d.SliceDbCache[0:]...)...)
				} else {
					d.SliceDbCache = append(d.SliceDbCache, SliceData)
				}
			} else {
				d.SliceDbCache = append(d.SliceDbCache[:n], append([]*SliceCache{SliceData}, d.SliceDbCache[n:]...)...)
			}
		case "desc":
			index = BinarySearchDesc(d.SliceDbCache, sortColumnValue)
			if index == -1 {
				//当是降序排序时,如果没找到,判断是不是比第1个还大,如果还大,则插入到最前面
				if sortColumnValue > d.SliceDbCache[0].SortColumn {
					d.SliceDbCache = append(d.SliceDbCache[:0], append([]*SliceCache{SliceData}, d.SliceDbCache[0:]...)...)
				} else {
					d.SliceDbCache = append(d.SliceDbCache, SliceData)
				}
			} else {
				d.SliceDbCache = append(d.SliceDbCache[:index], append([]*SliceCache{SliceData}, d.SliceDbCache[index:]...)...)
			}
		default:
			//如果未排序,则追加到最后.
			d.SliceDbCache = append(d.SliceDbCache, SliceData)
		}
		d.RwMutex.Unlock()
		atomic.AddInt64(&d.RowCount, 1)
	case "sliceNotDel": //数据保存于切片,但不删除
		//插入用于分页查询缓存
		d.RwMutex.Lock()
		SliceData := &SliceCache{
			Pkey:       PkeyValue,
			SortColumn: sortColumnValue,
			SortMode:   sortMode,
			RowMap:     rowMap,
		}
		//只是追加,保证切片的行号不变.
		d.SliceDbCache = append(d.SliceDbCache, SliceData)
		d.RwMutex.Unlock()
		atomic.AddInt64(&d.RowCount, 1)
	case "link": //数据保存于链表
		node := &Node{
			rowNum:     d.LinkDbCache.length + 1,
			pkey:       PkeyValue,
			sortColumn: sortColumnValue,
			row:        rowMap,
			pre:        nil,
			next:       nil,
		}
		switch d.TableConfig.GetSortMode() {
		case "asc":
			d.LinkDbCache.InsertNodeAsc(node)
		case "desc":
			d.LinkDbCache.InsertNodeDesc(node)
		default: //插入到最后.
			d.LinkDbCache.InsertTail(node)
		}
		atomic.AddInt64(&d.RowCount, 1)
	}
	return i, nil
}

//插入一行数据到数据库.
func (d *DBcache) InsertDbRow(condition string) (n int64, err error) {
	SqlStr := d.GetSqlStr(condition)
	sqlString := "INSERT INTO " + d.TableConfig.GetTableName() + " SET " + SqlStr
	if d.TableConfig.GetIsRealtime() == true {
		rs, err := d.DbConn.Exec(sqlString)
		if err != nil {
			err = fmt.Errorf("InsertDbRow(),插入行到数据库失败.语句:%s, err: %v", sqlString, err)
			return 0, err
		}
		n, err = rs.RowsAffected()
		if err != nil {
			err = fmt.Errorf("InsertDbRow(),获取受影响的行失败. err: %v", err)
			return 0, err
		}
		return n, err
	} else {
		//异步更新数据库时,是否需要等待返回执行结果.
		if d.TableConfig.GetIsWaitResult() {
			result := make(chan *WaitResult, 1)
			d.dataAsync.sendToAsyncChanResult(true, result, sqlString)
			waitResult := &WaitResult{}
			waitResult = <-result
			if waitResult.err != nil {
				err = fmt.Errorf("InsertDbRow(),插入行到数据库失败.语句:%s, err: %v", sqlString, waitResult.err)
				return 0, err
			}
			return waitResult.n, err
		} else {
			d.dataAsync.sendToAsyncChan(sqlString)
		}
	}
	return 0, nil
}

//(该函数仅于分页显示,提取数据)从缓存中,获取指定的行,开始行-结束行.(不包括结束行)并不是与数据库中行号一致.
//因为从数据库中检索数据时,数据先后不一定.这只是缓存的行号.目的是一样.不影响使用.
func (d *DBcache) GetRowBetween(start int, end int) (result []map[string]string) {
	switch d.TableConfig.GetCacheType() {
	case "slice": //数据保存于slice切片.
		if start > len(d.SliceDbCache) {
			start = len(d.SliceDbCache) - 1
		}
		if end > len(d.SliceDbCache) {
			end = len(d.SliceDbCache)
		}
		for i := start; i < end; i++ {
			row := make(map[string]string, end-start)
			rowMap := *d.SliceDbCache[i].RowMap
			rowMap.Range(func(column, value interface{}) bool {
				row[column.(string)] = value.(string)
				return true
			})
			result = append(result, row)
		}

	case "sliceNotDel": //数据保存于sliceNotDel切片(不删除,只记录)
		if start > len(d.SliceDbCache) {
			start = len(d.SliceDbCache) - 1
		}
		if end > len(d.SliceDbCache) {
			end = len(d.SliceDbCache)
		}
		//获取start到end之间删除的行
		var delRowCount int
		for k, _ := range d.DelRowNum {
			if k >= start && k < end {
				delRowCount++
			}
		}

		for i := start; i < end+delRowCount; i++ {
			if d.DelRowNum[i] {
				continue
			}

			row := make(map[string]string, end-start)
			rowMap := *d.SliceDbCache[i].RowMap
			rowMap.Range(func(column, value interface{}) bool {
				row[column.(string)] = value.(string)
				return true
			})
			result = append(result, row)
		}
	case "link": //数据保存于链表
		startInt64 := int64(start)
		endInt64 := int64(end)
		length := d.LinkDbCache.length
		if startInt64 > length {
			startInt64 = length
		}
		if endInt64 > length {
			endInt64 = length
		}
		nodes := d.LinkDbCache.GetNodeBetween(startInt64, endInt64)
		for _, node := range nodes {
			row := make(map[string]string, len(nodes))
			rowMap := node.row
			rowMap.Range(func(column, value interface{}) bool {
				row[column.(string)] = value.(string)
				return true
			})
			result = append(result, row)
		}
	}

	return result
}
//用于分页,获取总页数.pageSize参数是每页行数大小
func (d *DBcache) GetPageCount(pageSize int) (result int) {
	if pageSize <= 0{
		return 0
	}
	result = int(math.Ceil(float64(d.RowCount) / float64(pageSize)))
	return result
}

//用于分页,根据指定开始页,获取多少页,每页行数.返回数据.参数说明:startPage,开始页,pageNum多少页,pageSize参数是每页行数大小
func (d *DBcache) GetMultipageRows(startPage int,pageNum int,pageSize int) (result []map[string]string) {
	if pageSize <= 0{
		return nil
	}
	//获取总页数
	pageCount := d.GetPageCount(pageSize)
	if startPage <= 0 {
		startPage = 1
	}
	if startPage > pageCount {
		startPage = pageCount
	}
	var startRow,endRow int
	if startPage==1 {
		startRow=0
		endRow=pageNum*pageSize -1
	}else{
		startRow =(startPage-1)*pageSize -1
		endRow =(startPage+pageNum)*pageSize  -1
	}
	result = d.GetRowBetween(startRow, endRow)
	return result
}
//用于分页,根据页码和每页行数大小,返回数据.参数说明:page参数是页码,pageSize参数是每页行数大小
func (d *DBcache) GetOnePageRows(page int,pageSize int) (result []map[string]string) {
	if pageSize <= 0{
		return nil
	}
	//获取总页数
	pageCount := d.GetPageCount(pageSize)
	if page <= 0 {
		page = 1
	}
	if page > pageCount {
		page = pageCount
	}

	var startRow,endRow int
	if page==1 {
		startRow=0
		endRow=pageSize
	}else{
		startRow =(page-1)*pageSize -1
		endRow =page*pageSize  -1
	}
	result = d.GetRowBetween(startRow, endRow)
	return result
}

//关闭打开的对象
func (d *DBcache) Close() {
	//关闭异步同步文件对象.
	d.dataAsync.Close()
}
