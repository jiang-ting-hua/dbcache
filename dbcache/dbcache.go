package dbcache

import (
	"database/sql"
	"dbcache/conf"
	"dbcache/logs"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"
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

//异步更新数据库
type AsyncSql struct {
	exeSql    string //异步数据更新SQL语句
	timestamp string //执行语句的时间
	isFinish  bool   //是否完成.
}

var (
	DataSyncConf       = conf.DataSync{}      //数据库数据同步.是实时还是异步.
	AsyncSqlchan       chan *AsyncSql         //异步数据库同步管道
	TableConfig        = conf.Table{}         //缓存用户表信息
	DbCache            = sync.Map{}           //用来缓存的表数据
	RowNumDbCache      []*sync.Map            //用来根据行号查询缓存,用于页面分页显示.
	delRowNum          = map[int]bool{}       //保存RowNumDbCache中已删除行的行号,当有删除行时,只是把删除的行号保存.未进行切片的删除,因为切片的删除会影响性能.
	mutex              sync.RWMutex           //读写锁
	DbConfig           = conf.DbConfig{}      //配置文件中db配置信息
	DbConn             *sql.DB                //数据库对象
	ColumnInfo         map[string]*columnInfo //数据库缓存表中列的信息
	AsyncFileObj       *os.File               //异步数据库同步,保存需要更新的SQL语句文件对象.
	AsyncFailedFileObj *os.File               //异步数据库同步,保存失败的需要更新的SQL语句文件对象.
)

//关闭打开的对象
func Close() {
	DbConn.Close()
	AsyncFileObj.Close()
	AsyncFailedFileObj.Close()
}

//连接数据库
func ConnectDB() (db *sql.DB, err error) {
	//读取数据库登录配置文件
	err = conf.ParseConf(conf.CONFIG_FILE, &DbConfig)
	if err != nil {
		err = fmt.Errorf("ConnectDB(),数据库读取配置文件失败: %s", err)
		return nil, err
	}

	dataSource := DbConfig.User + ":" + DbConfig.Pwd + "@tcp(" + DbConfig.Ip + ":" + DbConfig.Port + ")/" + DbConfig.DatabaseName + "?" + DbConfig.ConnectInfo
	DbConn, err = sql.Open("mysql", dataSource)
	if err != nil {
		err = fmt.Errorf("ConnectDB(),数据库连接失败: %s", err)
		return nil, err
	}
	return DbConn, err
}

//检查文件大小
func checkFileSize(file *os.File) bool {
	fileInfo, err := file.Stat()
	if err != nil {
		err = fmt.Errorf("get async Sql file info failed, file name:%s, err:%v\n", file.Name(), err)
		return false
	}
	//如果文件大于等于异步保存SQL文件最大值,返回真.
	return fileInfo.Size() >= DataSyncConf.MaxAsyncFileSize*1024*1024
}

//分割文件
func splitFile(file *os.File) (newFile *os.File, err error) {
	info, err := file.Stat()
	if err != nil {
		err = fmt.Errorf("get async Sql file info failed, err:%v\n", err)
		return nil, err
	}
	oldFileName := info.Name()
	currentTime := time.Now().Format("20060102_150405")
	logFileName := path.Join(DataSyncConf.AsyncFilePath, oldFileName)
	newPath := fmt.Sprintf("%s_%s.bak", logFileName, currentTime)

	file.Close()

	os.Rename(logFileName, newPath)
	fileObj, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		err = fmt.Errorf("again open Async sql file failed, file name:%s, err:%v\n", logFileName, err)
		return nil, err
	}
	return fileObj, nil
}

//初始化异步同步信息.
func InitAsync() (err error) {
	//初始化文件对象
	AsyncFileObj, err = os.OpenFile(DataSyncConf.AsyncFilePath+DataSyncConf.AsyncFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		err = fmt.Errorf("open async sql file failed, file name:%s, err:%v\n", DataSyncConf.AsyncFilePath+DataSyncConf.AsyncFileName, err)
		return err
	}
	AsyncFailedFileObj, err = os.OpenFile(DataSyncConf.AsyncFilePath+DataSyncConf.AsyncFailedFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		err = fmt.Errorf("open async sql failed file failed, file name:%s, err:%v\n", DataSyncConf.AsyncFilePath+DataSyncConf.AsyncFailedFileName, err)
		return err
	}
	//初始化管道.
	AsyncSqlchan = make(chan *AsyncSql, DataSyncConf.AsyncMaxChan)
	//后台异步同步数据
	go backSyncSql()

	return nil
}

//发送要执行的sql语句到管道.
func sendToAsyncChan(exeSql string) {
	sqlTmp := &AsyncSql{
		exeSql:    exeSql,
		timestamp: time.Now().Format("2006-01-02 15-04-05"),
		isFinish:  false,
	}
	select {
	case AsyncSqlchan <- sqlTmp:
	default:
		fmt.Println("Async sql output File,channel blocked")
	}
}

//后台同步数据库
func backSyncSql() {
	for {
		//检查文件容量大小
		if checkFileSize(AsyncFileObj) {
			newFile, err := splitFile(AsyncFileObj)
			if err != nil {
				logs.Error("a", "backSyncSql(),splitFile() faild. err: %s", err)
			}
			AsyncFileObj = newFile
		}
		if checkFileSize(AsyncFailedFileObj) {
			newFile, err := splitFile(AsyncFailedFileObj)
			if err != nil {
				logs.Error("a", "backSyncSql(),splitFile() faild. err: %s", err)
			}
			AsyncFailedFileObj = newFile
		}

		//从管道中取出要执行的sql
		sqlTmp := &AsyncSql{}
		select {
		case sqlTmp = <-AsyncSqlchan:
		default:
			time.Sleep(time.Millisecond * 500)
			continue
		}

		if sqlTmp.isFinish == true {
			continue
		}

		sqlMsg := fmt.Sprintf("/* %s */  %s;\n", sqlTmp.timestamp, sqlTmp.exeSql)
		fmt.Fprintf(AsyncFileObj, sqlMsg)

		_, err := DbConn.Exec(sqlTmp.exeSql)
		if err != nil {
			time.Sleep(time.Millisecond * 500)
			_, err := DbConn.Exec(sqlTmp.exeSql)
			if err != nil {
				sqlMsg := fmt.Sprintf("/* [%s][%s] */  %s;\n", sqlTmp.timestamp, err, sqlTmp.exeSql)
				fmt.Fprintf(AsyncFailedFileObj, sqlMsg)
				logs.Error("a", "backSyncSql(),DbConn.Exec(%s) faild. err: %s", sqlTmp.exeSql, err)
				sqlTmp.isFinish = false
			}
		}
		sqlTmp.isFinish = true
	}
}

//初始化缓存信息
func InitCache(db *sql.DB) (rowNum int, err error) {
	//读取配置文件,数据库异步同步数据的信息.
	err = conf.ParseConf(conf.TABLES_CONF, &DataSyncConf)
	if err != nil {
		return 0, err
	}
	//读取配置文件,需要缓存表的信息.
	err = conf.ParseConf(conf.TABLES_CONF, &TableConfig)
	if err != nil {
		return 0, err
	}
	var selectSql string
	var countSql string
	if TableConfig.GetWhere() == "" {
		selectSql = "select " + TableConfig.GetColumn() + " from " + TableConfig.GetTableName()
		countSql = "select count(1) from " + TableConfig.GetTableName()
	} else {
		selectSql = "select " + TableConfig.GetColumn() + " from " + TableConfig.GetTableName() + " where " + TableConfig.GetWhere()
		countSql = "select count(1) from " + TableConfig.GetTableName() + " where " + TableConfig.GetWhere()
	}

	var count int64
	row := db.QueryRow(countSql)
	err = row.Scan(&count)
	if err != nil {
		if err == sql.ErrNoRows {
			err = fmt.Errorf("InitCache(),从数据库中查询总行数,查询的结果为空: %s", err)
			return 0, err
		} else {
			err = fmt.Errorf("InitCache(),从数据库中查询总行数失败: %s", err)
			return 0, err
		}
	}
	RowNumDbCache = make([]*sync.Map, count, count*2)

	rows, err := db.Query(selectSql)
	if err != nil {
		err = fmt.Errorf("InitCache(),执行select查询,检索数据时失败: %s", err)
		return 0, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		err = fmt.Errorf("InitCache(),从数据库中得到列名时失败: %s", err)
		return 0, err
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		err = fmt.Errorf("InitCache(),从数据库中得到列类型时失败: %s", err)
		return 0, err
	}

	ColumnInfo = make(map[string]*columnInfo, len(columns))
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
		ColumnInfo[name] = &column
	}


	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			err = fmt.Errorf("InitCache(),从数据中获得行数据失败, err: %s", err)
			return 0, err
		}
		var value string
		//var RowMap sync.Map
		RowMap := new(sync.Map)
		PkeyValue := ""
		for i, columnValue := range values {
			// 检查值是否为零（空值）,不为空转换为字符串
			if columnValue == nil {
				value = "NULL"
			} else {
				value = string(columnValue)
			}
			//取出主键值
			if TableConfig.GetPkey() == columns[i] {
				PkeyValue = value
			}

			RowMap.Store(columns[i], value)
		}

		DbCache.Store(PkeyValue, *RowMap)
		RowNumDbCache[rowNum] = RowMap
		rowNum++
	}
	if err = rows.Err(); err != nil {
		err = fmt.Errorf("InitCache(),从数据库中读取行失败: %s", err)
		return 0, err
	}

	//判断是实时更新,还是后台异步同步数据库数据.
	if DataSyncConf.RealTime == false {
		err := InitAsync()
		if err != nil {
			err = fmt.Errorf("InitAsync(),初始化异步同步数据库失败: %s", err)
			return 0, err
		}
	}

	return rowNum, nil
}

//获取列在数据库中的类型.
func GetColumnType(column string) (columnType string) {
	col, ok := ColumnInfo[column]
	if ok {
		return col.databaseTypeName
	}
	return ""
}

//根据主键,获取该行的数据.
func GetRow(Pkey string) (result map[string]string, err error) {
	result = map[string]string{}
	v, ok := DbCache.Load(Pkey)
	if ok {
		rowMap := v.(sync.Map)
		rowMap.Range(func(column, value interface{}) bool {
			result[column.(string)] = value.(string)
			return true
		})
	} else {
		err = fmt.Errorf("dbcache.GetRow(),数据未找到")
		return result, err
	}
	return result, nil
}

//(该函数仅于分页显示,提取数据)从缓存中,获取指定的行,开始行-结束行.(不包括结束行)并不是与数据库中行号一致.
//因为从数据库中检索数据时,数据先后不一定.这只是缓存的行号.目的是一样.不影响使用.
func GetRowBetween(start int, end int) (result []map[string]string) {
	if start > len(RowNumDbCache) {
		start = len(RowNumDbCache) - 1
	}
	if end > len(RowNumDbCache) {
		end = len(RowNumDbCache)
	}
	//获取start到end之间删除的行
	var delRowCount int
	for k, _ := range delRowNum {
		if k >= start && k < end {
			delRowCount++
		}
	}

	for i := start; i < end+delRowCount; i++ {
		if delRowNum[i] {
			continue
		}

		row := make(map[string]string, end-start)
		rowMap := *RowNumDbCache[i]
		rowMap.Range(func(column, value interface{}) bool {
			row[column.(string)] = value.(string)
			return true
		})
		result = append(result, row)
	}
	return result
}

//(该函数仅于分页显示,提取数据),获取用于分页缓存中数据的行号
func getRowNum(pkeyValue string) (n int) {
	pkeyValue = strings.TrimSpace(pkeyValue)
	var isFound bool
	for i := 0; i < len(RowNumDbCache); i++ {
		rowMap := *RowNumDbCache[i]
		isFound = false
		rowMap.Range(func(column, value interface{}) bool {
			if column.(string) == TableConfig.GetPkey() && value.(string) == pkeyValue {
				isFound = true
			}
			return true
		})
		if isFound {
			return i
		}
	}
	return -1
}

//(该函数仅于分页),当有删除行时,只是把删除的行号保存.未进行切片的删除,因为切片的删除会影响性能.
func delRowNumDbCache(pkeyValue string) {
	//检查当保存删除的行容量,重新初始化RowNumDbCache
	size := len(RowNumDbCache)
	if len(delRowNum) >= size {
		mutex.Lock()
		RowNumDbCache = make([]*sync.Map, 0, size)
		delRowNum = make(map[int]bool,size)
		DbCache.Range(func(_, v interface{}) bool {
			rowMap := v.(sync.Map)
			RowNumDbCache = append(RowNumDbCache, &rowMap)
			return true
		})
		mutex.Unlock()
	}

	n := getRowNum(pkeyValue)
	if n != -1 {
		mutex.Lock()
		delRowNum[n] = true
		mutex.Unlock()
	}
}

//根据主键,获取一列的数据.
func GetColumn(Pkey string, column string) (result string, err error) {
	results, err := GetRow(Pkey)
	if err != nil {
		err = fmt.Errorf("GetColumn(),数据未找到: %s", err)
		return "", err
	}
	result, ok := results[column]
	if !ok {
		err = fmt.Errorf("GetColumn(),数据未找到: %s", err)
		return "", err
	}
	return result, err
}

//分解字符串保存于切片中,去除两边的空格.
func SplitString(str string, operator string) (slices []string) {
	slices = strings.Split(str, operator)
	for i, _ := range slices {
		slices[i] = strings.TrimSpace(slices[i])
	}
	return slices
}

//分割表达式
//以(and,or,逗号等分割)分解条件表达式,保存于切片,例如:user=xiaoming,pwd=1345534
//分解为每个单独的字符串,例:user=xiaoming,分解成三个,user,=,xiaoming
func SplitCondition(str string, operator string) (result []string, err error) {
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
		err = fmt.Errorf("dbcache.SplitCondition(),字符串:%s 按(%s)分割失败.", str, operator)
		return nil, err
	}
}

//获取and或or二边的条件表达式
func GetCondition(where string, operator string) (whereCondition [][]string, err error) {
	slices := make([]string, 0)
	whereCondition = [][]string{}
	switch {
	case operator == "=":
		slices = append(slices, where)
	case operator == "and" || operator == "or":
		slices = SplitString(where, operator)
	case operator == ",":
		slices = SplitString(where, operator)
	default:
		slices = SplitString(where, operator)
	}

	for _, condition := range slices {
		if i := strings.Index(condition, "!="); i != -1 {
			result, err := SplitCondition(condition, "!=")
			if err != nil {
				err = fmt.Errorf("dbcache.GetCondition(),条件错误: %s ,err: %s", condition, err)
				return nil, err
			}
			whereCondition = append(whereCondition, result)

		} else if i := strings.Index(condition, "="); i != -1 {
			result, err := SplitCondition(condition, "=")
			if err != nil {
				err = fmt.Errorf("dbcache.GetCondition(),条件错误: %s ,err: %s", condition, err)
				return nil, err
			}
			whereCondition = append(whereCondition, result)
		}
	}
	return whereCondition, err
}

//根据where条件,获取多行数据.
func GetWhere(where string) (result []map[string]string, err error) {
	whereCondition := [][]string{}
	where = strings.TrimSpace(where)
	if len(where) == 0 {
		return nil, fmt.Errorf("dbcache.GetWhere(),where条件不能为空")
	}

	where = strings.ToLower(where)
	isAnd := strings.Contains(where, " and ")
	isOr := strings.Contains(where, " or ")

	if isAnd == true && isOr == false {
		whereCondition, err = GetCondition(where, "and")
		if err != nil {
			err = fmt.Errorf("dbcache.GetWhere(),条件错误: %s ,err : %s", where, err)
			return nil, err
		}
	}

	if isAnd == false && isOr == true {
		whereCondition, err = GetCondition(where, "or")
		if err != nil {
			err = fmt.Errorf("dbcache.GetWhere(),条件错误: %s ,err : %s", where, err)
			return nil, err
		}
	}

	if isAnd == false && isOr == false {
		whereCondition, err = GetCondition(where, "=")
		if err != nil {
			err = fmt.Errorf("dbcache.GetWhere(),条件错误: %s , err : %s", where, err)
			return nil, err
		}
	}

	DbCache.Range(func(k, v interface{}) bool {
		row := map[string]string{}
		rowMap := v.(sync.Map)
		for i, condition := range whereCondition {
			keyValue, ok := rowMap.Load(condition[0])
			if ok {
				if condition[1] == "=" {
					if condition[2] == keyValue.(string) {
						whereCondition[i][3] = "true"
					}
				} else if condition[1] == "!=" {
					if condition[2] != keyValue.(string) {
						whereCondition[i][3] = "true"
					}
				}
			}
		}
		switch {
		case isAnd == true && isOr == false:
			isMatch := false
			for i, condition := range whereCondition {
				if condition[3] == "false" {
					isMatch = true
				} else {
					whereCondition[i][3] = "false"
				}
			}
			if isMatch == false {
				rowMap.Range(func(k, v interface{}) bool {
					(row)[k.(string)] = v.(string)
					return true
				})

				result = append(result, row)
			}

		case isAnd == false && isOr == true:
			isMatch := false
			for i, condition := range whereCondition {
				if condition[3] == "true" {
					isMatch = true
					whereCondition[i][3] = "false"
				}
			}

			if isMatch == true {
				rowMap.Range(func(k, v interface{}) bool {
					(row)[k.(string)] = v.(string)
					return true
				})
				result = append(result, row)
			}

		case isAnd == false && isOr == false:
			isMatch := false
			for i, condition := range whereCondition {
				if condition[3] == "true" {
					isMatch = true
					whereCondition[i][3] = "false"
				}
			}

			if isMatch == true {
				rowMap.Range(func(k, v interface{}) bool {
					(row)[k.(string)] = v.(string)
					return true
				})
				result = append(result, row)
			}
		}

		return true
	})
	return result, err
}

//根据主键值,删除该行数据.
func DelRow(Pkey string) (n int64, err error) {
	n, err = DelDbRow(Pkey)
	if err != nil {
		err = fmt.Errorf("DelRow(),在数据库中删除主键为(%s)的行失败.err : %s", Pkey, err)
		return 0, err
	}
	//删除缓存
	DbCache.Delete(Pkey)
	//删除用于分页缓存中的数据
	delRowNumDbCache(Pkey)
	return n, err
}

//根据主键值,删除数据库中该行数据.
func DelDbRow(key string) (n int64, err error) {
	columnType := GetColumnType(key)
	var sqlString string
	if strings.Contains(columnType, "INT") || columnType == "FLOAT" || columnType == "DOUBLE" || columnType == "DECIMAL" {
		sqlString = "DELETE from " + TableConfig.GetTableName() + " where " + TableConfig.GetPkey() + "=" + key
	} else {
		sqlString = "DELETE from " + TableConfig.GetTableName() + " where " + TableConfig.GetPkey() + "='" + key + "'"
	}
	//判断是实时更新,还是异步更新
	if DataSyncConf.RealTime == true {
		rs, err := DbConn.Exec(sqlString)
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
		sendToAsyncChan(sqlString)
	}
	return 0, nil
}

//根据主键,更新一列的数据.
func UpdateColumn(Pkey string, column string, value string) (n int64, err error) {
	Columns := TableConfig.GetColumns()
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

	v, ok := DbCache.Load(Pkey)
	if ok {
		//更新数据库
		i, err := UpdateDbcolumn(Pkey, column, value)
		if err != nil {
			err = fmt.Errorf("dbcache.UpdateColumn():数据库列更新失败-->%w", err)
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
func UpdateDbcolumn(Pkey string, column string, value string) (n int64, err error) {
	columnType := GetColumnType(column)
	if columnType == "" {
		columnType = "VARCHAR"
	}
	var sqlString string
	if strings.Contains(columnType, "INT") || columnType == "FLOAT" || columnType == "DOUBLE" || columnType == "DECIMAL" {
		sqlString = "UPDATE " + TableConfig.GetTableName() + " SET " + column + "=" + value + " WHERE " + TableConfig.GetPkey() + "='" + Pkey + "'"
	} else {
		sqlString = "UPDATE " + TableConfig.GetTableName() + " SET " + column + "='" + value + "' WHERE " + TableConfig.GetPkey() + "='" + Pkey + "'"
	}
	//判断是实时更新,还是异步更新
	if DataSyncConf.RealTime == true {
		rs, err := DbConn.Exec(sqlString)
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
		sendToAsyncChan(sqlString)
	}
	return 0, nil
}

//根据主键,更新多列数据.
func UpdateColumns(Pkey string, where string) (n int64, err error) {
	whereCondition, err := GetCondition(where, ",")
	if err != nil {
		err = fmt.Errorf("UpdateColumns(),条件错误: %s. err: %s", where, err)
		return 0, err
	}
	columns := TableConfig.GetColumns()

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

	v, ok := DbCache.Load(Pkey)
	if ok {
		rowMap := v.(sync.Map)
		//更新数据库
		n, err = UpdateDbcolumns(Pkey, where)
		if err != nil {
			err = fmt.Errorf("UpdateDbColumns() filed, err:%s", err)
			return 0, err
		}

		//更新缓存
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

//根据表达式,得到SQL语句的字符串.
func GetSqlStr(condition string) (SqlStr string) {
	whereCondition, err := GetCondition(condition, ",")
	if err != nil {
		logs.Error("a", "GetSqlStr(),条件错误: %s, err: %s", condition, err)
		return ""
	}
	var columns string

	for _, condition := range whereCondition {
		columnType := GetColumnType(condition[0])
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
func isExistPkey(condition string) (isTrue bool) {
	whereCondition, err := GetCondition(condition, ",")
	if err != nil {
		logs.Info("a", "isExistPkey(),条件错误: %s, err: %s", condition, err)
		return false
	}
	Pkey := TableConfig.GetPkey()
	isTrue = false
	for _, condition := range whereCondition {
		if condition[0] == Pkey {
			isTrue = true
		}
	}
	return isTrue
}

//根据主键,更新数据库中多列.
func UpdateDbcolumns(Pkey string, condition string) (n int64, err error) {

	SqlStr := GetSqlStr(condition)
	sqlString := "UPDATE " + TableConfig.GetTableName() + " SET " + SqlStr + " WHERE " + TableConfig.GetPkey() + "='" + Pkey + "'"
	if DataSyncConf.RealTime == true {
		rs, err := DbConn.Exec(sqlString)
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
		sendToAsyncChan(sqlString)
	}
	return 0, nil
}

//插入一行数据.
func InsertRow(condition string) (n int64, err error) {
	RowMap := new(sync.Map)
	whereCondition, err := GetCondition(condition, ",")
	if err != nil {
		err = fmt.Errorf("InsertRow(),获取条件错误. err: %s", err)
		return 0, err
	}

	Pkey := TableConfig.GetPkey()
	var PkeyValue string
	isPkey := false
	for _, condition := range whereCondition {
		RowMap.Store(condition[0], condition[2])
		if condition[0] == Pkey {
			PkeyValue = condition[2]
			isPkey = true
		}

		colInfo, ok := ColumnInfo[condition[0]]
		if ok {
			if colInfo.isNullable == true && colInfo.nullable == false {
				if strings.TrimSpace(condition[2]) == "" {
					err = fmt.Errorf("InsertRow(),该列%s不能为空. err: %s", condition[0], err)
					return 0, err
				}
			}
		}
	}

	if TableConfig.PkeyIsIncrement() == false && isPkey != true {
		err = fmt.Errorf("InsertRow(),插入行中,没有主键.条件: %s, 主键: %s, err: %s", condition, Pkey, err)
		return 0, err
	}
	//插入数据库
	i, err := InsertDbRow(condition)
	if err != nil {
		return 0, err
	}
	//插入缓存
	DbCache.Store(PkeyValue, *RowMap)

	//插入用于分页查询缓存
	mutex.Lock()
	RowNumDbCache = append(RowNumDbCache, RowMap)
	mutex.Unlock()

	return i, nil
}

//插入一行数据到数据库.
func InsertDbRow(condition string) (n int64, err error) {
	SqlStr := GetSqlStr(condition)
	sqlString := "INSERT INTO " + TableConfig.GetTableName() + " SET " + SqlStr
	if DataSyncConf.RealTime == true {
		rs, err := DbConn.Exec(sqlString)
		if err != nil {
			err = fmt.Errorf("InsertDbRow(),插入行到数据库失败.语句:%s, err: %s", sqlString, err)
			return 0, err
		}
		n, err = rs.RowsAffected()
		if err != nil {
			err = fmt.Errorf("InsertDbRow(),获取受影响的行失败. err: %w", err)
			return 0, err
		}
		return n, err
	} else {
		sendToAsyncChan(sqlString)
	}
	return 0, nil
}
