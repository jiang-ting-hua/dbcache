package cache

import (
	"database/sql"
	"dbcache/conf"
	"dbcache/logs"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"strings"
	"sync"
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
	DbConn       *sql.DB                //数据库对象
	TableConfig  conf.Table             //[配置文件cache.conf]保存缓存数据表信息
	DataSyncConf conf.DataSync          //[配置文件cache.conf]保存数据库数据同步.是实时还是异步.
	ColumnInfo   map[string]*columnInfo //数据库缓存表中列的信息
	CacheType    string                 //缓存类型.array数组,link链表[用于页面分页显示]
	dataSync       *DataSync
	//map数据缓存对象[主缓存对象]
	DbCache sync.Map //用来缓存的表数据
	//链表缓存对象
	LinkDbCache LinkCache //链表保存缓存数据[用于页面分页显示]
	//链表缓存对象
	RowNumDbCache []*sync.Map  //用来根据行号查询缓存,[用于页面分页显示]
	delRowNum     map[int]bool //保存RowNumDbCache中已删除行的行号,当有删除行时,只是把删除的行号保存.未进行切片的删除,因为切片的删除会影响性能.
	RowCount      int64        //总行数
	mutex         sync.RWMutex //读写锁
}

func NewDBcache(db *sql.DB) *DBcache {
	return &DBcache{
		DbConn:        db,
		TableConfig:   conf.Table{},
		ColumnInfo:    nil,
		CacheType:     "array",
		dataSync:      NewDataSync(),
		DbCache:       sync.Map{},
		LinkDbCache:   NewLinkCache(),
		RowNumDbCache: nil,
		delRowNum:     make(map[int]bool),
		RowCount:      0,
		mutex:         sync.RWMutex{},
	}
}


//初始化缓存信息
func InitCache(db *sql.DB,) (dbCache *DBcache, err error) {
	dbCache = NewDBcache(db)
	//读取配置文件,初始化配置信息
	err = conf.ParseConf(conf.TABLES_CONF, &dbCache.TableConfig)
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
			err = fmt.Errorf("InitCache(),从数据库中查询总行数,查询的结果为空: %s", err)
			return nil, err
		} else {
			err = fmt.Errorf("InitCache(),从数据库中查询总行数失败: %s", err)
			return nil, err
		}
	}
	dbCache.RowCount = count
	dbCache.RowNumDbCache = make([]*sync.Map, count, count+count/2)

	// 执行select查询,检索缓存数据
	rows, err := db.Query(selectSql)
	if err != nil {
		err = fmt.Errorf("InitCache(),执行select查询,检索数据时失败: %s", err)
		return nil, err
	}
	defer rows.Close()
	// 得到列名
	columns, err := rows.Columns()
	if err != nil {
		err = fmt.Errorf("InitCache(),从数据库中得到列名时失败: %s", err)
		return nil, err
	}
	// 得到列类型信息
	types, err := rows.ColumnTypes()
	if err != nil {
		err = fmt.Errorf("InitCache(),从数据库中得到列类型时失败: %s", err)
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

			RowMap.Store(columns[i], value)
		}

		dbCache.DbCache.Store(PkeyValue, *RowMap)
		dbCache.RowNumDbCache[rowNum] = RowMap
		node :=&Node{
			rowNum: rowNum,
			pkey:   PkeyValue,
			row:    RowMap,
			pre:    nil,
			next:   nil,
		}
		dbCache.LinkDbCache.InsertTail(node)
		rowNum++ //行计数.
	}
	if err = rows.Err(); err != nil {
		err = fmt.Errorf("InitCache(),从数据库中读取行失败: %s", err)
		return nil, err
	}

	//判断是实时更新,还是后台异步同步数据库数据.
	if dbCache.DataSyncConf.RealTime == false {
		err := InitAsync(db,dbCache.dataSync,dbCache.TableConfig.GetTableName())
		if err != nil {
			err = fmt.Errorf("InitAsync(),初始化异步同步数据库失败: %s", err)
			return nil, err
		}
	}
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
		err = fmt.Errorf("dbcache.GetRow(),数据未找到")
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
		err = fmt.Errorf("DelRow(),在数据库中删除主键为(%s)的行失败.err : %s", Pkey, err)
		return 0, err
	}
	//删除缓存
	d.DbCache.Delete(Pkey)

	//删除用于分页缓存中的数据
	switch d.TableConfig.CacheType {
	case "array":   //数据保存于数组.
		d.delRowNumDbCache(Pkey)
	case "link":    //数据保存于链表
		d.LinkDbCache.DeleteNodePkey(Pkey)
	}
	return n, err
}

//根据主键值,删除数据库中该行数据.
func (d *DBcache) DelDbRow(key string) (n int64, err error) {
	//删除缓存中后,再删除数据库中对应的行.通过主键查找.
	columnType := d.GetColumnType(key)
	var sqlString string
	if strings.Contains(columnType, "INT") || columnType == "FLOAT" || columnType == "DOUBLE" || columnType == "DECIMAL" {
		sqlString = "DELETE from " + d.TableConfig.GetTableName() + " where " + d.TableConfig.GetPkey() + "=" + key
	} else {
		sqlString = "DELETE from " + d.TableConfig.GetTableName() + " where " + d.TableConfig.GetPkey() + "='" + key + "'"
	}
	//判断是实时更新,还是异步更新
	if d.DataSyncConf.RealTime == true {
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
		d.dataSync.sendToAsyncChan(sqlString)
	}
	return 0, nil
}

//(该函数仅于分页),当有删除行时,只是把删除的行号保存.未进行切片的删除,因为切片的删除会影响性能.
func (d *DBcache) delRowNumDbCache(pkeyValue string) {
	//检查当保存删除的行容量,重新初始化RowNumDbCache
	size := len(d.RowNumDbCache)
	if size/2 == 0 {
		return
	}
	if len(d.delRowNum) >= size/2 {
		d.mutex.Lock()
		d.RowNumDbCache = make([]*sync.Map, 0, size)
		d.delRowNum = make(map[int]bool, size)
		d.DbCache.Range(func(_, v interface{}) bool {
			rowMap := v.(sync.Map)
			d.RowNumDbCache = append(d.RowNumDbCache, &rowMap)
			return true
		})
		d.mutex.Unlock()
	}

	n := d.getRowNum(pkeyValue)
	if n != -1 {
		d.mutex.Lock()
		d.delRowNum[n] = true
		d.mutex.Unlock()
	}
}

//(该函数仅于分页显示,提取数据),获取用于分页缓存中数据的行号
func (d *DBcache) getRowNum(pkeyValue string) (n int) {
	pkeyValue = strings.TrimSpace(pkeyValue)
	var isFound bool
	for i := 0; i < len(d.RowNumDbCache); i++ {
		rowMap := *d.RowNumDbCache[i]
		isFound = false
		rowMap.Range(func(column, value interface{}) bool {
			if column.(string) == d.TableConfig.GetPkey() && value.(string) == pkeyValue {
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

//获取列在数据库中的类型.
func (d *DBcache) GetColumnType(column string) (columnType string) {
	col, ok := d.ColumnInfo[column]
	if ok {
		return col.databaseTypeName
	}
	return ""
}

//根据where条件,获取多行数据.
func (d *DBcache)GetWhere(where string) (result []map[string]string, err error) {
	//存储各条件表达式
	whereCondition := [][]string{}

	where = strings.TrimSpace(where)
	if len(where) == 0 {
		return nil, fmt.Errorf("dbcache.GetWhere(),where条件不能为空")
	}

	//检查是否包含and 或 or
	where = strings.ToLower(where)
	isAnd := strings.Contains(where, " and ")
	isOr := strings.Contains(where, " or ")

	//如果where是and组合条件
	if isAnd == true && isOr == false {
		whereCondition, err = d.GetCondition(where, "and")
		if err != nil {
			err = fmt.Errorf("dbcache.GetWhere(),条件错误: %s ,err : %s", where, err)
			return nil, err
		}
	}

	//如果where是or组合条件
	if isAnd == false && isOr == true {
		whereCondition, err = d.GetCondition(where, "or")
		if err != nil {
			err = fmt.Errorf("dbcache.GetWhere(),条件错误: %s ,err : %s", where, err)
			return nil, err
		}
	}

	//如果没and和or,只是单条件表达式
	if isAnd == false && isOr == false {
		whereCondition, err = d.GetCondition(where, "=")
		if err != nil {
			err = fmt.Errorf("dbcache.GetWhere(),条件错误: %s , err : %s", where, err)
			return nil, err
		}
	}

	//从sync.map中取得每行数据,存储于(MAP类型)
	d.DbCache.Range(func(k, v interface{}) bool {
		//row := new(map[string]string)
		//*row = map[string]string{}
		row := map[string]string{}
		rowMap := v.(sync.Map)
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
				result = append(result, row)
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
				result = append(result, row)
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
				result = append(result, row)
			}
		}

		return true
	})
	return result, err
}

//获取and或or二边的条件表达式
func (d *DBcache)GetCondition(where string, operator string) (whereCondition [][]string, err error) {
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
				err = fmt.Errorf("dbcache.GetCondition(),条件错误: %s ,err: %s", condition, err)
				return nil, err
			}
			whereCondition = append(whereCondition, result)
			//如果条件是=(等于)
		} else if i := strings.Index(condition, "="); i != -1 {
			result, err := d.SplitCondition(condition, "=")
			if err != nil {
				err = fmt.Errorf("dbcache.GetCondition(),条件错误: %s ,err: %s", condition, err)
				return nil, err
			}
			whereCondition = append(whereCondition, result)
		}
	}
	return whereCondition, err
}

//分解字符串保存于切片中,去除两边的空格.
func (d *DBcache)SplitString(str string, operator string) (slices []string) {
	slices = strings.Split(str, operator)
	for i, _ := range slices {
		slices[i] = strings.TrimSpace(slices[i])
	}
	return slices
}

//分割表达式
//以(and,or,逗号等分割)分解条件表达式,保存于切片,例如:user=xiaoming,pwd=1345534
//分解为每个单独的字符串,例:user=xiaoming,分解成三个,user,=,xiaoming
func (d *DBcache)SplitCondition(str string, operator string) (result []string, err error) {
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

//根据主键,更新一列的数据.
func (d *DBcache)UpdateColumn(Pkey string, column string, value string) (n int64, err error) {
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
			err = fmt.Errorf("dbcache.UpdateColumn():数据库列更新失败,%s", err)
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
func (d *DBcache)UpdateDbcolumn(Pkey string, column string, value string) (n int64, err error) {
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
	if d.DataSyncConf.RealTime == true {
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
		d.dataSync.sendToAsyncChan(sqlString)
	}
	return 0, nil
}

//根据主键,更新多列数据.
func (d *DBcache)UpdateColumns(Pkey string, where string) (n int64, err error) {
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
			err = fmt.Errorf("UpdateDbColumns() filed, err:%s", err)
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
func (d *DBcache)UpdateDbcolumns(Pkey string, condition string) (n int64, err error) {

	SqlStr := d.GetSqlStr(condition)
	sqlString := "UPDATE " + d.TableConfig.GetTableName() + " SET " + SqlStr + " WHERE " + d.TableConfig.GetPkey() + "='" + Pkey + "'"
	if d.DataSyncConf.RealTime == true {
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
		d.dataSync.sendToAsyncChan(sqlString)
	}
	return 0, nil
}

//根据表达式,得到SQL语句的字符串.
func (d *DBcache)GetSqlStr(condition string) (SqlStr string) {
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
func (d *DBcache)isExistPkey(condition string) (isTrue bool) {
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
func (d *DBcache)InsertRow(condition string) (n int64, err error) {
	RowMap := new(sync.Map)
	whereCondition, err := d.GetCondition(condition, ",")
	if err != nil {
		err = fmt.Errorf("InsertRow(),获取条件错误. err: %s", err)
		return 0, err
	}
	//判断插入一行数据中，有没有主键．这只是需要主键．
	//如果是自增列，则不需要主键．
	Pkey := d.TableConfig.GetPkey()
	var PkeyValue string
	isPkey := false
	for _, condition := range whereCondition {
		//将插入的行数据保存于缓存中
		RowMap.Store(condition[0], condition[2])
		//判断是否有主键
		if condition[0] == Pkey {
			PkeyValue = condition[2]
			isPkey = true
		}
		//判断是否为空
		colInfo, ok := d.ColumnInfo[condition[0]]
		if ok {
			if colInfo.isNullable == true && colInfo.nullable == false {
				if strings.TrimSpace(condition[2]) == "" {
					err = fmt.Errorf("InsertRow(),该列%s不能为空. err: %s", condition[0], err)
					return 0, err
				}
			}
		}
	}
	//不是自增列,必须要有主键.自增列可以不要
	if d.TableConfig.PkeyIsIncrement() == false && isPkey != true {
		err = fmt.Errorf("InsertRow(),插入行中,没有主键.条件: %s, 主键: %s, err: %s", condition, Pkey, err)
		return 0, err
	}
	//插入数据库
	i, err := d.InsertDbRow(condition)
	if err != nil {
		return 0, err
	}
	//插入缓存
	d.DbCache.Store(PkeyValue, *RowMap)

	switch d.TableConfig.CacheType {
	case "array":   //数据保存于数组.
		//插入用于分页查询缓存
		d.mutex.Lock()
		d.RowNumDbCache = append(d.RowNumDbCache, RowMap)
		d.mutex.Unlock()
	case "link":    //数据保存于链表
		node :=&Node{
			rowNum: d.LinkDbCache.length+1,
			pkey:   PkeyValue,
			row:    RowMap,
			pre:    nil,
			next:   nil,
		}
		d.LinkDbCache.InsertTail(node)
	}
	return i, nil
}

//插入一行数据到数据库.
func (d *DBcache)InsertDbRow(condition string) (n int64, err error) {
	SqlStr := d.GetSqlStr(condition)
	sqlString := "INSERT INTO " + d.TableConfig.GetTableName() + " SET " + SqlStr
	if d.DataSyncConf.RealTime == true {
		rs, err := d.DbConn.Exec(sqlString)
		if err != nil {
			err = fmt.Errorf("InsertDbRow(),插入行到数据库失败.语句:%s, err: %s", sqlString, err)
			return 0, err
		}
		n, err = rs.RowsAffected()
		if err != nil {
			err = fmt.Errorf("InsertDbRow(),获取受影响的行失败. err: %s", err)
			return 0, err
		}
		return n, err
	} else {
		d.dataSync.sendToAsyncChan(sqlString)
	}
	return 0, nil
}

//(该函数仅于分页显示,提取数据)从缓存中,获取指定的行,开始行-结束行.(不包括结束行)并不是与数据库中行号一致.
//因为从数据库中检索数据时,数据先后不一定.这只是缓存的行号.目的是一样.不影响使用.
func (d *DBcache)GetRowBetween(start int, end int) (result []map[string]string) {
	switch d.TableConfig.CacheType {
	case "array":   //数据保存于数组.
		if start > len(d.RowNumDbCache) {
			start = len(d.RowNumDbCache) - 1
		}
		if end > len(d.RowNumDbCache) {
			end = len(d.RowNumDbCache)
		}
		//获取start到end之间删除的行
		var delRowCount int
		for k, _ := range d.delRowNum {
			if k >= start && k < end {
				delRowCount++
			}
		}

		for i := start; i < end+delRowCount; i++ {
			if d.delRowNum[i] {
				continue
			}

			row := make(map[string]string, end-start)
			rowMap := *d.RowNumDbCache[i]
			rowMap.Range(func(column, value interface{}) bool {
				row[column.(string)] = value.(string)
				return true
			})
			result = append(result, row)
		}
	case "link":    //数据保存于链表
		startInt64 :=int64(start)
		endInt64:=int64(end)
		length := d.LinkDbCache.length
		if startInt64 > length {
			startInt64 = length
		}
		if endInt64 > length {
			endInt64 = length
		}
		nodes := d.LinkDbCache.GetNodeBetween(startInt64, endInt64)
		for _,node:= range nodes{
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
//关闭打开的对象
func (d *DBcache)Close() {
	//关闭数据库对象
	d.DbConn.Close()
	//关闭异步同步文件对象.
	d.dataSync.AsyncFileObj.Close()
	d.dataSync.AsyncFailedFileObj.Close()
}