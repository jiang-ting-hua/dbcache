package main

import (
	"dbcache/cache"
	"dbcache/db"
	"dbcache/logs" //日志库
	"fmt"
	"time"
)

func main() {
	//数据库表缓存是并发安全的.适用于在读多,写少的情况下.

	//要先配置文件config.conf(设置数据库和日志) 和 文件cache.conf(配置需要缓存的表及列,实时同步更新,还是异步更新数据库).
	//users.sql这个有建表结构和数据.
	//目前只支持主键为一个列.

	//缓存数据可以是字符,整型,浮点,日期(datetime,timestamp).
	// (注意日期型,涉及时区,如果在连接时加上参数:&parseTime=true&loc=Local 返回数据带时区信息.暂时未做处理,这样不能保存,不带这二个参数,可以保存)
	//parseTime是查询结果是否自动解析为时间. loc是MySQL的时区设置.

	//目前如果有更新,先更新数据库,再更新缓存.
	//注意:更新数据库,分为实时更新同步数据库和异步更新数据库,在cache.conf文件中配置

	//如果实时更新,先更新数据库,再更新缓存.
	//异步更新,会先把数据保存于当前目录下的文件async_sql.sql,再更新数据库,如果更新失败,会把失败的sql的语句保存于async_sql_failed.sql文件.

	//支持日志系统: s 标准输出屏幕, f 记录到日志, e 发送邮件, a 所有(包括s,f,e)(需先在配置文件config.conf中配置),
	// 等级说明:1 DEBUG,2 TRACE,3 INFO,4 WARNING,5 ERROR,6 FATAL
	//注意二个开关,enable控制是否开启,run_level运行等级,只在大于或等于设置等级才输出日志.)
	//如有什么问题,讨论可联系:38704889@qq.com

	//以下是使用方法.

	//初始化日志库
	logs.InitLog()
	defer logs.Close()

	//连接数据库,初始化调用.
	db, err := db.ConnectDB()
	if err != nil {
		logs.Fatal("a", "dbcache.ConnectDB().连接数据库失败, err: %s", err)
		return
	}
	defer db.Close()

	//初始化需要缓存的数据,把数据缓存在内存中.
	dbCache, err := cache.InitCache(db)
	if err != nil {
		logs.Fatal("a", "dbcache.InitCache().初始化缓存失败, err: %s", err)
		return
	}
	defer dbCache.Close()
	fmt.Println("缓存总行数:", dbCache.RowCount)
	fmt.Println(dbCache.LinkDbCache.GetLength())
	fmt.Println()

	// 查询在内存中查询数据.
	// 以下为增删改查的样例.

	//一. GetRow:根据主键值,取得该行数据
	//pkey := "2A7420SW66C5TJC02A77"
	fmt.Println("一. GetRow().根据主键,取得该行数据.")
	pkey := "00YS0SW2N4NT7K8HP13E"
	result, err := dbCache.GetRow(pkey)
	if err != nil {
		fmt.Println("一. GetRow().根据主键,取得该行数据错误, err:", err)
	}
	for k, v := range result {
		fmt.Printf("%s=%s, ", k, v)
	}
	fmt.Println()
	fmt.Println()

	//二. GetColumn:根据主键,取得某列的数据
	fmt.Println("二. GetColumn().根据主键,取得某列的数据")
	v, err := dbCache.GetColumn("00YS0SW2N4NT7K8HP13E", "name")
	if err != nil {
		fmt.Println("二. GetColumn().根据主键,取得某列的数据错误, err:", err)
	}
	fmt.Println("根据主键，取得某列的数据: ", v)
	fmt.Println()

	//三. DelRow:根据主键,删除该行数据
	fmt.Printf("三. DelRow().根据主键,删除该行数据\n")
	pkey = "013CIHW1G6HX8D6Q4H1Q"
	n, err := dbCache.DelRow(pkey)
	if err != nil {
		logs.Info("a", "三. DelRow().根据主键,删除该行数据错误:%d行,主键:%s,  err:", n, pkey, err)
	}
	fmt.Printf("三. 根据主键,删除行数据,数据库影响%d行,如果是异步执行为0.\n", n)
	fmt.Println()

	//四. GetWhere:根据where条件,查询缓存中所有符合条件的行.不用加引号
	fmt.Println("四. 根据where条件,查询缓存中所有符合条件的行.")
	var value []map[string]string
	//value ,_ = dbcache.GetWhere("name=AFA5Y9FB or password=Q80BJT")
	value, err = dbCache.GetWhere("address=重庆2 and password=666")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("    查询到符合条件的有%d行:\n", len(value))
	for _, c := range value {
		for k, v := range c {
			fmt.Printf("%s=%s,", k, v)
		}
		fmt.Println()
	}
	fmt.Println()

	//五. UpdateColumn:根据主键,更新一列
	fmt.Println("五. UpdateColumn().根据主键,更新一列数据")
	pkey = "00YS0SW2N4NT7K8HP13E"
	col := "name"
	val := "王明99"
	n, err = dbCache.UpdateColumn(pkey, col, val)
	if err != nil {
		logs.Error("a", "五. UpdateColumn().更新一列错误, err: %s", err)
	}
	fmt.Printf("三. 根据主键,更新一列数据,数据库影响%d行,如果是异步执行为0.\n", n)
	fmt.Println()

	//六. UpdateColumns:根据主键,更新多列
	fmt.Printf("六. UpdateColumns().根据主键,更新多列数据:\n")
	pkey = "00YS0SW2N4NT7K8HP13E"
	cols := "age=111,address= 重庆111"
	n, err = dbCache.UpdateColumns(pkey, cols)
	if err != nil {
		logs.Error("a", "六. UpdateColumns().更新多列错误, err:%s", err)
	}
	fmt.Printf("三. 根据主键,更新多列数据,数据库影响%d行,如果是异步执行为0.\n", n)
	fmt.Println()

	//七. InsertRow():插入一行数据
	fmt.Printf("七. InsertRow().插入一行数据\n")
	insert := "uid=21111111,name=jth,address=重庆,password=888888,age=33,price=66.123456,create_date=2020-02-02 02:02:02,update_date=2020-01-01 01:01:01"
	n, err = dbCache.InsertRow(insert)
	if err != nil {
		logs.Error("a", "七. InsertRow().插入错误, err:%s", err)
	}
	fmt.Printf("三. 插入一行数据,数据库影响%d行,如果是异步执行为0.\n", n)

	//八,GetRowNum():从缓存中,获取指定的行,开始行-结束行.用于页面分页显示.
	fmt.Printf("八,GetRowNum():获取缓存中,start行到end行之间的数据.\n")
	rows := dbCache.GetRowBetween(0,10)
	for i,rowMap:=range rows{
		fmt.Printf(" 第%d行 ",i)
		for k,v:=range rowMap{
			fmt.Printf(" %s=%s, ",k,v)
		}
		fmt.Println()
	}

	time.Sleep(time.Second * 3)
}

func prof(){
	/*//性能分析
	//新建性能分析文件
	f, err := os.Create("./cpu.prof")
	if err != nil {
		fmt.Println("create cpu profile file failed, err:",err)
	}
	fm, err := os.Create("./mem.prof")
	if err != nil {
		fmt.Println("create mem profile file failed, err:",err)
	}
	fg, err := os.Create("./goroutine.prof")
	if err != nil {
		fmt.Println("create goroutine profile file failed, err:",err)
	}
	//获取系统cpu信息
	err = pprof.StartCPUProfile(f)
	if err != nil {
		fmt.Println("StartCPUProfile(), err:",err)
	}
	defer pprof.StopCPUProfile()
	//获取系统mem信息
	err = pprof.WriteHeapProfile(fm)
	if err != nil {
		fmt.Println("WriteHeapProfile(), err:",err)
	}
	defer fm.Close()
	//获取系统goroutine信息
	lookup := pprof.Lookup("goroutine")
	if lookup != nil {
		fmt.Println(`Lookup("goroutine"), err:`,err)
	}
	lookup.WriteTo(fg,0)
	defer fg.Close()*/
}