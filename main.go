package main
//此文件为样例.
import (
	"dbcache/cache"
	"dbcache/conf"
	"dbcache/db"
	"dbcache/logs" //日志库
	"fmt"
)
/*
	使用说明:以下二步.

	一.配置程序目录下二个件:
	config.conf(设置数据库和日志,rpc和grpc)
	cache.conf(配置需要缓存的表及列,是否实时更新,还是异步更新数据库等.

	二.在./conf目录下,cache.go源文件中,增加对应表的结构体和方法.每一个需要缓存的表,都需要增加各自的结构体和方法.
*/

//注意:
//目前只支持主键为一个列.
//缓存数据可以是字符,整型,浮点,日期(datetime,timestamp).

//如果实时更新,数据更新是先更新数据库,再更新缓存.
//异步更新,会先把执行SQL语句保存于当前目录下的文件async_sql.sql,再更新数据库,如果更新失败,会把失败的sql的语句保存于async_sql_failed.sql文件.

//支持日志系统: s 标准输出屏幕, f 记录到日志, e 发送邮件, a 所有(包括s,f,e)(需先在配置文件config.conf中配置),
//等级说明:1 DEBUG,2 TRACE,3 INFO,4 WARNING,5 ERROR,6 FATAL
//注意二个开关,enable控制是否开启,run_level运行等级,只在大于或等于设置等级才输出日志.)
//如有什么问题,讨论可联系:38704889@qq.com

/*主要函数,下面有用例:
  1. GetRow():根据主键值,取得该行数据
  2. GetColumn():根据主键,取得某列的数据
  3.DelRow():根据主键,删除该行数据
  4.GetWhere():根据where条件,查询缓存中所有符合条件的行.不用加引号
  5.UpdateColumn():根据主键,更新一列
  6. UpdateColumns():根据主键,更新多列
  7.InsertRow():插入一行数据
  8.GetRowBetween():从缓存中,获取指定的行,开始行-结束行.用于页面分页显示.
*/

/*
    (注意日期型,涉及时区,如果在连接时加上参数:&parseTime=true返回数据带时区信息.
    暂时未转换为时间类型,只是转换为字符串,如果需要time类型,可以自己转换.所以连接mysql时,不带这个参数.)

	&parseTime=true&loc=Local  其中parseTime是查询结果是否自动解析为时间. loc是MySQL的时区设置.
	在windows下，time.Parse()的时区和time.Format()的时区是一致的。
	在linux环境下，time.Parse()的默认时区是UTC，time.Format()的时区默认是本地
    在时间转换时要注意.相差8小时 (CST=UTC+8小时) (UTC=CST-8小时)
    time.UTC():将当地时区转化为UTC时间. time.Local():将UTC时间转化为当地时间
    解决办法:使用time.FixedZone,在init初始化时,或相关时间调用函数的代码使用之前加入如下代码)
			timelocal := time.FixedZone("CST", 3600*8)
			time.Local = timelocal

    字符串转time.Time
		timeStr := "2020-02-03 21:00:00"//时间字符串
		t, err := time.ParseInLocation("2006-01-02 15:04", timeStr, time.Local) //t被转为本地时间的time.Time
		t,err := time.Parse("2006-01-02 15:04", timeStr)                        //t被转为UTC时间的time.Time

    UTC世界标准时间、世界统一时间,CST时间,CST可以同时表示美国，澳大利亚，中国，古巴四个国家的标准时间。
    CST同时可以代表如下 4 个不同的时区：
	Central Standard Time (USA) UT-6:00 美国
	Central Standard Time (Australia) UT+9:30 澳大利亚
	China Standard Time UT+8:00 中国
	Cuba Standard Time UT-4:00 古巴
*/

func main() {
	//初始化日志库
	logs.InitLog()
	defer logs.Close()

	//连接数据库,初始化调用.
	db, err := db.ConnectDB()
	if err != nil {
		logs.Fatal("a", "连接数据库失败, err: %s", err)
		return
	}
	defer db.Close()

	//缓存users表
	UsersCache, err := cache.InitCache(db, new(conf.Users))
	if err != nil {
		logs.Fatal("a", "初始化缓存失败, err: %s", err)
		return
	}
	defer UsersCache.Close()

	//缓存Goods表
	GoodsCache, err := cache.InitCache(db, new(conf.Goods))
	if err != nil {
		logs.Fatal("a", "初始化缓存失败, err: %s", err)
		return
	}
	defer GoodsCache.Close()

	////启动rpc,配置IP地址和端口,在config.conf配置文件中.
	//err = rpcserver.RpcRun()
	//if err!=nil{
	//	fmt.Println("RPC service failed",err)
	//	return
	//}
	//
	////启动Grpc,配置IP地址和端口,在config.conf配置文件中.
	//err = grpcserver.GrpcRun()
	//if err!=nil{
	//	fmt.Println("GRPC service failed",err)
	//	return
	//}


	// 以下为users表增删改查的样例.
	//----------------------------------------------------------------------
	//获取总页数:
	pageRows:=20  //每页多少行
	fmt.Printf("users表: 总行数:%d,每页%d行,总页数:%d\n",UsersCache.RowCount,pageRows,UsersCache.GetPageCount(pageRows))
	fmt.Printf("goods表: 总行数:%d,每页%d行,总页数:%d\n",GoodsCache.RowCount,pageRows,GoodsCache.GetPageCount(pageRows))

	//一. GetRow:根据主键值,取得该行数据
	fmt.Println("一. GetRow().根据主键,取得该行数据.")
	pkey := "00YS0SW2N4NT7K8HP13E"
	result, err := UsersCache.GetRow(pkey)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range result {
		fmt.Printf("%s=%s, ", k, v)
	}
	fmt.Println()
	//bytes, err := json.Marshal(result)
	//if err!=nil{
	//	fmt.Println(err)
	//}
	//fmt.Println(string(bytes))

	//二. GetColumn:根据主键,取得某列的数据
	fmt.Println("二. GetColumn().根据主键,取得某列的数据")
	v, err := UsersCache.GetColumn("00YS0SW2N4NT7K8HP13E", "name")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("根据主键，取得某列的数据: ", v)

	//三. DelRow:根据主键,删除该行数据
	fmt.Printf("三. DelRow().根据主键,删除该行数据\n")
	pkey = "013CIHW1G6HX8D6Q4H1Q"
	n, err := UsersCache.DelRow(pkey)
	if err != nil {
		logs.Info("a", "根据主键,删除行数据失败, err: %s", err)
	}
	fmt.Println("删除数据行数:", n)

	//四. GetWhere:根据where条件,查询缓存中所有符合条件的行.不用加引号
	fmt.Println("四. 根据where条件,查询缓存中所有符合条件的行.")
	var value []map[string]string
	//value ,_ = dbcache.GetWhere("name=AFA5Y9FB or password=Q80BJT")
	value, err = UsersCache.GetWhere("address=重庆 and password=888888")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("查询到符合条件的有%d行:\n", len(value))
	for _, c := range value {
		for k, v := range c {
			fmt.Printf("%s=%s,", k, v)
		}
		fmt.Println()
	}
	//bytes, err = json.Marshal(value)
	//if err!=nil{
	//	fmt.Println(err)
	//}
	//fmt.Println(string(bytes))

	//五. UpdateColumn:根据主键,更新一列
	fmt.Println("五. UpdateColumn().根据主键,更新一列数据")
	pkey = "00YS0SW2N4NT7K8HP13E"
	col := "name"
	val := "王明99"
	n, err = UsersCache.UpdateColumn(pkey, col, val)
	if err != nil {
		logs.Error("a", "更新一列错误, err: %s", err)
	}

	//六. UpdateColumns:根据主键,更新多列
	fmt.Printf("六. UpdateColumns().根据主键,更新多列数据:\n")
	pkey = "00YS0SW2N4NT7K8HP13E"
	cols := "age=111,address= 重庆111,update_date=2020-05-06 07:30:00"
	n, err = UsersCache.UpdateColumns(pkey, cols)
	if err != nil {
		logs.Error("a", "更新多列错误, err:%s", err)
	}


	//七. InsertRow():插入一行数据
	fmt.Printf("七. InsertRow().插入一行数据\n")
	insert := "uid=22222211117,name=jth,address=重庆,password=888888,age=9999993,price=66.123456,create_date=2020-02-02 02:02:02,update_date=2020-01-01 01:01:01"
	n, err = UsersCache.InsertRow(insert)
	if err != nil {
		logs.Error("a", "插入错误, err: %v", err)
	}
	fmt.Printf("插入%d行数据\n", n)

	//八,GetRowBetween():从缓存中,获取指定的行,开始行-结束行.用于页面分页显示.
	fmt.Printf("八,GetRowNum():获取缓存中,start行到end行之间的数据.\n")
	rows := UsersCache.GetRowBetween(0, 15)
	for i, rowMap := range rows {
		fmt.Printf(" 第%d行 ", i)
		for k, v := range rowMap {
			fmt.Printf(" %s=%s, ", k, v)
		}
		fmt.Println()
	}

	//Goods表操作----------------------------------------------------------
	fmt.Println("以下是对Goods表操作.")
	rows = GoodsCache.GetRowBetween(0, 10)
	for i, rowMap := range rows {
		fmt.Printf(" 第%d行 ", i)
		for k, v := range rowMap {
			fmt.Printf(" %s=%s, ", k, v)
		}
		fmt.Println()
	}

	//Goods表操作----------------------------------------------------------
	fmt.Println()
	pkey = "50496c578bc54cb49b21513107fcbd10"
	result, err = GoodsCache.GetRow(pkey)
	if err != nil {
		fmt.Println(err)
	}
	for k, v := range result {
		fmt.Printf("%s=%s, ", k, v)
	}


	//测试启动RPC,防止退出
	wait:=make(chan struct{})
	<-wait
}

func prof() {
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
