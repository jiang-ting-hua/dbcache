# dbcache
# 数据库中表数据缓存在内存中, 加快读取速度,并减少数据库操作.
### go语言写的, 用于数据库全表多字段的缓存. (可以数据库一张表,指定要缓存的列.) 数据库中表数据缓存在内存中, 加快读取速度,并减少数据库操作.是支持高并发读写. 适用于在读多,写少的情况下.支持数据库实时同步和异步同步.

####  一 .  要先配置文件config.conf(连接数据库) 和 文件cache.conf(配置需要缓存的表及列,实时同步更新,还是异步更新数据库).

####  二 .  users.sql这个有建表结构和数据.目前只在mysql上测试了的.

####  三 .  目前只支持主键为一个列,并且不能重复,也可以用自增列做为唯一键.

####  四 .  缓存数据可以是字符,整型,浮点,日期(datetime,timestamp).(注意日期型,涉及时区,如果在连接时加上参数:&parseTime=true&loc=Local 返 		回数据带时区信息.暂时未做处理,这样不能保存,不带这二个参数,可以保存).parseTime是查询结果是否自动解析为时间. loc是MySQL的时区设	      置.可以改为带时区.存储为time.time类型.

####  五 . 目前如果有更新,先更新数据库,再更新缓存.注意:更新数据库,分为实时更新同步数据库和异步更新数据库,在cache.conf文件中配置.如果实时	    更新,先更新数据库,再更新缓存.异步更新,会先把数据保存于当前目录下的文件async_sql.sql,再更新数据库,如果更新失败,会把失败的sql的		 语句保存于async_sql_failed.sql文件.

####  六 . 支持日志系统: s 标准输出屏幕, f 记录到日志, e 发送邮件, a 所有(包括s,f,e)(需先在配置文件config.conf中配置),等级说明:1 DEBUG,2 	    TRACE,3 INFO,4 WARNING,5 ERROR,6 FATAL. 注意二个开关,enable控制是否开启,run_level运行等级,只在大于或等于设置等级才输出日志.)


###  注意二个开关,enable控制是否开启,run_level运行等级,只在大于或等于设置等级才输出日志.)
如有什么问题,讨论可联系:38704889@qq.com

### 以下是使用方法.

	初始化日志库,需设置配置文件config.conf
	logs.InitLog()
	defer logs.Close()

	连接数据库,初始化调用.需设置配置文件config.conf
	db, err := dbcache.ConnectDB()

	初始化需要缓存的数据,把数据缓存在内存中.是根据配置文件cache.conf需要缓存的表及列
	rowNum, err := dbcache.InitCache(db)


### 以下为增删改查的样例.具体在main.go中.

	一. GetRow:根据主键值,取得该行数据
	
	pkey := "21111111"
	result, err := dbcache.GetRow(pkey)
	

	二. GetColumn:根据主键,取得某列的数据
	
	v, err := dbcache.GetColumn("21111111", "name")


	三. DelRow:根据主键,删除该行数据
	
	pkey = "294SKHRA7548PXFFTTLJ"
	n, err := dbcache.DelRow(pkey)


	四. GetWhere:根据where条件,查询缓存中所有符合条件的行.不用加引号
	
	value, err = dbcache.GetWhere("address=重庆 and password=888888")


	五. UpdateColumn:根据主键,更新一列
	
	pkey = "21111111"
	col := "name"
	val := "王明"
	n, err = dbcache.UpdateColumn(pkey, col, val)


	六. UpdateColumns:根据主键,更新多列
	
	pkey = "21111111"
	cols := "name=李平,address= 重庆"
	n, err = dbcache.UpdateColumns(pkey, cols)

	七. InsertRow:插入一行数据
	
	insert := "uid=21111111,name=jth,address=重庆,password=888888,age=33,price=66.123456,create_date=2020-02-02 02:02:02,update_date=2020-01-01 01:01:01"

        八,GetRowBetween():从缓存中,获取指定的行,开始行-结束行.用于页面分页显示.
	rows := dbcache.GetRowBetween(0,10)


