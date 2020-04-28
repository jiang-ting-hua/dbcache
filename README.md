## dbcache 适用于读多写少的情况.

### 用于数据库全表多字段的缓存. 可以缓存多张表.

### 使用说明:以下二步.

### 一. 配置程序目录下二个配置文件:

		config.conf(设置数据库连接和日志).users.sql和goods.sql这二个文件是测试数据.直接导入数据库.
		
		cache.conf(配置需要缓存的表及列,是否实时更新,还是异步更新数据库)

### 二. 在conf目录下,cache.go源文件中,增加对应表的结构体和方法.每一个需要缓存的表,都需要增加各自的结构体和方法.(里面有缓存二张数据库表的样例,users表和Goods表)


    注意:
	目前只支持主键为一个列.
	
	缓存数据可以是字符,整型,浮点,日期(datetime,timestamp).	
	(注意日期型,涉及时区,如果在连接时加上参数:&parseTime=true&loc=Local 返回数据带时区信息.暂时未做处理,这样不能保存.不带这二个参数,可以保存)
	parseTime是查询结果是否自动解析为时间. loc是MySQL的时区设置.
	
	目前如果有更新,先更新数据库,再更新缓存.如果实时更新,先更新数据库,再更新缓存.
	异步更新,会先把执行SQL语句保存于当前目录下的文件async_sql.sql,再更新数据库,如果更新失败,会把失败的sql的语句保存于async_sql_failed.sql文件.
	
	支持日志系统: s 标准输出屏幕, f 记录到日志, e 发送邮件, a 所有(包括s,f,e)(需先在配置文件config.conf中配置),
	等级说明:1 DEBUG,2 TRACE,3 INFO,4 WARNING,5 ERROR,6 FATAL
	注意二个开关,enable控制是否开启,run_level运行等级,只在大于或等于设置等级才输出日志.)
	如有什么问题,讨论可联系:38704889@qq.com

## 主要函数,具体使用方法,详见main.go文件中:

	1. GetRow():根据主键值,取得该行数据
	
	2. GetColumn():根据主键,取得某列的数据
	
	3. DelRow():根据主键,删除该行数据
	
    4. GetWhere():根据where条件,查询缓存中所有符合条件的行.不用加引号
    
	5. UpdateColumn():根据主键,更新一列
	
	6. UpdateColumns():根据主键,更新多列
	
	7. InsertRow():插入一行数据
	
	8. GetRowBetween(0, 100):从缓存中,获取指定的行,开始行-结束行.用于页面分页显示.


### cache.conf配置文件(样例,users表和Goods表)

	#数据库goods表
	[Goods]
	table_name=goods
	columns=goods_id,type_id,type_name,goods_name,description,qty,price,create_date,update_date
	pkey=goods_id
	pkey_auto_increment = false
	where=
	orther=order by price asc
	#用于分页查询,缓存类型:一.slice切片(按orther里排序),二.sliceNotDel切片(不删除,只记录,速度最快,但后插入数据未排序),三.link链表
	cache_type=sliceNotDel
	#是否同步更新,true:实时更新,false:异步更新.
	is_realtime = false
	#异步更新,是否等待返回结果(上面条件是is_realtime = false时)
	is_wait_result = true

	#数据库users表
	[Users]
	table_name=users
	columns=uid,age,price,name,address,password,create_date,update_date
	pkey=uid
	pkey_auto_increment = false
	where=
	orther=order by age desc
	#用于分页查询,缓存类型:一.slice切片(按orther里排序),二.sliceNotDel切片(不删除,只记录,速度最快,但后插入数据未排序),三.link链表
	cache_type=slice
	#是否同步更新,true:实时更新,false:异步更新.
	is_realtime = false
	#异步更新,是否等待返回结果(上面条件是is_realtime = false时)
	is_wait_result = true
	


