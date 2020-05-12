package main

import "fmt"

func main() {
	//初始化连接grpc
	grpcClient, err := InitGrpc( "127.0.0.1:9998")
	if err != nil {
		fmt.Println("InitGrpc() error: ", err)
		return
	}
	defer grpcClient.Close()

	//以下是样例:
	//一. GetRow:根据主键值,取得该行数据
	fmt.Println("一. GetRow().根据主键,取得该行数据.")
	result, err := grpcClient.GetRow("users", "00YS0SW2N4NT7K8HP13E")
	if err!=nil{
		fmt.Println(err)
		return
	}
	for k,v :=range result{
		fmt.Println(k+"="+v)
	}

	//二. GetColumn:根据主键,取得某列的数据
	fmt.Println("二. GetColumn().根据主键,取得某列的数据")
	v, err := grpcClient.GetColumn("users","00YS0SW2N4NT7K8HP13E", "name")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("根据主键，取得某列的数据: ", v)


	//三. DelRow:根据主键,删除该行数据
	fmt.Printf("三. DelRow().根据主键,删除该行数据\n")
	pkey := "013CIHW1G6HX8D6Q4H1Q"
	n, err := grpcClient.DelRow("users",pkey)
	if err != nil {
		fmt.Printf("根据主键,删除行数据失败, err: %s", err)
	}
	fmt.Println("删除数据行数:", n)

	//四. GetWhere:根据where条件,查询缓存中所有符合条件的行.不用加引号
	fmt.Println("四. 根据where条件,查询缓存中所有符合条件的行.")
	var value []map[string]string
	//value ,_ = dbcache.GetWhere("name=AFA5Y9FB or password=Q80BJT")
	value, err = grpcClient.GetWhere("users","address=重庆 and password=888888")
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

	//五. UpdateColumn:根据主键,更新一列
	fmt.Println("五. UpdateColumn().根据主键,更新一列数据")
	pkey = "00YS0SW2N4NT7K8HP13E"
	col := "name"
	val := "王明99881"
	n, err = grpcClient.UpdateColumn("users",pkey, col, val)
	if err != nil {
		fmt.Printf( "更新一列错误, err: %s", err)
	}
	fmt.Println("UpdateColumn()更新列数:",n)

	//六. UpdateColumns:根据主键,更新多列
	fmt.Printf("六. UpdateColumns().根据主键,更新多列数据:\n")
	pkey = "00YS0SW2N4NT7K8HP13E"
	cols := "age=111,address= 重庆111"
	n, err = grpcClient.UpdateColumns("users",pkey, cols)
	if err != nil {
		fmt.Printf("更新多列错误, err:%s", err)
	}
	fmt.Println("UpdateColumns()更新列数:",n)


	//七. InsertRow():插入一行数据
	fmt.Printf("七. InsertRow().插入一行数据\n")
	insert := "uid=22222211117,name=jth,address=重庆,password=888888,age=9999993,price=66.123456,create_date=2020-02-02 02:02:02,update_date=2020-01-01 01:01:01"
	n, err = grpcClient.InsertRow("users",insert)
	if err != nil {
		fmt.Printf( "插入错误, err:%s", err)
	}
	fmt.Printf("插入%d行数据\n", n)

	//八,GetRowBetween():从缓存中,获取指定的行,开始行-结束行.用于页面分页显示.
	fmt.Printf("八,GetRowNum():获取缓存中,start行到end行之间的数据.\n")
	rows ,err:= grpcClient.GetRowBetween("users",0, 15)
	if err!=nil{
		fmt.Println(err)
	}
	for i, rowMap := range rows {
		fmt.Printf(" 第%d行 ", i)
		for k, v := range rowMap {
			fmt.Printf(" %s=%s, ", k, v)
		}
		fmt.Println()
	}
	//九,GetPageCount():获取总页数.用于页面分页显示.
	fmt.Printf("九,GetPageCount():获取总页数.\n")
	pageSize:=15  //每页15行
	page,err:= grpcClient.GetPageCount("users",pageSize)
	if err!=nil{
		fmt.Println(err)
	}
	fmt.Printf("九,GetPageCount():每页%d行,获取总页数:%d\n",pageSize,page)

	//十,GetMultipageRows():用于分页,根据指定开始页,获取多少页,每页行数.返回数据.
	//参数说明:startPage,开始页,pageNum多少页,pageSize参数是每页行数大小
	fmt.Printf("十 , GetMultipageRows():用于分页,根据指定开始页,获取多少页,每页行数.返回数据.\n")
	rows ,err = grpcClient.GetMultipageRows("users",5, 2,10)
	if err!=nil{
		fmt.Println(err)
	}
	for i, rowMap := range rows {
		fmt.Printf(" 第%d行 ", i)
		for k, v := range rowMap {
			fmt.Printf(" %s=%s, ", k, v)
		}
		fmt.Println()
	}

	//十一,GetOnePageRows():用于分页,根据页码和每页行数大小,返回数据.page参数是页码,pageSize参数是每页行数大小
	//参数说明:page参数是页码,pageSize参数是每页行数大小
	fmt.Printf("十 , GetOnePageRows():用于分页,根据页码和每页行数大小,返回数据.\n")
	rows ,err = grpcClient.GetOnePageRows("users",5, 10)
	if err!=nil{
		fmt.Println(err)
	}
	for i, rowMap := range rows {
		fmt.Printf(" 第%d行 ", i)
		for k, v := range rowMap {
			fmt.Printf(" %s=%s, ", k, v)
		}
		fmt.Println()
	}
}