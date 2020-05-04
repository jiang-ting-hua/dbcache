package main

import (
	"fmt"
)

func main(){
	//当rpc服务器是类型是jsonrpc时.JSON RPC采用JSON进行数据编解码，因而支持跨语言调用。但目前的jsonrpc库是基于tcp协议实现的，暂时不支持使用http进行数据传输。
	//当rpc服务器是类型是rpc时.使用encoding/gob进行编解码,只能go语言调用.由于其他语言不支持gob编解码方式，所以使用net/rpc库实现的RPC方法没办法进行跨语言调用。

	//InitRpc()函数第一个参数:rpcType有二个选项:rpc和jsonrpc.
	//rpcClient, err := InitRpc("jsonrpc","tcp", "127.0.0.1:9999")
	rpcClient, err := InitRpc("rpc","tcp", "127.0.0.1:9999")
	if err != nil {
		fmt.Println("InitRpc() error: ", err)
		return
	}
	defer rpcClient.Conn.Close()
//以下是样例:
//--------------GetRow()---------------------------------
	//一,GetRowRpc(),根据表名,主键值,取得缓存中,该行的数据.
	fmt.Println("一. GetRow().根据主键,取得该行数据.")
	result, err := rpcClient.GetRow("users", "00YS0SW2N4NT7K8HP13E")
	if err!=nil{
		fmt.Println(err)
		return
	}
	for k,v :=range result{
		fmt.Println(k+"="+v)
	}
//--------------GetColumn()---------------------------------
	//二. GetColumnRpc:根据主键,取得某列的数据
	fmt.Println("二. GetColumn().根据主键,取得某列的数据")
	v, err := rpcClient.GetColumn("users","00YS0SW2N4NT7K8HP13E", "name")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("根据主键，取得某列的数据: ", v)
//--------------DelRow()---------------------------------
	//三. DelRow:根据主键,删除该行数据
	fmt.Printf("三. DelRow().根据主键,删除该行数据\n")
	pkey := "013CIHW1G6HX8D6Q4H1Q"
	n, err := rpcClient.DelRow("users",pkey)
	if err != nil {
		fmt.Println("根据主键,删除行数据失败, err: %s", err)
	}
	fmt.Println("删除数据行数:", n)
//--------------GetWhere()---------------------------------
	//四. GetWhere:根据where条件,查询缓存中所有符合条件的行.不用加引号
	fmt.Println("四. 根据where条件,查询缓存中所有符合条件的行.")
	var value []map[string]string
	//value ,_ = dbcache.GetWhere("name=AFA5Y9FB or password=Q80BJT")
	value, err = rpcClient.GetWhere("users","address=重庆 and password=888888")
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
//--------------UpdateColumn()---------------------------------
	//五. UpdateColumn:根据主键,更新一列
	fmt.Println("五. UpdateColumn().根据主键,更新一列数据")
	pkey = "00YS0SW2N4NT7K8HP13E"
	col := "name"
	val := "王明99"
	n, err = rpcClient.UpdateColumn("users",pkey, col, val)
	if err != nil {
		fmt.Println("更新一列错误, err:", err)
	}
	fmt.Printf("更新%d列\n", n)
//--------------UpdateColumns()---------------------------------
	//六. UpdateColumns:根据主键,更新多列
	fmt.Printf("六. UpdateColumns().根据主键,更新多列数据:\n")
	pkey = "00YS0SW2N4NT7K8HP13E"
	cols := "age=111,address= 重庆111"
	n, err = rpcClient.UpdateColumns("users",pkey, cols)
	if err != nil {
		fmt.Println( "更新多列错误, err:", err)
	}
	fmt.Printf("更新%d列\n", n)
//--------------InsertRow()---------------------------------
	//七. InsertRow():插入一行数据
	fmt.Printf("七. InsertRow().插入一行数据\n")
	insert := "uid=22222211117,name=jth,address=重庆,password=888888,age=9999993,price=66.123456,create_date=2020-02-02 02:02:02,update_date=2020-01-01 01:01:01"
	n, err = rpcClient.InsertRow("users",insert)
	if err != nil {
		fmt.Println(  "插入错误, err:", err)
	}
	fmt.Printf("插入%d行数据\n", n)

//--------------GetRowBetween()---------------------------------
	//八,GetRowBetween():从缓存中,获取指定的行,开始行-结束行.用于页面分页显示.
	fmt.Printf("八,GetRowNum():获取缓存中,start行到end行之间的数据.\n")
	rows,err := rpcClient.GetRowBetween("users",0, 15)
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