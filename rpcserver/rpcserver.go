package rpcserver

import (
	"dbcache/conf"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"net/rpc/jsonrpc"
)
var (
	RpcConf=conf.RpcServer{}
)

func RpcRun()(err error){
	//读取配置文件,初始化配置信息
	err = conf.ParseConf(conf.CONFIG_FILE, &RpcConf)
	if err != nil {
		return  err
	}
	//注册服务
	rpc.Register(new(DBcache))

	if RpcConf.RpcType=="rpc"{      //rpc采用http协议作为rpc载体
		rpc.HandleHTTP()
		//fmt.Println("rpc http server: "+RpcConf.Ip+":"+RpcConf.Port)
		//err = http.ListenAndServe(RpcConf.Ip+":"+RpcConf.Port, nil)
		//if err!=nil{
		//	return err
		//}

		rpcListen, err := net.Listen(RpcConf.Protocol, RpcConf.Ip+":"+RpcConf.Port)
		if err != nil {
			return err
		}
	    //另外开一个协程处理
		go func(rpcListen net.Listener){
			fmt.Println("rpc server: "+RpcConf.Ip+":"+RpcConf.Port)
			http.Serve(rpcListen, nil)
		}(rpcListen)

	}else if RpcConf.RpcType=="jsonrpc"{  //jsonrpc基于tcp协议,实现JSON进行数据编解码,，因而支持跨语言tcp调用。

		rpcListen, err := net.Listen(RpcConf.Protocol, RpcConf.Ip+":"+RpcConf.Port)
		if err != nil {
			return err
		}
		//另外开一个协程并发处理
		go func(rpcListen net.Listener){
			fmt.Println("json rpc tcp server: "+RpcConf.Ip+":"+RpcConf.Port)
			for {
				conn, err := rpcListen.Accept() // 接收客户端连接请求
				if err != nil {
					fmt.Println("json rpc tcp server : err:",err)
					continue
				}
				go func(conn net.Conn) { // 并发处理客户端请求
					fmt.Println("json rpc,new client in coming")
					jsonrpc.ServeConn(conn)
				}(conn)
			}
		}(rpcListen)
	}
	return nil
}

