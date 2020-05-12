package grpcserver

import (
	"dbcache/conf"
	pb "dbcache/proto"
	"fmt"
	"google.golang.org/grpc"
	"net"
)

var (
	GrpcConf=conf.GrpcServer{}
)

func GrpcRun()(err error){
	//读取配置文件,初始化配置信息
	err = conf.ParseConf(conf.CONFIG_FILE, &GrpcConf)
	if err != nil {
		return  err
	}
	//监听
	grpcListen, err := net.Listen(GrpcConf.Protocol, GrpcConf.Ip+":"+GrpcConf.Port)
	if err != nil {
		return err
	}
	//实例化grpc服务
	Server := grpc.NewServer()
	//在grpc上注册服务
	pb.RegisterGrpcDBcacheServer(Server,new(DBcacheGrpc))
	//启动服务器
	//另外开一个协程处理
	go func(rpcListen net.Listener){
		fmt.Println("grpc server: "+GrpcConf.Ip+":"+GrpcConf.Port)
		Server.Serve(grpcListen)
	}(grpcListen)

	return nil
}