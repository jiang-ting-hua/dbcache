package main

import (
	pb "dbcache/proto"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
)

//grpc服务名
const GrpcServiceName = "DBcacheGrpc"

type DBcacheGrpcClient struct {
	Client   pb.GrpcDBcacheClient
	grpcConn *grpc.ClientConn
}

//初始化连接Rpc服务.
func InitGrpc(address string) (grpcClient *DBcacheGrpcClient, err error) {
	//连接
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		err := fmt.Errorf("InitGrpc() Grpc error:%s", err)
		return nil, err
	}
	//实例化gprc客户端
	client := pb.NewGrpcDBcacheClient(conn)
	return &DBcacheGrpcClient{client, conn}, nil
}
func (d *DBcacheGrpcClient) Close() {
	d.grpcConn.Close()
}

//--------------GetRow()---------------------------------
//参数说明:tableName,缓存的表名,pkey:主键值.
func (d *DBcacheGrpcClient) GetRow(tableName string, pkey string) (result map[string]string, err error) {
	//组建请求参数
	req := pb.GetRowRequest{
		TableName: tableName,
		Pkey:      pkey,
	}
	//调用接口
	resp, err := d.Client.GetRow(context.Background(), &req)
	if err != nil {
		err = fmt.Errorf("grpc GetRow() error: %s", err)
		return nil, err
	}
	return resp.Result, nil
}

//--------------GetColumn()---------------------------------
//参数说明:tableName,缓存的表名,pkey:主键值,column:列值
func (d *DBcacheGrpcClient) GetColumn(tableName string, pkey string, column string) (result string, err error) {
	//组建请求参数
	req := pb.GetColumnRequest{
		TableName: tableName,
		Pkey:      pkey,
		Column:    column,
	}
	//调用接口
	resp, err := d.Client.GetColumn(context.Background(), &req)
	if err != nil {
		err = fmt.Errorf("grpc GetColumn() error: %s", err)
		return "", err
	}
	return resp.Result, nil
}

//--------------DelRow()---------------------------------
//参数说明:tableName,缓存的表名,pkey:主键值
func (d *DBcacheGrpcClient) DelRow(tableName string, pkey string) (result int64, err error) {
	//组建请求参数
	req := pb.DelRowRequest{
		TableName: tableName,
		Pkey:      pkey,
	}
	//调用接口
	resp, err := d.Client.DelRow(context.Background(), &req)
	if err != nil {
		err = fmt.Errorf("grpc DelRow() error: %s", err)
		return 0, err
	}
	return resp.Result, nil
}

//--------------GetWhere()---------------------------------
//参数说明:tableName,缓存的表名,pkey:主键值,where:查询条件
func (d *DBcacheGrpcClient) GetWhere(tableName string, where string) (result []map[string]string, err error) {
	//组建请求参数
	req := pb.GetWhereRequest{
		TableName: tableName,
		Where:     where,
	}
	//调用接口
	stream, err := d.Client.GetWhere(context.Background(), &req)
	if err != nil {
		err = fmt.Errorf("grpc GetWhere() error: %s", err)
		return nil, err
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		result = append(result, resp.Result.Result)
	}
	return result, nil
}

//--------------GetColumn()---------------------------------
//参数说明:tableName,缓存的表名,pkey:主键值.column:列名,value:列值
func (d *DBcacheGrpcClient) UpdateColumn(tableName string, pkey string, column string, value string) (n int64, err error) {
	//组建请求参数
	req := pb.UpdateColumnRequest{
		TableName:   tableName,
		Pkey:        pkey,
		Column:      column,
		ColumnValue: value,
	}
	//调用接口
	resp, err := d.Client.UpdateColumn(context.Background(), &req)
	if err != nil {
		err = fmt.Errorf("grpc UpdateColumn() error: %s", err)
		return 0, err
	}
	return resp.Result, nil
}

//--------------UpdateColumns()---------------------------------
//参数说明:tableName,缓存的表名,pkey:主键值.where:多列的组合表达式
func (d *DBcacheGrpcClient) UpdateColumns(tableName string, pkey string, where string) (n int64, err error) {
	//组建请求参数
	req := pb.UpdateColumnsRequest{
		TableName: tableName,
		Pkey:      pkey,
		Where:     where,
	}
	//调用接口
	resp, err := d.Client.UpdateColumns(context.Background(), &req)
	if err != nil {
		err = fmt.Errorf("grpc UpdateColumns() error: %s", err)
		return 0, err
	}
	return resp.Result, nil
}

//--------------InsertRow()---------------------------------
//参数说明:tableName,缓存的表名,condition:多列的组合表达式
func (d *DBcacheGrpcClient) InsertRow(tableName string, condition string) (n int64, err error) {
	//组建请求参数
	req := pb.InsertRowRequest{
		TableName: tableName,
		Condition: condition,
	}
	//调用接口
	resp, err := d.Client.InsertRow(context.Background(), &req)
	if err != nil {
		err = fmt.Errorf("grpc InsertRow() error: %s", err)
		return 0, err
	}
	return resp.Result, nil
}

//--------------GetRowBetween()---------------------------------
//参数说明:tableName,缓存的表名,pkey:主键值,where:查询条件
func (d *DBcacheGrpcClient) GetRowBetween(tableName string, start int, end int) (result []map[string]string, err error) {
	//组建请求参数
	req := pb.GetRowBetweenRequest{
		TableName: tableName,
		Start:     int64(start),
		End:       int64(end),
	}
	//调用接口
	stream, err := d.Client.GetRowBetween(context.Background(), &req)
	if err != nil {
		err = fmt.Errorf("grpc GetRowBetween() error: %s", err)
		return nil, err
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		result = append(result, resp.Result.Result)
	}
	return result, nil
}

//--------------GetPageCount()---------------------------------
//参数说明:tableName,缓存的表名,pageSize: 每页多少行
func (d *DBcacheGrpcClient) GetPageCount(tableName string, pageSize int) (result int, err error) {
	//组建请求参数
	req := pb.GetPageCountRequest{
		TableName: tableName,
		PageSize:  int64(pageSize),
	}
	//调用接口
	resp, err := d.Client.GetPageCount(context.Background(), &req)
	if err != nil {
		err = fmt.Errorf("grpc GetPageCount() error: %s", err)
		return 0, err
	}
	return int(resp.Result), nil
}

//--------------GetMultipageRows()---------------------------------
//参数说明:tableName,缓存的表名,startPage,开始页,pageNum多少页,pageSize参数是每页行数大小
func (d *DBcacheGrpcClient) GetMultipageRows(tableName string, startPage int, pageNum int, pageSize int) (result []map[string]string, err error) {
	//组建请求参数
	req := pb.GetMultipageRowsRequest{
		TableName: tableName,
		StartPage: int64(startPage),
		PageNum:   int64(pageNum),
		PageSize:  int64(pageSize),
	}
	//调用接口
	stream, err := d.Client.GetMultipageRows(context.Background(), &req)
	if err != nil {
		err = fmt.Errorf("grpc GetMultipageRows() error: %s", err)
		return nil, err
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		result = append(result, resp.Result.Result)
	}
	return result, nil
}

//--------------GetOnePageRows()---------------------------------
//参数说明:tableName,缓存的表名,page参数是页码,pageSize参数是每页行数大小
func (d *DBcacheGrpcClient) GetOnePageRows(tableName string, page int,pageSize int) (result []map[string]string, err error) {
	//组建请求参数
	req := pb.GetOnePageRowsRequest{
		TableName: tableName,
		Page: int64(page),
		PageSize:   int64(pageSize),
	}
	//调用接口
	stream, err := d.Client.GetOnePageRows(context.Background(), &req)
	if err != nil {
		err = fmt.Errorf("grpc GetOnePageRows() error: %s", err)
		return nil, err
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		result = append(result, resp.Result.Result)
	}
	return result, nil
}
