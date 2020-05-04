package main

import (
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
)
//rpc服务名
const RpcServiceName="DBcache"

type DBcacheRpcClient struct{
	Conn *rpc.Client
}
//初始化连接Rpc服务.
func InitRpc(rpcType string,protocol string,address string)(rpcClient *DBcacheRpcClient,err error){
	if rpcType=="rpc"{
		client, err := rpc.DialHTTP(protocol, address)
		if err != nil {
			err:=fmt.Errorf("InitRpc() rpc error:%s", err)
			return nil,err
		}
		return &DBcacheRpcClient{client},nil
	}else if rpcType=="jsonrpc"{
		conn, err := net.Dial(protocol, address)
		if err != nil {
			err:=fmt.Errorf("InitRpc() jsonrpc error:%s", err)
			return nil,err
		}
        client := jsonrpc.NewClient(conn)
		return &DBcacheRpcClient{client},nil
	}
	return nil,nil
}

//--------------GetRow()---------------------------------
//GetRow,client请求的参数.
type GetRowRequest struct{
	TableName string
	Pkey string
}
//GetRow,响应的结果.
type GetRowResponse struct{
	Result map[string]string

}
//参数说明:tableName,缓存的表名,pkey:主键值.
func (d *DBcacheRpcClient)GetRow(tableName string,pkey string)(result map[string]string,err error){
	req := GetRowRequest{tableName, pkey}
	resp:= GetRowResponse{make(map[string]string)}
	err = d.Conn.Call(RpcServiceName+".GetRow", req, &resp)
	if err != nil {
		err=fmt.Errorf("GetRow() error: %s", err)
		return nil,err
	}
	return resp.Result,nil
}

//--------------GetColumn()---------------------------------
type GetColumnRequest struct{
	TableName string
	Pkey string
	Column string
}
type GetColumnResponse struct{
	Result string
}
func (d *DBcacheRpcClient)GetColumn(tableName string,pkey string, column string)(result string, err error){
	req := GetColumnRequest{tableName, pkey,column}
	resp:= GetColumnResponse{}
	err = d.Conn.Call(RpcServiceName+".GetColumn", req, &resp)
	if err != nil {
		err=fmt.Errorf("GetColumn() error: %s", err)
		return "",err
	}
	return resp.Result,nil
}

//--------------DelRow()---------------------------------
type DelRowRequest struct{
	TableName string
	Pkey string
}
type DelRowResponse struct{
	Result int64
}
func (d *DBcacheRpcClient)DelRow(tableName string,pkey string) (n int64, err error){
	req := DelRowRequest{tableName, pkey}
	resp:= DelRowResponse{}
	err = d.Conn.Call(RpcServiceName+".DelRow", req, &resp)
	if err != nil {
		err=fmt.Errorf("DelRow() error: %s", err)
		return 0,err
	}
	return resp.Result,nil
}

//--------------GetWhere()---------------------------------
type GetWhereRequest struct{
	TableName string
	Where string
}
type GetWhereResponse struct{
	Result []map[string]string
}
func (d *DBcacheRpcClient)GetWhere(tableName string,where string) (result []map[string]string, err error){
	req := GetWhereRequest{tableName, where}
	resp:= GetWhereResponse{make([]map[string]string,0)}
	err = d.Conn.Call(RpcServiceName+".GetWhere", req, &resp)
	if err != nil {
		err=fmt.Errorf("GetWhere() error: %s", err)
		return nil,err
	}
	return resp.Result,nil
}
//--------------UpdateColumn()---------------------------------
type UpdateColumnRequest struct{
	TableName string
	Pkey string
	Column string
	ColumnValue string
}
type UpdateColumnResponse struct{
	Result int64
}
func (d *DBcacheRpcClient)UpdateColumn(tableName string,Pkey string, column string, value string) (n int64, err error){
	req := UpdateColumnRequest{tableName, Pkey,column,value}
	resp:= UpdateColumnResponse{}
	err = d.Conn.Call(RpcServiceName+".UpdateColumn", req, &resp)
	if err != nil {
		err=fmt.Errorf("UpdateColumn() error: %s", err)
		return 0,err
	}
	return resp.Result,nil
}

//--------------UpdateColumns()---------------------------------
type UpdateColumnsRequest struct{
	TableName string
	Pkey string
	Where string
}
type UpdateColumnsResponse struct{
	Result int64
}
func (d *DBcacheRpcClient)UpdateColumns(tableName string,Pkey string, where string) (n int64, err error){
	req := UpdateColumnsRequest{tableName, Pkey,where}
	resp:= UpdateColumnsResponse{}
	err = d.Conn.Call(RpcServiceName+".UpdateColumns", req, &resp)
	if err != nil {
		err=fmt.Errorf("UpdateColumns() error: %s", err)
		return 0,err
	}
	return resp.Result,nil
}

//--------------InsertRow()---------------------------------
type InsertRowRequest struct{
	TableName string
	Condition string
}
type InsertRowResponse struct{
	Result int64
}
func (d *DBcacheRpcClient)InsertRow(tableName string,condition string) (n int64, err error){
	req := InsertRowRequest{tableName, condition}
	resp:= InsertRowResponse{}
	err = d.Conn.Call(RpcServiceName+".InsertRow", req, &resp)
	if err != nil {
		err=fmt.Errorf("InsertRow() error: %s", err)
		return 0,err
	}
	return resp.Result,nil
}

//--------------GetRowBetween()---------------------------------
type GetRowBetweenRequest struct{
	TableName string
	Start int
	End int
}
type GetRowBetweenResponse struct{
	Result []map[string]string
}
func (d *DBcacheRpcClient)GetRowBetween(tableName string,start int,end int) (result []map[string]string, err error){
	req := GetRowBetweenRequest{tableName, start,end}
	resp:= GetRowBetweenResponse{make([]map[string]string,0)}
	err = d.Conn.Call(RpcServiceName+".GetRowBetween", req, &resp)
	if err != nil {
		err=fmt.Errorf("GetRowBetweenRpc() error: %s", err)
		return nil,err
	}
	return resp.Result,nil
}