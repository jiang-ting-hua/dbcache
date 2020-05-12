package grpcserver

import (
	"context"
	"dbcache/cache"
	pb "dbcache/proto"
	"fmt"
)

//定义服务对象,实现pb的GrpcDBcacheServer接口
type DBcacheGrpc struct{}

//GetRow方法
func (d *DBcacheGrpc) GetRow(ctx context.Context, req *pb.GetRowRequest) (resp *pb.GetRowResponse, err error) {
	cacheObj, ok := cache.CacheObj[req.TableName]
	if !ok {
		err = fmt.Errorf("%s,The Table is not cache.", req.TableName)
		return nil, err
	}
	result, err := cacheObj.GetRow(req.Pkey)
	if err != nil {
		return nil, err
	}
	resp = &pb.GetRowResponse{
		Result: result,
	}
	return resp, nil
}
//GetRow方法 GetColumn(context.Context, *GetColumnRequest) (*GetColumnResponse, error)
func (d *DBcacheGrpc) GetColumn(ctx context.Context, req *pb.GetColumnRequest) (resp *pb.GetColumnResponse, err error) {
	cacheObj, ok := cache.CacheObj[req.TableName]
	if !ok {
		err = fmt.Errorf("%s,The Table is not cache.", req.TableName)
		return nil, err
	}
	result, err := cacheObj.GetColumn(req.Pkey,req.Column)
	if err != nil {
		return nil, err
	}
	resp = &pb.GetColumnResponse{
		Result: result,
	}
	return resp, nil
}
//DelRow方法
func (d *DBcacheGrpc) DelRow(ctx context.Context, req *pb.DelRowRequest) (resp *pb.DelRowResponse, err error) {
	cacheObj, ok := cache.CacheObj[req.TableName]
	if !ok {
		err = fmt.Errorf("%s,The Table is not cache.", req.TableName)
		return nil, err
	}
	result, err := cacheObj.DelRow(req.Pkey)
	if err != nil {
		return nil, err
	}
	resp = &pb.DelRowResponse{
		Result: result,
	}
	return resp, nil
}

//GetWhere方法
func (d *DBcacheGrpc) GetWhere(req *pb.GetWhereRequest, stream pb.GrpcDBcache_GetWhereServer) (err error) {
	cacheObj, ok := cache.CacheObj[req.TableName]
	if !ok {
		err = fmt.Errorf("%s,The Table is not cache.", req.TableName)
		return err
	}
	result, err := cacheObj.GetWhere(req.Where)
	if err != nil {
		return err
	}
	for _, v := range result {
		err := stream.Send(&pb.GetWhereResponse{
			Result: &pb.GetWhereStream{Result: v,},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

//UpdateColumn方法
func (d *DBcacheGrpc) UpdateColumn(ctx context.Context, req *pb.UpdateColumnRequest) (resp *pb.UpdateColumnResponse, err error) {
	cacheObj, ok := cache.CacheObj[req.TableName]
	if !ok {
		err = fmt.Errorf("%s,The Table is not cache.", req.TableName)
		return nil, err
	}
	result, err := cacheObj.UpdateColumn(req.Pkey, req.Column, req.ColumnValue)
	if err != nil {
		return nil, err
	}
	resp = &pb.UpdateColumnResponse{
		Result: result,
	}
	return resp, nil
}

//UpdateColumns
func (d *DBcacheGrpc) UpdateColumns(ctx context.Context, req *pb.UpdateColumnsRequest) (resp *pb.UpdateColumnsResponse, err error) {
	cacheObj, ok := cache.CacheObj[req.TableName]
	if !ok {
		err = fmt.Errorf("%s,The Table is not cache.", req.TableName)
		return nil, err
	}
	result, err := cacheObj.UpdateColumns(req.Pkey, req.Where)
	if err != nil {
		return nil, err
	}
	resp = &pb.UpdateColumnsResponse{
		Result: result,
	}
	return resp, nil
}

//InsertRow
func (d *DBcacheGrpc) InsertRow(ctx context.Context, req *pb.InsertRowRequest) (resp *pb.InsertRowResponse, err error) {
	cacheObj, ok := cache.CacheObj[req.TableName]
	if !ok {
		err = fmt.Errorf("%s,The Table is not cache.", req.TableName)
		return nil, err
	}
	result, err := cacheObj.InsertRow(req.Condition)
	if err != nil {
		return nil, err
	}
	resp = &pb.InsertRowResponse{
		Result: result,
	}
	return resp, nil
}

//GetRowBetween方法
func (d *DBcacheGrpc) GetRowBetween(req *pb.GetRowBetweenRequest, stream pb.GrpcDBcache_GetRowBetweenServer) (err error) {
	cacheObj, ok := cache.CacheObj[req.TableName]
	if !ok {
		err = fmt.Errorf("%s,The Table is not cache.", req.TableName)
		return err
	}
	result := cacheObj.GetRowBetween(int(req.Start), int(req.End))
	if len(result) == 0 {
		return nil
	}
	for _, v := range result {
		err := stream.Send(&pb.GetRowBetweenResponse{
			Result: &pb.GetRowBetweentream{Result: v,},
		})
		if err != nil {
			return err
		}
	}
	return nil
}
//GetPageCount方法
func (d *DBcacheGrpc) GetPageCount(ctx context.Context,req *pb.GetPageCountRequest) (resp *pb.GetPageCountResponse,err error) {
	cacheObj, ok := cache.CacheObj[req.TableName]
	if !ok {
		err = fmt.Errorf("%s,The Table is not cache.", req.TableName)
		return nil,err
	}
	result := cacheObj.GetPageCount(int(req.PageSize))
	resp = &pb.GetPageCountResponse{
		Result: int64(result),
	}
	return resp, nil
}

//GetMultipageRows方法
func (d *DBcacheGrpc) GetMultipageRows(req *pb.GetMultipageRowsRequest, stream pb.GrpcDBcache_GetMultipageRowsServer) (err error) {
	cacheObj, ok := cache.CacheObj[req.TableName]
	if !ok {
		err = fmt.Errorf("%s,The Table is not cache.", req.TableName)
		return err
	}
	result := cacheObj.GetMultipageRows(int(req.StartPage), int(req.PageNum),int(req.PageSize))
	if len(result) == 0 {
		return nil
	}
	for _, v := range result {
		err := stream.Send(&pb.GetMultipageRowsResponse{
			Result: &pb.GetMultipageRowstream{Result: v,},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

//GetOnePageRows方法
func (d *DBcacheGrpc) GetOnePageRows(req *pb.GetOnePageRowsRequest, stream pb.GrpcDBcache_GetOnePageRowsServer) (err error) {
	cacheObj, ok := cache.CacheObj[req.TableName]
	if !ok {
		err = fmt.Errorf("%s,The Table is not cache.", req.TableName)
		return err
	}
	result := cacheObj.GetOnePageRows(int(req.Page), int(req.PageSize))
	if len(result) == 0 {
		return nil
	}
	for _, v := range result {
		err := stream.Send(&pb.GetOnePageRowsResponse{
			Result: &pb.GetOnePageRowstream{Result: v,},
		})
		if err != nil {
			return err
		}
	}
	return nil
}