package rpcserver

import (
	"dbcache/cache"
	"fmt"
)

type DBcache struct{
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
//GetRow()
func (g *DBcache)GetRow(req GetRowRequest,resp *GetRowResponse)(err error){
	cacheObj,ok := cache.CacheObj[req.TableName]
	if !ok{
		err = fmt.Errorf("%s,The Table is not cache.",req.TableName)
		return err
	}
	result, err := cacheObj.GetRow(req.Pkey)
	if err!=nil{
		return err
	}
	resp.Result=result
	return nil
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
func (g *DBcache)GetColumn(req GetColumnRequest,resp *GetColumnResponse)(err error){
	cacheObj,ok := cache.CacheObj[req.TableName]
	if !ok{
		err = fmt.Errorf("%s,The Table is not cache.",req.TableName)
		return err
	}
	result, err := cacheObj.GetColumn(req.Pkey,req.Column)
	if err!=nil{
		return err
	}
	resp.Result=result
	return nil
}

//--------------DelRow()---------------------------------
type DelRowRequest struct{
	TableName string
	Pkey string
}
type DelRowResponse struct{
	Result int64
}
func (g *DBcache)DelRow(req DelRowRequest,resp *DelRowResponse)(err error){
	cacheObj,ok := cache.CacheObj[req.TableName]
	if !ok{
		err = fmt.Errorf("%s,The Table is not cache.",req.TableName)
		return err
	}
	result, err := cacheObj.DelRow(req.Pkey)
	if err!=nil{
		return err
	}
	resp.Result=result
	return nil
}

//--------------GetWhere()---------------------------------
type GetWhereRequest struct{
	TableName string
	Where string
}
type GetWhereResponse struct{
	Result []map[string]string
}
func (g *DBcache)GetWhere(req GetWhereRequest,resp *GetWhereResponse)(err error){
	cacheObj,ok := cache.CacheObj[req.TableName]
	if !ok{
		err = fmt.Errorf("%s,The Table is not cache.",req.TableName)
		return err
	}
	result, err := cacheObj.GetWhere(req.Where)
	if err!=nil{
		return err
	}
	resp.Result=result
	return nil
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
func (g *DBcache)UpdateColumn(req UpdateColumnRequest,resp *UpdateColumnResponse)(err error){
	cacheObj,ok := cache.CacheObj[req.TableName]
	if !ok{
		err = fmt.Errorf("%s,The Table is not cache.",req.TableName)
		return err
	}
	result, err := cacheObj.UpdateColumn(req.Pkey,req.Column,req.ColumnValue)
	if err!=nil{
		return err
	}
	resp.Result=result
	return nil
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
func (g *DBcache)UpdateColumns(req UpdateColumnsRequest,resp *UpdateColumnsResponse)(err error){
	cacheObj,ok := cache.CacheObj[req.TableName]
	if !ok{
		err = fmt.Errorf("%s,The Table is not cache.",req.TableName)
		return err
	}
	result, err := cacheObj.UpdateColumns(req.Pkey,req.Where)
	if err!=nil{
		return err
	}
	resp.Result=result
	return nil
}

//--------------InsertRow()---------------------------------
type InsertRowRequest struct{
	TableName string
	Condition string
}
type InsertRowResponse struct{
	Result int64
}
func (g *DBcache)InsertRow(req InsertRowRequest,resp *InsertRowResponse)(err error){
	cacheObj,ok := cache.CacheObj[req.TableName]
	if !ok{
		err = fmt.Errorf("%s,The Table is not cache.",req.TableName)
		return err
	}
	result, err := cacheObj.InsertRow(req.Condition)
	if err!=nil{
		return err
	}
	resp.Result=result
	return nil
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
func (g *DBcache)GetRowBetween(req GetRowBetweenRequest,resp *GetRowBetweenResponse)(err error){
	cacheObj,ok := cache.CacheObj[req.TableName]
	if !ok{
		err = fmt.Errorf("%s,The Table is not cache.",req.TableName)
		return err
	}
	result := cacheObj.GetRowBetween(req.Start,req.End)

	resp.Result=result
	return nil
}

//--------------GetPageCount()---------------------------------
type GetPageCountRequest struct{
	TableName string
	PageSize int
}
type GetPageCountResponse struct{
	Result int
}
func (g *DBcache)GetPageCount(req GetPageCountRequest,resp *GetPageCountResponse)(err error){
	cacheObj,ok := cache.CacheObj[req.TableName]
	if !ok{
		err = fmt.Errorf("%s,The Table is not cache.",req.TableName)
		return err
	}
	result := cacheObj.GetPageCount(req.PageSize)

	resp.Result=result
	return nil
}

//--------------GetMultipageRows()---------------------------------
type GetMultipageRowsRequest struct{
	TableName string
	StartPage int
	PageNum int
	PageSize int
}
type GetMultipageRowsResponse struct{
	Result []map[string]string
}
func (g *DBcache)GetMultipageRows(req GetMultipageRowsRequest,resp *GetMultipageRowsResponse)(err error){
	cacheObj,ok := cache.CacheObj[req.TableName]
	if !ok{
		err = fmt.Errorf("%s,The Table is not cache.",req.TableName)
		return err
	}
	result := cacheObj.GetMultipageRows(req.StartPage,req.PageNum,req.PageSize)

	resp.Result=result
	return nil
}

//--------------GetOnePageRows()---------------------------------
type GetOnePageRowsRequest struct{
	TableName string
	Page int
	PageSize int
}
type GetOnePageRowsResponse struct{
	Result []map[string]string
}
func (g *DBcache)GetOnePageRows(req GetOnePageRowsRequest,resp *GetOnePageRowsResponse)(err error){
	cacheObj,ok := cache.CacheObj[req.TableName]
	if !ok{
		err = fmt.Errorf("%s,The Table is not cache.",req.TableName)
		return err
	}
	result := cacheObj.GetOnePageRows(req.Page,req.PageSize)

	resp.Result=result
	return nil
}