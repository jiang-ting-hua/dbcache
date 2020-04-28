package cache

import "sync"

//双向链表.行数据节点
type Node struct {
	rowNum int64
	pkey   string
	sortColumn string
	row    *sync.Map
	pre    *Node
	next   *Node
}

//新建一个节点
func NewNode(rowNum int64,pkey string,sortColumn string, row *sync.Map) *Node {
	return &Node{
		rowNum: rowNum,
		pkey:   pkey,
		sortColumn:sortColumn,
		row:    row,
		pre:    nil,
		next:   nil,
	}
}
//返回rowNum数据
func (r *Node) RowNum() int64 {
	return r.rowNum
}
//返回pkey数据
func (r *Node) Pkey() string {
	return r.pkey
}

//返回行数据
func (r *Node) RowValue() *sync.Map {
	return r.row
}

//返回上一个节点
func (r *Node) Pre() *Node {
	return r.pre
}

//返回下一下节点
func (r *Node) Next() *Node {
	return r.next
}
