package cache

import (
	"strings"
	"sync"
)

//总缓存数据
type LinkCache struct {
	head   *Node
	tail   *Node
	length int64
	mutex  *sync.RWMutex //读写锁
}

//新建一个双链表
func NewLinkCache() (LinkCache) {
	head := NewNode(0, "", nil)
	return LinkCache{
		head:   head,
		tail:   head,
		length: 0,
		mutex:  &sync.RWMutex{},
	}
}

//返回链表长度
func (l *LinkCache) GetLength() int64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.length
}

//返回第一个节点
func (l *LinkCache) GetFirstNode() (*Node) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.head.next
}

//从头部插入
func (l *LinkCache) InsertHead(node *Node) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	t := l.head //头节点
	if t.next == nil {
		//节点为空时,直接链接上
		node.next = nil
		t.next = node //前面连上后面
		node.pre = t  //后面指向前面
		l.length++
	} else {
		//标记上一个节点
		t.next.pre = node //当前节点的pre指向插入的node
		//标记插入行的下一个节点.
		node.next = t.next //插入的node的next指向后面的t.next节点

		//标记头部节点的下一个节点.(重新确定头)
		t.next = node //重新指定头后面为node
		node.pre = t  //指定node的pre为头(重新指向头)
		l.length++
	}

}

//直接从尾部开始插入
func (l *LinkCache) InsertTail(node *Node) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	t := l.tail //尾节点
	if t.next == nil {
		//节点为空时,直接链接上
		node.next = nil
		t.next = node //前面连上后面
		node.pre = t  //后面指向前面
		l.length++
		l.tail = node
	} else {
		t.next = node //把node连接到最后一个节点
		node.pre = t  //把node的pre指向最后一个节点.
		l.length++
		l.tail = node
	}

}

//从头部开始遍历到尾部插入,效率没有InsertTail()这个函数好,因为每次都要从头遍历到最后
func (l *LinkCache) InsertTailFromHead(node *Node) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	t := l.head
	if t.next == nil {
		//节点为空时,直接链接上
		node.next = nil
		t.next = node //前面连上后面
		node.pre = t  //后面指向前面
		l.length++
		l.tail = t
	} else {
		//一直循环到最后
		for t.next != nil {
			t = t.next
		}
		t.next = node //把node连接到最后一个节点
		node.pre = t  //把node的pre指向最后一个节点.
		l.length++
		l.tail = t
	}

}

//删除一个节点,根据主键.
func (l *LinkCache) DeleteNodePkey(Pkey string) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if Pkey == "" {
		return false
	}
	t := l.head
	//循环查找与Pkey相等的节点
	for t.next != nil && t.next.pkey != Pkey {
		t = t.next
	}

	//如果查找到
	if t.next != nil && t.next.pkey == Pkey {
		if t.next.next != nil {
			t.next.next.pre = t //设置要删的节点,后面一个节点的pre
		}
		t.next = t.next.next //设置要删的节点,前面一个节点的next
		l.length--
		l.tail = t
		return true
	} else {
		return false
	}
}
//得到链表中指定开始到结束的节点
func (l *LinkCache) GetNodeBetween(start int64, end int64) []*Node {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	t := l.head
	var index int64
	//移动链表头位置
	if t.next !=nil{
		t = t.next
	}
	//循环从头遍历到开始位置
	for t.next != nil {

		if index == start {
			break
		}
		t = t.next
		index++
	}
	//如果不为空,开始获取数据
	if t.next != nil {
		node := make([]*Node,0,end - start)
		for i := start; i < end; i++ {
			node=append(node,t)
			if t.next != nil{
				t = t.next
			}else{
				break
			}
		}
		return node
	}else{
		return nil
	}
    return nil
}

//得到链表中所有节点
func (l *LinkCache) GetAllNode() []*Node {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	t := l.head
	//移动链表头位置
	if t.next !=nil{
		t = t.next
	}
	node := make([]*Node,0,l.length)
	//循环从头一直到尾部遍历
	for t.next != nil {
		node=append(node,t)
		t = t.next
	}
	return node
}

//插入一个节点到目的节点前面
func (l *LinkCache) InsertValueBefore(dest *Node, node *Node) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	t := l.head
	//循环查找
	for t.next != nil && t.next != dest {
		t = t.next
	}
	if t.next == dest {
		if t.next != nil {
			t.next.pre = node
		}

		node.next = t.next
		node.pre = t
		t.next = node

		l.length++
		return true
	} else {

		return false
	}

}

//插入一个节点到目的节点后面
func (l *LinkCache) InsertValueAfter(dest *Node, node *Node) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	t := l.head
	//循环查找
	for t.next != nil && t.next != dest {
		t = t.next
	}
	if t.next == dest {
		if t.next.next != nil {
			t.next.next.pre = node
		}

		node.next = t.next.next
		t.next.next = node
		node.pre = t.next

		l.length++
		return true
	} else {

		return false
	}

}

//插入一个节点到目的数据值前面
func (l *LinkCache) InsertValueBeforeByValue(pkey interface{}, node *Node) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	t := l.head
	//循环查找
	for t.next != nil && t.next.pkey != pkey.(string) {
		t = t.next
	}
	if t.next.pkey == pkey.(string) {
		if t.next != nil {
			t.next.pre = node
		}

		node.next = t.next
		node.pre = t
		t.next = node

		l.length++
		return true
	} else {

		return false
	}

}

//插入一个节点到目的数据值后面
func (l *LinkCache) InsertValueAfterByValue(pkey interface{}, node *Node) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	t := l.head
	//循环查找
	for t.next != nil && t.next.pkey != pkey.(string) {
		t = t.next
	}
	if t.next.pkey == pkey.(string) {
		if t.next.next != nil {
			t.next.next.pre = node
		}

		node.next = t.next.next
		t.next.next = node
		node.pre = t.next

		l.length++
		return true
	} else {

		return false
	}

}

//得到索引index的节点
func (l *LinkCache) GetNodeAtIndex(index int64) *Node {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	if index > l.length-1 || index < 0 {
		return nil
	}
	t := l.head
	//移动位置
	for index > -1 {
		t = t.next
		index--
	}
	return t
}

//删除一个节点
func (l *LinkCache) DeleteNode(node *Node) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if node == nil {
		return false
	}
	t := l.head
	//循环查找
	for t.next != nil && t.next != node {
		t = t.next
	}
	//如果查找到
	if t.next == node {
		if t.next.next != nil {
			t.next.next.pre = t //设置要删的节点,后面一个节点的pre
		}
		t.next = t.next.next //设置要删的节点,前面一个节点的next
		l.length--
		return true
	} else {
		return false
	}
}

//删除一个节点,按照索引
func (l *LinkCache) DeleteNodeAtIndex(index int64) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.head == nil {
		return false
	}
	if index > l.length-1 || index < 0 {
		return false
	}
	t := l.head
	//移动位置
	for index > 0 {
		t = t.next
		index--
	}

	if t.next.next != nil {
		t.next.next.pre = t //设置要删的节点,后面一个节点的pre
	}
	t.next = t.next.next //设置要删的节点,前面一个节点的next
	l.length--
	return true
}

//根据输入值,在链表中查找.
func (l *LinkCache) FindString(input string)(out string) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	t := l.head.next
	//循环从头一直到尾部遍历
	for t.next != nil {
		if strings.Contains(t.pkey,input){
			out=out+","
		}
		t = t.next
	}
	return out
}

