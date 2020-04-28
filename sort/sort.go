package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type QuickSortData struct {
	Data     []interface{}                                   //需要排序的切片
	IsAsc    bool                                            //是否升序
	CompFunc func(data1, data2 interface{}, isAsc bool) bool //用于比较两个数的函数
}

//func NewQuickSortData(data []interface{},isAsc bool) *QuickSortData{
//	return &QuickSortData{
//		Data:        data,
//		IsAsc:       isAsc,
//		CompFunc: nil,
//	}
//}

func main2() {
	mydata := new(QuickSortData)
	mydata.Data = []interface{}{"x", "a", "c", "z", "b"}
	mydata.IsAsc = false
	mydata.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
		if isAsc {
			return data1.(string) < data2.(string)
		} else {
			return data1.(string) > data2.(string)
		}
	}
	mydata.QuickSort()
	fmt.Println(mydata.Data)
}
func main222() {
	mydata := new(QuickSortData)
	mydata.Data = []interface{}{1, 9, 2,  3, 7, 4, 6, 5}
	mydata.IsAsc = true
	mydata.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
		if isAsc {
			return data1.(int) < data2.(int)
		} else {
			return data1.(int) > data2.(int)
		}
	}
	mydata.QuickSort()
	fmt.Println(mydata.Data)
}
func (q *QuickSortData) QuickSort() {
	rand.Seed(time.Now().UnixNano())
	if len(q.Data) < 10 {
		q.Data = BinarySearchSort(q.Data, q.CompFunc, q.IsAsc)
	} else {
		QuickSortIndexGo(q.Data, 0, len(q.Data)-1, q.CompFunc, q.IsAsc)
	}
}

//快速排序,递归
func QuickSortIndexGo(data []interface{}, left, right int, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool) {
	if right-left < 10 {
		//调用二分插入排序,对指定数据段排序
		BinarySearchSortIndex(data, left, right, compFunc, isAsc)
	} else {
		//快速排序
		//以第一个为标准,比第一个小的往左交换,比第一个大的往右交换
		//一,随机取一个数,交换到第一个位置.以第一个为标准.
		randNum := rand.Int()%(right-left) + left
		Swap(data, left, randNum)
		t := data[left] //备份以此为标准的中间值

		lt := left      //保证left到lt之间的数据小于标准值t.(lt++)
		gt := right + 1 //保证gt到right之间的数据大于标准值t.(gt--)
		i := left + 1   // 保证lt+1到i之间的数据等于标准值t.(i++)

		for i < gt {
			//if data[i] < t { //小于标准数
			if compFunc(data[i], t, isAsc) {
				Swap(data, i, lt+1) //移动小于的地方
				lt++
				i++
			//} else if data[i] > t { //大于标准数
			} else if compFunc(data[i], t, !isAsc) {
				Swap(data, i, gt-1) //移动大于的地方
				gt--
			} else {
				i++ //相等
			}
		}
		Swap(data, left, lt) //交换第1个到左边移动的位置.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			QuickSortIndexGo(data, left, lt-1, compFunc, isAsc) //递归处理左边这段
			wg.Done()
		}()
		go func() {
			QuickSortIndexGo(data, gt, right, compFunc, isAsc) //递归处理右边这段
			wg.Done()
		}()
		wg.Wait()
	}
}

//数据交换
func Swap(data []interface{}, i, j int) {
	data[i], data[j] = data[j], data[i]
}

//对指定区间数据,利用二分法插入排序.
func BinarySearchSortIndex(data []interface{}, start, end int, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool) []interface{} {
	if end-start <= 1 {
		return data
	}
	//从第2个数开始,与第1个比较.然后从第3个数开始,依次和第1个和每2个比较,以此类推.
	for i := start + 1; i <= end; i++ {
		//循环插入,寻找合适的位置
		p := FindLocation(data, start, i-1, i, compFunc, isAsc)
		if p != i { //不等,需要插入,如果相等,不需要插入
			for j := i; j > p; j-- {
				data[j], data[j-1] = data[j-1], data[j] //交换数据
			}
		}
	}
	return data
}

//二分法插入排序,查找位置.(升序排序).start开始位置,end结束位置,cur当前位置
func FindLocation(data []interface{}, start, end, cur int, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool) int {
	//对比当前位置与需要排序的元素大小,近回较大值的位置
	if start >= end {
		//if data[start].SortColumn < data[cur].SortColumn {
		if compFunc(data[start], data[cur], isAsc) {
			return cur
		} else {
			return start
		}
	}
	mid := (start + end) / 2
	//二分查找递归
	//if data[mid].SortColumn > data[cur].SortColumn {
	if compFunc(data[mid], data[cur], !isAsc) {
		return FindLocation(data, start, mid, cur, compFunc, isAsc)
	} else {
		return FindLocation(data, mid+1, end, cur, compFunc, isAsc)
	}
}

//二分法插入排序.(升序排序)[用在内存中,不要用在硬盘等外存中,例如硬盘文件,顺序更快.]
func BinarySearchSort(data []interface{}, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool) []interface{} {
	if len(data) <= 1 {
		return data
	}
	//从第2个数开始,与第1个比较.然后从第3个数开始,依次和第1个和每2个比较,以此类推.
	for i := 1; i < len(data); i++ {
		//循环插入,寻找合适的位置
		p := FindLocation(data, 0, i-1, i, compFunc, isAsc)
		if p != i { //不等,需要交换位置,如果相等,不需要交换位置
			for j := i; j > p; j-- {
				data[j], data[j-1] = data[j-1], data[j] //交换数据
			}
		}
	}
	return data
}
