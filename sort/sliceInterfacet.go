package slicesort

import (
	"reflect"
	"sync"
)
//(注意排序对象是:[]interface{})数据是切片,切片中每个数据是接口,切片的具体数据可以是,字符串,整型,浮点点的切片.
type SliceInterfacetSort struct {
	Data     []interface{}                                   //需要排序的切片
	IsAsc    bool                                            //是否升序(true升序,false降序)
	CompFunc func(data1, data2 interface{}, isAsc bool) bool //用于比较两个数的函数
}

func NewSliceInterfacetSort(data []interface{}, isAsc bool) (sortObj *SliceInterfacetSort) {
	if len(data) <= 1 || data == nil {
		return nil
	}
	sortObj=&SliceInterfacetSort{
		Data:     data,
		IsAsc:    isAsc,
		CompFunc: nil,
	}

	t := reflect.TypeOf(data[0])
	switch t.Kind() {
	case reflect.String:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(string) < data2.(string)
			} else {
				return data1.(string) > data2.(string)
			}
		}
	case reflect.Int:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(int) < data2.(int)
			} else {
				return data1.(int) > data2.(int)
			}
		}
	case reflect.Int32:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(int32) < data2.(int32)
			} else {
				return data1.(int32) > data2.(int32)
			}
		}
	case reflect.Int64:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(int64) < data2.(int64)
			} else {
				return data1.(int64) > data2.(int64)
			}
		}
	case reflect.Uint:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(uint) < data2.(uint)
			} else {
				return data1.(uint) > data2.(uint)
			}
		}
	case reflect.Uint32:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(uint32) < data2.(uint32)
			} else {
				return data1.(uint32) > data2.(uint32)
			}
		}
	case reflect.Uint64:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(uint64) < data2.(uint64)
			} else {
				return data1.(uint64) > data2.(uint64)
			}
		}
	case reflect.Float32:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(float32) < data2.(float32)
			} else {
				return data1.(float32) > data2.(float32)
			}
		}
	case  reflect.Float64:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(float64) < data2.(float64)
			} else {
				return data1.(float64) > data2.(float64)
			}
		}
	default:
		return nil
	}
	return sortObj
}

//快速排序
func (s *SliceInterfacetSort) QuickSort() {
	if len(s.Data) < 10 {
		s.BinarySearchSort(s.CompFunc, s.IsAsc)
	} else {
		s.QuickSortIndexGo( 0, len(s.Data)-1, s.CompFunc, s.IsAsc)
	}
}

//快速排序,递归
func (s *SliceInterfacetSort) QuickSortIndexGo( left, right int, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool) {
	if right-left < 10 {
		//调用二分插入排序,对指定数据段排序
		s.BinarySearchSortIndex(left, right, compFunc, isAsc)
	} else {
		//以第一个为标准,比第一个小的往左交换,比第一个大的往右交换
		t := s.Data[left] //备份以此为标准的中间值

		lt := left      //保证left到lt之间的数据小于标准值t.(lt++)
		gt := right + 1 //保证gt到right之间的数据大于标准值t.(gt--)
		i := left + 1   // 保证lt+1到i之间的数据等于标准值t.(i++)

		for i < gt {
			//if data[i] < t { //小于标准数
			if compFunc(s.Data[i], t, isAsc) {
				s.Swap( i, lt+1) //移动小于的地方
				lt++
				i++
				//} else if data[i] > t { //大于标准数
			} else if compFunc(s.Data[i], t, !isAsc) {
				s.Swap( i, gt-1) //移动大于的地方
				gt--
			} else {
				i++ //相等
			}
		}
		s.Swap( left, lt) //交换第1个到左边移动的位置.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			s.QuickSortIndexGo(left, lt-1, compFunc, isAsc) //递归处理左边这段
			wg.Done()
		}()
		go func() {
			s.QuickSortIndexGo( gt, right, compFunc, isAsc) //递归处理右边这段
			wg.Done()
		}()
		wg.Wait()
	}
}

//数据交换
func (s *SliceInterfacetSort) Swap( i, j int) {
	s.Data[i], s.Data[j] = s.Data[j], s.Data[i]
}

//对指定区间数据,利用二分法插入排序.
func (s *SliceInterfacetSort) BinarySearchSortIndex( start, end int, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool)  {
	if end-start <= 1 {
		return
	}
	//从第2个数开始,与第1个比较.然后从第3个数开始,依次和第1个和每2个比较,以此类推.
	for i := start + 1; i <= end; i++ {
		//循环插入,寻找合适的位置
		p := s.FindLocation( start, i-1, i, compFunc, isAsc)
		if p != i { //不等,需要插入,如果相等,不需要插入
			for j := i; j > p; j-- {
				s.Swap(j, j-1)
				//data[j], data[j-1] = data[j-1], data[j] //交换数据
			}
		}
	}
	return
}

//二分法插入排序,查找位置.(升序排序).start开始位置,end结束位置,cur当前位置
func (s *SliceInterfacetSort) FindLocation( start, end, cur int, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool) int {
	//对比当前位置与需要排序的元素大小,近回较大值的位置
	if start >= end {
		//if data[start].SortColumn < data[cur].SortColumn {
		if compFunc(s.Data[start], s.Data[cur], isAsc) {
			return cur
		} else {
			return start
		}
	}
	mid := (start + end) / 2
	//二分查找递归
	//if data[mid].SortColumn > data[cur].SortColumn {
	if compFunc(s.Data[mid], s.Data[cur], !isAsc) {
		return s.FindLocation( start, mid, cur, compFunc, isAsc)
	} else {
		return s.FindLocation( mid+1, end, cur, compFunc, isAsc)
	}
}

//二分法插入排序.(升序排序)[用在内存中,不要用在硬盘等外存中,例如硬盘文件,顺序更快.]
func (s *SliceInterfacetSort) BinarySearchSort(compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool)  {
	if len(s.Data) <= 1 {
		return
	}
	//从第2个数开始,与第1个比较.然后从第3个数开始,依次和第1个和每2个比较,以此类推.
	for i := 1; i < len(s.Data); i++ {
		//循环插入,寻找合适的位置
		p := s.FindLocation( 0, i-1, i, compFunc, isAsc)
		if p != i { //不等,需要交换位置,如果相等,不需要交换位置
			for j := i; j > p; j-- {
				s.Data[j], s.Data[j-1] = s.Data[j-1], s.Data[j] //交换数据
			}
		}
	}
	return
}
