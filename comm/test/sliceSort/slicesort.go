package main

import (
	"fmt"
	"reflect"
	"sync"
)
//(注意排序对象是:interface{})数据是切片,以接口接收切片对象,排序对象是,字符串切片,整型切片,浮点切片
type SliceData struct {
	Data     interface{}                                     //需要排序的切片
	IsAsc    bool                                            //是否升序(true升序,false降序)
	CompFunc func(data1, data2 interface{}, isAsc bool) bool //用于比较两个数的函数
}

func NewSliceDataSort(data interface{}, isAsc bool) (sortObj *SliceData,err error) {
	if  data == nil {
		err=fmt.Errorf("切片为nil")
		return nil,err
	}
	t := reflect.TypeOf(data)
	v := reflect.ValueOf(data)
	if t.Kind() == reflect.Ptr{
		t=t.Elem()
		v=v.Elem()
	}

	if t.Kind() != reflect.Slice{
		err=fmt.Errorf("必须是一个切片")
		return nil,err
	}
	if v.Len() <= 1 {
		return nil,nil
	}

	sortObj=&SliceData{
		Data:     data,
		IsAsc:    isAsc,
		CompFunc: nil,
	}

	switch v.Index(0).Kind() {
	case reflect.String:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(string) < data2.(string)
			} else {
				return data1.(string) > data2.(string)
			}
		}
	case reflect.Int,reflect.Int8,reflect.Int16,reflect.Int32,reflect.Int64:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(int64) < data2.(int64)
			} else {
				return data1.(int64) > data2.(int64)
			}
		}
	case reflect.Uint,reflect.Uint8,reflect.Uint16,reflect.Uint32,reflect.Uint64:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(uint64) < data2.(uint64)
			} else {
				return data1.(uint64) > data2.(uint64)
			}
		}
	case reflect.Float32, reflect.Float64:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(float64) < data2.(float64)
			} else {
				return data1.(float64) > data2.(float64)
			}
		}
	default:
		err=fmt.Errorf("切片类型不能是:%s",t.Kind())
		return nil,err
	}
	return sortObj,nil
}

func main() {
	tt:=&[]string{"x", "a", "j","c", "z","f","b","dd","jo","twa","iaj","di"}
	//tt:=[]int{1,11, 9, 2, 3,10, 7, 4,8,12, 6, 5}
	fmt.Println(tt)
	sortObj, err := NewSliceDataSort(tt, true)
	if err!=nil {
		fmt.Println(err)
	}
	sortObj.QuickSort()
	fmt.Println(tt)

}

func (s *SliceData) QuickSort() {
	//t := reflect.TypeOf(s.Data)
	v := reflect.ValueOf(s.Data)
	if v.Kind() == reflect.Ptr{
		v=v.Elem()
	}
	if v.Kind() != reflect.Slice{
		fmt.Println("必须是一个切片")
		return
	}

	if v.Len() < 10 {
		s.BinarySearchSort( s.CompFunc, s.IsAsc)
	}else {
		s.QuickSortIndexGo(0, v.Len()-1, s.CompFunc, s.IsAsc)
	}
}
//二分法插入排序.(升序排序)[用在内存中,不要用在硬盘等外存中,例如硬盘文件,顺序更快.]
func (s *SliceData)BinarySearchSort(compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool)  {
	v := reflect.ValueOf(s.Data)
	if v.Len() <= 1 {
		return
	}
	//从第2个数开始,与第1个比较.然后从第3个数开始,依次和第1个和每2个比较,以此类推.
	for i := 1; i < v.Len(); i++ {
		//循环插入,寻找合适的位置

		p := s.FindLocation(0, i-1, i, compFunc, isAsc)
		if p != i { //不等,需要交换位置,如果相等,不需要交换位置
			for j := i; j > p; j-- {
				//交换数据
				s.Swap( j, j-1)
			}
		}
	}
	return
}
//获取切片的数据
func (s *SliceData)GetValue(idx int)(value interface{}){
	v := reflect.ValueOf(s.Data)
	if v.Kind() == reflect.Ptr{
		v=v.Elem()
	}
	reflectValue:=v.Index(idx)
	switch reflectValue.Kind() {
	case reflect.String:
		value=reflectValue.String()
	case reflect.Int,reflect.Int8,reflect.Int16,reflect.Int32,reflect.Int64:
		value=reflectValue.Int()
	case reflect.Uint,reflect.Uint8,reflect.Uint16,reflect.Uint32,reflect.Uint64:
		value=reflectValue.Uint()
	case reflect.Float32, reflect.Float64:
		value=reflectValue.Float()
		//case reflect.Bool:
		//	value = reflectValue.Bool()
		//case reflect.Ptr:
		//	value = reflectValue.Elem().Addr()
	}
	return value
}
//二分法插入排序,查找位置.(升序排序).start开始位置,end结束位置,cur当前位置
func (s *SliceData)FindLocation( start, end, cur int, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool) int {
	//对比当前位置与需要排序的元素大小,近回较大值的位置
	if start >= end {
		//if data[start].SortColumn < data[cur].SortColumn {
		if compFunc(s.GetValue(start), s.GetValue(cur), isAsc) {
			return cur
		} else {
			return start
		}
	}
	mid := (start + end) / 2
	//二分查找递归
	//if data[mid].SortColumn > data[cur].SortColumn {
	if compFunc(s.GetValue(mid), s.GetValue(cur), !isAsc) {
		return s.FindLocation( start, mid, cur, compFunc, isAsc)
	} else {
		return s.FindLocation( mid+1, end, cur, compFunc, isAsc)
	}
}


//快速排序,递归
func (s *SliceData)QuickSortIndexGo( left, right int, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool) {
	if right-left < 10 {
		//调用二分插入排序,对指定数据段排序
		s.BinarySearchSortIndex( left, right, compFunc, isAsc)
	} else {
		//以第一个为标准,比第一个小的往左交换,比第一个大的往右交换
		t :=s.GetValue(left)

		lt := left      //保证left到lt之间的数据小于标准值t.(lt++)
		gt := right + 1 //保证gt到right之间的数据大于标准值t.(gt--)
		i := left + 1   // 保证lt+1到i之间的数据等于标准值t.(i++)

		for i < gt {
			//if data[i] < t { //小于标准数
			if compFunc(s.GetValue(i), t, isAsc) {
				s.Swap( i, lt+1) //移动小于的地方
				lt++
				i++
				//} else if data[i] > t { //大于标准数
			} else if compFunc(s.GetValue(i), t, !isAsc) {
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
func (s *SliceData)Swap( i, j int) {
	v := reflect.ValueOf(s.Data)
	if v.Kind() == reflect.Ptr{
		v=v.Elem()
	}
	iValue:=v.Index(i)
	jValue:=v.Index(j)
	//交换数据
	switch v.Index(0).Kind() {
	case reflect.String:
		tmp:=iValue.String()
		iValue.SetString(jValue.String())
		jValue.SetString(tmp)
	case reflect.Int,reflect.Int8,reflect.Int16,reflect.Int32,reflect.Int64:
		tmp:=iValue.Int()
		iValue.SetInt(jValue.Int())
		jValue.SetInt(tmp)
	case reflect.Uint,reflect.Uint8,reflect.Uint16,reflect.Uint32,reflect.Uint64:
		tmp:=iValue.Uint()
		iValue.SetUint(jValue.Uint())
		jValue.SetUint(tmp)
	case reflect.Float32, reflect.Float64:
		tmp:=iValue.Float()
		iValue.SetFloat(jValue.Float())
		jValue.SetFloat(tmp)
	case reflect.Bool:
		tmp := iValue.Bool()
		iValue.SetBool(jValue.Bool())
		jValue.SetBool(tmp)
	case reflect.Ptr:
		tmp := iValue.Elem().Addr()
		iValue.Set(jValue.Elem().Addr())
		jValue.Set(tmp)
	}
	//data[i], data[j] = data[j], data[i]
}

//对指定区间数据,利用二分法插入排序.
func (s *SliceData)BinarySearchSortIndex( start, end int, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool)  {
	if end-start <= 1 {
		return
	}
	//从第2个数开始,与第1个比较.然后从第3个数开始,依次和第1个和每2个比较,以此类推.
	for i := start + 1; i <= end; i++ {
		//循环插入,寻找合适的位置
		p := s.FindLocation( start, i-1, i, compFunc, isAsc)
		if p != i { //不等,需要插入,如果相等,不需要插入
			for j := i; j > p; j-- {
				s.Swap( j, j-1)
				//data[j], data[j-1] = data[j-1], data[j] //交换数据
			}
		}
	}
	return
}


