package main

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"
)
//以切片中结构体字段排序,切片中数据是指针结构体(结构体中是基础类型)
//(注意排序对象是:interface{})数据是切片,切片中每个数据是指针结构体.以结构体中字段索引号排序.(注意结构体首字母要大写)
type SliceStructData struct {
	Data            interface{}                                     //需要排序的切片
	SortStructField int                                             //以结构体中字段索引排序.
	IsAsc           bool                                            //是否升序(true升序,false降序)
	CompFunc        func(data1, data2 interface{}, isAsc bool) bool //用于比较两个数的函数
}

//测试结构体
type AA struct {
	Dd string
	Yy int
}
//测试
func main() {
	a := &AA{
		Dd: "aaa",
		Yy: 100,
	}
	b := &AA{
		Dd: "bbb",
		Yy: 200,
	}
	c := &AA{
		Dd: "ccc",
		Yy: 300,
	}
	d := &AA{
		Dd: "xxx",
		Yy: 400,
	}
	e := &AA{
		Dd: "yyy",
		Yy: 500,
	}
	f := &AA{
		Dd: "qq",
		Yy: 600,
	}
	var cc []*AA
	cc = append(cc, f)
	cc = append(cc, d)
	cc = append(cc, a)
	cc = append(cc, e)
	cc = append(cc, c)
	cc = append(cc, b)

	for _, v := range cc {
		fmt.Println(v.Dd," , ",v.Yy)
	}
	//sortObj, err := NewSliceStructDataSort(cc, 0, true)
	sortObj, err := NewSliceStructDataSort(cc, 1, true)
	if err != nil {
		fmt.Println(err)
	}
	sortObj.QuickSort()
	fmt.Println("--------------------------")
	for _, v := range cc {
		fmt.Println(v.Dd," , ",v.Yy)
	}

}




func NewSliceStructDataSort(data interface{}, sortStructField int, isAsc bool) (sortObj *SliceStructData, err error) {
	if data == nil {
		err = fmt.Errorf("切片为nil")
		return nil, err
	}
	t := reflect.TypeOf(data)
	v := reflect.ValueOf(data)
	if v.Len() <= 1 {
		return nil, nil
	}

	if t.Kind() != reflect.Slice {
		err = fmt.Errorf("必须是一个切片")
		return nil, err
	}
	if t.Elem().Kind() != reflect.Ptr || v.Index(0).Elem().Kind() != reflect.Struct {
		err = fmt.Errorf("切片里的结构体必须是一个指针结构体")
		return nil, err
	}
	numField := v.Index(0).Elem().NumField()
	if sortStructField > numField-1 || sortStructField < 0 {
		err = fmt.Errorf("切片里的结构体字段索引错误")
		return nil, err
	}

	sortObj = &SliceStructData{
		Data:            data,
		SortStructField: sortStructField,
		IsAsc:           isAsc,
		CompFunc:        nil,
	}

	switch v.Index(0).Elem().Field(sortStructField).Kind() {
	case reflect.String:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(string) < data2.(string)
			} else {
				return data1.(string) > data2.(string)
			}
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		sortObj.CompFunc = func(data1, data2 interface{}, isAsc bool) bool {
			if isAsc {
				return data1.(int64) < data2.(int64)
			} else {
				return data1.(int64) > data2.(int64)
			}
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
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
		err = fmt.Errorf("切片类型不能是:%s", t.Kind())
		return nil, err
	}
	return sortObj, nil
}

func (s *SliceStructData) QuickSort() {
	rand.Seed(time.Now().UnixNano())
	t := reflect.TypeOf(s.Data)
	v := reflect.ValueOf(s.Data)
	if t.Kind() != reflect.Slice {
		fmt.Println("必须是一个切片")
		return
	}

	if v.Len() < 10 {
		s.BinarySearchSort(s.CompFunc, s.IsAsc)
	} else {
		s.QuickSortIndexGo(0, v.Len()-1, s.CompFunc, s.IsAsc)
	}
}

//二分法插入排序.(升序排序)[用在内存中,不要用在硬盘等外存中,例如硬盘文件,顺序更快.]
func (s *SliceStructData) BinarySearchSort(compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool) {
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
				s.Swap(j, j-1)
			}
		}
	}
	return
}

//获取切片的数据
func (s *SliceStructData) GetValue(idx int) (value interface{}) {
	v := reflect.ValueOf(s.Data)
	switch v.Index(idx).Elem().Field(s.SortStructField).Kind() {
	case reflect.String:
		value = v.Index(idx).Elem().Field(s.SortStructField).String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value = v.Index(idx).Elem().Field(s.SortStructField).Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value = v.Index(idx).Elem().Field(s.SortStructField).Uint()
	case reflect.Float32, reflect.Float64:
		value = v.Index(idx).Elem().Field(s.SortStructField).Float()
	}
	return value
}

//二分法插入排序,查找位置.(升序排序).start开始位置,end结束位置,cur当前位置
func (s *SliceStructData) FindLocation(start, end, cur int, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool) int {
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
		return s.FindLocation(start, mid, cur, compFunc, isAsc)
	} else {
		return s.FindLocation(mid+1, end, cur, compFunc, isAsc)
	}
}

//快速排序,递归
func (s *SliceStructData) QuickSortIndexGo(left, right int, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool) {
	if right-left < 10 {
		//调用二分插入排序,对指定数据段排序
		s.BinarySearchSortIndex(left, right, compFunc, isAsc)
	} else {
		//快速排序
		//以第一个为标准,比第一个小的往左交换,比第一个大的往右交换
		//一,随机取一个数,交换到第一个位置.以第一个为标准.
		randNum := rand.Int()%(right-left) + left
		s.Swap(left, randNum)
		//t := data[left] //备份以此为标准的中间值
		t := s.GetValue(left)

		lt := left      //保证left到lt之间的数据小于标准值t.(lt++)
		gt := right + 1 //保证gt到right之间的数据大于标准值t.(gt--)
		i := left + 1   // 保证lt+1到i之间的数据等于标准值t.(i++)

		for i < gt {
			//if data[i] < t { //小于标准数
			if compFunc(s.GetValue(i), t, isAsc) {
				s.Swap(i, lt+1) //移动小于的地方
				lt++
				i++
				//} else if data[i] > t { //大于标准数
			} else if compFunc(s.GetValue(i), t, !isAsc) {
				s.Swap(i, gt-1) //移动大于的地方
				gt--
			} else {
				i++ //相等
			}
		}
		s.Swap(left, lt) //交换第1个到左边移动的位置.

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			s.QuickSortIndexGo(left, lt-1, compFunc, isAsc) //递归处理左边这段
			wg.Done()
		}()
		go func() {
			s.QuickSortIndexGo(gt, right, compFunc, isAsc) //递归处理右边这段
			wg.Done()
		}()
		wg.Wait()
	}
}

//数据交换
func (s *SliceStructData) Swap(i, j int) {
	v := reflect.ValueOf(s.Data)
	//ttt:=s.Data.([]*AA)
	//fmt.Println(ttt[0])
	////交换数据
	//fmt.Println(i,j)
	//fmt.Println(v.Index(0).Elem().NumField())
	for k := 0; k < v.Index(0).Elem().NumField(); k++ {
		switch v.Index(0).Elem().Field(k).Kind() {
		case reflect.String:
			tmp := v.Index(i).Elem().Field(k).String()
			v.Index(i).Elem().Field(k).SetString(v.Index(j).Elem().Field(k).String())
			v.Index(j).Elem().Field(k).SetString(tmp)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			tmp := v.Index(i).Elem().Field(k).Int()
			v.Index(i).Elem().Field(k).SetInt(v.Index(j).Elem().Field(k).Int())
			v.Index(j).Elem().Field(k).SetInt(tmp)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			tmp := v.Index(i).Elem().Field(k).Uint()
			v.Index(i).Elem().Field(k).SetUint(v.Index(j).Elem().Field(k).Uint())
			v.Index(j).Elem().Field(k).SetUint(tmp)
		case reflect.Float32, reflect.Float64:
			tmp := v.Index(i).Elem().Field(k).Float()
			v.Index(i).Elem().Field(k).SetFloat(v.Index(j).Elem().Field(k).Float())
			v.Index(j).Elem().Field(k).SetFloat(tmp)
		//case reflect.Ptr:
			//tmp := v.Index(i).Elem().Field(k)
			//v.Index(i).Elem().Field(k).Set(v.Index(j).Elem().Field(k))
			//v.Index(j).Elem().Field(k).Set(tmp)
		}
	}

}

//对指定区间数据,利用二分法插入排序.
func (s *SliceStructData) BinarySearchSortIndex(start, end int, compFunc func(data1, data2 interface{}, isAsc bool) bool, isAsc bool) {
	if end-start <= 1 {
		return
	}
	//从第2个数开始,与第1个比较.然后从第3个数开始,依次和第1个和每2个比较,以此类推.
	for i := start + 1; i <= end; i++ {
		//循环插入,寻找合适的位置
		p := s.FindLocation(start, i-1, i, compFunc, isAsc)
		if p != i { //不等,需要插入,如果相等,不需要插入
			for j := i; j > p; j-- {
				s.Swap(j, j-1)
				//data[j], data[j-1] = data[j-1], data[j] //交换数据
			}
		}
	}
	return
}
