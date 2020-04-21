package cache

import (
	"math/rand"
	"time"
)

//start开始位置,end结束位置,cur当前位置
func FindMidIndex(data []*SliceCache, start, end, cur int) int {
	//对比当前位置与需要排序的元素大小,近回较大值的位置
	if start >= end {
		if data[start].SortColumn < data[cur].SortColumn {
			return cur
		} else {
			return start
		}
	}
	mid := (start + end) / 2
	//二分查找递归
	if data[mid].SortColumn > data[cur].SortColumn {
		return FindMidIndex(data, start, mid, cur)
	} else {
		return FindMidIndex(data, mid+1, end, cur)
	}
}

//快速排序
func QuickSort(data []*SliceCache) []*SliceCache {
	rand.Seed(time.Now().UnixNano())
	if len(data) < 100 {
		return BinarySearchSort(data)
	} else {
		QuickSortIndex(data, 0, len(data)-1)
		return data
	}
}

//快速排序,递归
func QuickSortIndex(data []*SliceCache, left, right int) {
	if right-left < 100 {
		//调用二分插入排序,对指定数据段排序
		BinarySearchSortIndex(data, left, right)
	} else {
		//快速排序
		//以第一个为标准,比第一个小的往左交换,比第一个大的往右交换
		//一,随机取一个数,交换到第一个位置.以第一个为标准.
		randNum := rand.Int()%(right-left) + left
		Swap(data, left, randNum)
		t := data[left].SortColumn //备份以此为标准的中间值

		lt := left      //保证left到lt之间的数据小于标准值t.(lt++)
		gt := right + 1 //保证gt到right之间的数据大于标准值t.(gt--)
		i := left + 1   // 保证lt+1到i之间的数据等于标准值t.(i++)

		for i < gt {
			if data[i].SortColumn < t { //小于标准数
				Swap(data, i, lt+1) //移动小于的地方
				lt++
				i++
			} else if data[i].SortColumn > t { //大于标准数
				Swap(data, i, gt-1) //移动大于的地方
				gt--
			} else {
				i++ //相等
			}
		}
		Swap(data, left, lt) //交换第1个到左边移动的位置.
		QuickSortIndex(data, left, lt-1) //递归处理左边这段
		QuickSortIndex(data, gt, right)  //递归处理右边这段
	}
}

//二分法插入排序,对指定数据段索引排序
func BinarySearchSortIndex(data []*SliceCache, start, end int) []*SliceCache {
	if end-start <= 1 {
		return data
	}
	for i := start + 1; i <= end; i++ {
		//循环插入,寻找合适的位置
		p := FindMidIndex(data, start, i-1, i)
		if p != i { //不等,需要插入,如果相等,不需要插入
			for j := i; j > p; j-- {
				data[j], data[j-1] = data[j-1], data[j] //交换数据
			}
		}
	}
	return data
}

//数据交换
func Swap(data []*SliceCache, i, j int) {
	data[i], data[j] = data[j], data[i]
}

//二分法插入排序
func BinarySearchSort(data []*SliceCache) []*SliceCache {
	if len(data) <= 1 {
		return data
	}
	for i := 1; i < len(data); i++ {
		//循环插入,寻找合适的位置
		p := FindMidIndex(data, 0, i-1, i)
		if p != i { //不等,需要交换位置,如果相等,不需要交换位置
			for j := i; j > p; j-- {
				data[j], data[j-1] = data[j-1], data[j] //交换数据
			}
		}
	}
	return data
}

//二分查找.要求：切片是升序
func BinarySearchAsc(data []*SliceCache, findData string) int {
	low := 0
	high := len(data) - 1
	for low <= high {
		mid := (low + high) / 2
		if findData > data[mid].SortColumn {
			low = mid + 1
		} else if findData < data[mid].SortColumn {
			high = mid - 1
		} else {
			return mid //相等的情况
		}
	}
	return -1
}
//二分查找.要求：切片是降序
func BinarySearchDesc(data []*SliceCache, findData string) int {
	low := 0
	high := len(data) - 1
	for low <= high {
		mid := (low + high) / 2
		if findData < data[mid].SortColumn {
			low = mid + 1
		} else if findData > data[mid].SortColumn {
			high = mid - 1
		} else {
			return mid //相等的情况
		}
	}
	return -1
}
//插入排序
func InsertSortAsc(data []*SliceCache) []*SliceCache {
	if len(data) <= 1 {
		return data
	}
	//i=1,从第2个位置开始
	for i := 1; i < len(data); i++ {
		t := data[i] //将当前位置数据临时保存.
		j := i - 1   //j保存上一个位置
		//j>=0保证上一个位置不越界.
		// t<data[j],前面大于当前位置(从小到大排序)
		for j >= 0 && data[j].SortColumn > t.SortColumn {
			//上一个位置大于当前位置,则把上一个往后移动.
			data[j+1] = data[j]
			j--
		}
		data[j+1] = t //移动后,把原位置的数据存入移动后的位置
	}
	return data
}