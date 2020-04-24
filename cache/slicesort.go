package cache

import (
	"math/rand"
	"sync"
	"time"
)
//---------------------数据查找----------------------------------------------------
//二分查找.(数据原已排序,数据是升序)
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

//二分查找.(数据原已排序,数据是降序)
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

//数据交换
func Swap(data []*SliceCache, i, j int) {
	data[i], data[j] = data[j], data[i]
}

//---------------------快速排序(升序排序)----------------------------------------------------
//快速排序(升序排序)
func QuickSortAsc(data []*SliceCache) []*SliceCache {
	rand.Seed(time.Now().UnixNano())
	if len(data) < 100 {
		return BinarySearchSortAsc(data)
	} else {
		QuickSortIndexAsc(data, 0, len(data)-1)
		return data
	}
}

//快速排序,递归
func QuickSortIndexAsc(data []*SliceCache, left, right int) {
	if right-left < 100 {
		//调用二分插入排序,对指定数据段排序
		BinarySearchSortIndexAsc(data, left, right)
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
		Swap(data, left, lt)             //交换第1个到左边移动的位置.

		QuickSortIndexAsc(data, left, lt-1) //递归处理左边这段
		QuickSortIndexAsc(data, gt, right)  //递归处理右边这段
	}
}
//---------------------并发快速排序(升序排序)----------------------------------------------------
//并发快速排序(升序排序)
func QuickSortGoAsc(data []*SliceCache) []*SliceCache {
	rand.Seed(time.Now().UnixNano())
	if len(data) < 100 {
		return BinarySearchSortAsc(data)
	} else {
		QuickSortIndexGoAsc(data, 0, len(data)-1)
		return data
	}
}

//快速排序,递归
func QuickSortIndexGoAsc(data []*SliceCache, left, right int) {
	if right-left < 100 {
		//调用二分插入排序,对指定数据段排序
		BinarySearchSortIndexAsc(data, left, right)
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
		Swap(data, left, lt)             //交换第1个到左边移动的位置.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			QuickSortIndexGoAsc(data, left, lt-1) //递归处理左边这段
			wg.Done()
		}()
		go func() {
			QuickSortIndexGoAsc(data, gt, right)  //递归处理右边这段
			wg.Done()
		}()
		wg.Wait()
	}
}


//---------------------快速排序(降序排序)----------------------------------------------------
//快速排序(降序排序)
func QuickSortDesc(data []*SliceCache) []*SliceCache {
	rand.Seed(time.Now().UnixNano())
	if len(data) < 100 {
		return BinarySearchSortDesc(data)
	} else {
		QuickSortIndexDesc(data, 0, len(data)-1)
		return data
	}
}

//快速排序(降序排序),递归
func QuickSortIndexDesc(data []*SliceCache, left, right int) {
	if right-left < 100 {
		//调用二分插入排序,对指定数据段排序
		BinarySearchSortIndexDesc(data, left, right)
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
			if data[i].SortColumn > t { //小于标准数
				Swap(data, i, lt+1) //移动小于的地方
				lt++
				i++
			} else if data[i].SortColumn < t { //大于标准数
				Swap(data, i, gt-1) //移动大于的地方
				gt--
			} else {
				i++ //相等
			}
		}
		Swap(data, left, lt)             //交换第1个到左边移动的位置.

		QuickSortIndexDesc(data, left, lt-1) //递归处理左边这段
		QuickSortIndexDesc(data, gt, right)  //递归处理右边这段
	}
}
//---------------------并发快速排序(降序排序)----------------------------------------------------
//并发快速排序(降序排序)
func QuickSortGoDesc(data []*SliceCache) []*SliceCache {
	rand.Seed(time.Now().UnixNano())
	if len(data) < 100 {
		return BinarySearchSortDesc(data)
	} else {
		QuickSortIndexGoDesc(data, 0, len(data)-1)
		return data
	}
}

//并发快速排序(降序排序),递归
func QuickSortIndexGoDesc(data []*SliceCache, left, right int) {
	if right-left < 100 {
		//调用二分插入排序,对指定数据段排序
		BinarySearchSortIndexDesc(data, left, right)
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
			if data[i].SortColumn > t { //小于标准数
				Swap(data, i, lt+1) //移动小于的地方
				lt++
				i++
			} else if data[i].SortColumn < t { //大于标准数
				Swap(data, i, gt-1) //移动大于的地方
				gt--
			} else {
				i++ //相等
			}
		}
		Swap(data, left, lt)             //交换第1个到左边移动的位置.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			QuickSortIndexDesc(data, left, lt-1) //递归处理左边这段
			wg.Done()
		}()
		go func() {
			QuickSortIndexDesc(data, gt, right)  //递归处理右边这段
			wg.Done()
		}()
		wg.Wait()
	}
}

//---------------------二分法插入排序[指定区间](升序排序)----------------------------------------------------
//对指定区间数据,利用二分法插入排序.
func BinarySearchSortIndexAsc(data []*SliceCache, start, end int) []*SliceCache {
	if end-start <= 1 {
		return data
	}
	//从第2个数开始,与第1个比较.然后从第3个数开始,依次和第1个和每2个比较,以此类推.
	for i := start + 1; i <= end; i++ {
		//循环插入,寻找合适的位置
		p := FindLocationAsc(data, start, i-1, i)
		if p != i { //不等,需要插入,如果相等,不需要插入
			for j := i; j > p; j-- {
				data[j], data[j-1] = data[j-1], data[j] //交换数据
			}
		}
	}
	return data
}
//---------------------二分法插入排序[指定区间](降序排序)----------------------------------------------------
//对指定区间数据,利用二分法插入排序.
func BinarySearchSortIndexDesc(data []*SliceCache, start, end int) []*SliceCache {
	if end-start <= 1 {
		return data
	}
	//从第2个数开始,与第1个比较.然后从第3个数开始,依次和第1个和每2个比较,以此类推.
	for i := start + 1; i <= end; i++ {
		//循环插入,寻找合适的位置
		p := FindLocationDesc(data, start, i-1, i)
		if p != i { //不等,需要插入,如果相等,不需要插入
			for j := i; j > p; j-- {
				data[j], data[j-1] = data[j-1], data[j] //交换数据
			}
		}
	}
	return data
}

//---------------------二分法插入排序(升序排序)----------------------------------------------------
//二分法插入排序.(升序排序)[用在内存中,不要用在硬盘等外存中,例如硬盘文件,顺序更快.]
func BinarySearchSortAsc(data []*SliceCache) []*SliceCache {
	if len(data) <= 1 {
		return data
	}
	//从第2个数开始,与第1个比较.然后从第3个数开始,依次和第1个和每2个比较,以此类推.
	for i := 1; i < len(data); i++ {
		//循环插入,寻找合适的位置
		p := FindLocationAsc(data, 0, i-1, i)
		if p != i { //不等,需要交换位置,如果相等,不需要交换位置
			for j := i; j > p; j-- {
				data[j], data[j-1] = data[j-1], data[j] //交换数据
			}
		}
	}
	return data
}

//二分法插入排序,查找位置.(升序排序).start开始位置,end结束位置,cur当前位置
func FindLocationAsc(data []*SliceCache, start, end, cur int) int {
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
		return FindLocationAsc(data, start, mid, cur)
	} else {
		return FindLocationAsc(data, mid+1, end, cur)
	}
}
//---------------------二分法插入排序(降序排序)----------------------------------------------------
//二分法插入排序.(降序排序)[用在内存中,不要用在硬盘等外存中,例如硬盘文件,顺序更快.]
func BinarySearchSortDesc(data []*SliceCache) []*SliceCache {
	if len(data) <= 1 {
		return data
	}
	//从第2个数开始,与第1个比较.然后从第3个数开始,依次和第1个和每2个比较,以此类推.
	for i := 1; i < len(data); i++ {
		//循环插入,寻找合适的位置
		p := FindLocationDesc(data, 0, i-1, i)
		if p != i { //不等,需要交换位置,如果相等,不需要交换位置
			for j := i; j > p; j-- {
				data[j], data[j-1] = data[j-1], data[j] //交换数据
			}
		}
	}
	return data
}


//二分法插入排序,查找位置.(降序排序).start开始位置,end结束位置,cur当前位置
func FindLocationDesc(data []*SliceCache, start, end, cur int) int {
	//对比当前位置与需要排序的元素大小,近回较大值的位置
	if start >= end {
		if data[start].SortColumn > data[cur].SortColumn {
			return cur
		} else {
			return start
		}
	}
	mid := (start + end) / 2
	//二分查找递归
	if data[mid].SortColumn < data[cur].SortColumn {
		return FindLocationDesc(data, start, mid, cur)
	} else {
		return FindLocationDesc(data, mid+1, end, cur)
	}
}
//---------------------插入排序(升序排序)----------------------------------------------------
//插入排序(升序排序)
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

//插入排序,按主键排序(升序排序)
func InsertSortPkeyAsc(data []*SliceCache) []*SliceCache {
	if len(data) <= 1 {
		return data
	}
	//i=1,从第2个位置开始
	for i := 1; i < len(data); i++ {
		t := data[i] //将当前位置数据临时保存.
		j := i - 1   //j保存上一个位置
		//j>=0保证上一个位置不越界.
		// t<data[j],前面大于当前位置(从小到大排序)
		for j >= 0 && data[j].Pkey > t.Pkey {
			//上一个位置大于当前位置,则把上一个往后移动.
			data[j+1] = data[j]
			j--
		}
		data[j+1] = t //移动后,把原位置的数据存入移动后的位置
	}
	return data
}
//---------------------插入排序(降序排序)----------------------------------------------------
//插入排序(降序排序)
func InsertSortDesc(data []*SliceCache) []*SliceCache {
	if len(data) <= 1 {
		return data
	}
	//i=1,从第2个位置开始
	for i := 1; i < len(data); i++ {
		t := data[i] //将当前位置数据临时保存.
		j := i - 1   //j保存上一个位置
		//j>=0保证上一个位置不越界.
		// t<data[j],前面大于当前位置(从小到大排序)
		for j >= 0 && data[j].SortColumn < t.SortColumn {
			//上一个位置大于当前位置,则把上一个往后移动.
			data[j+1] = data[j]
			j--
		}
		data[j+1] = t //移动后,把原位置的数据存入移动后的位置
	}
	return data
}

//插入排序,按主键排序(降序排序)
func InsertSortPkeyDesc(data []*SliceCache) []*SliceCache {
	if len(data) <= 1 {
		return data
	}
	//i=1,从第2个位置开始
	for i := 1; i < len(data); i++ {
		t := data[i] //将当前位置数据临时保存.
		j := i - 1   //j保存上一个位置
		//j>=0保证上一个位置不越界.
		// t<data[j],前面大于当前位置(从小到大排序)
		for j >= 0 && data[j].Pkey < t.Pkey {
			//上一个位置大于当前位置,则把上一个往后移动.
			data[j+1] = data[j]
			j--
		}
		data[j+1] = t //移动后,把原位置的数据存入移动后的位置
	}
	return data
}


//---------------------归并排序(升序排序)----------------------------------------------------
//归并排序(升序排序). (缺点,会多占空间)
func MergeSortAsc(data []*SliceCache) []*SliceCache {
	if len(data) <= 1 {
		return data
	}
	i := len(data) / 2
	//数据一分为二一直递归分割,再逐步合二为一排序归并.
	//分成二段,进行递归分解分成一个元素.然后排序合并成最终排好序的切片
	left := MergeSortAsc(data[0:i])
	right := MergeSortAsc(data[i:])
	//对这二段归并排序
	result := MergeAsc(left, right)
	return result
}
//并发归并排序(升序排序). (缺点,会多占空间)
func MergeSortGoAsc(data []*SliceCache) []*SliceCache {
	if len(data) <= 1 {
		return data
	}
	i := len(data) / 2
	//数据一分为二一直递归分割,再逐步合二为一排序归并.
	//分成二段,进行递归分解分成一个元素.然后排序合并成最终排好序的切片
	var wg sync.WaitGroup
	wg.Add(2)
	var left,right []*SliceCache
	go func(){
		left = MergeSortGoAsc(data[0:i])
		wg.Done()
	}()
	go func(){
		right = MergeSortGoAsc(data[i:])
		wg.Done()
	}()
	wg.Wait()
	//对这二段归并排序
	result := MergeAsc(left, right)
	return result
}
//归并排序(升序排序).
func MergeAsc(one, two []*SliceCache) []*SliceCache {
	i, j := 0, 0
	oneLen := len(one)
	twoLen := len(two)
	result := make([]*SliceCache, 0, oneLen+twoLen)
	for i < oneLen && j < twoLen {
		if one[i].SortColumn < two[j].SortColumn {
			result = append(result, one[i])
			i++
		} else if one[i].SortColumn > two[j].SortColumn {
			result = append(result, two[j])
			j++
		} else {
			result = append(result, one[i])
			i++
			result = append(result, two[j])
			j++
		}
	}
	for i < oneLen {
		result = append(result, one[i])
		i++
	}
	for j < twoLen {
		result = append(result, two[j])
		j++
	}
	return result
}

//---------------------归并排序(降序排序)----------------------------------------------------
//归并排序(降序排序). (缺点,会多占空间)
func MergeSortDesc(data []*SliceCache) []*SliceCache {
	if len(data) <= 1 {
		return data
	}
	i := len(data) / 2
	//数据一分为二一直递归分割,再逐步合二为一排序归并.
	//分成二段,进行递归分解分成一个元素.然后排序合并成最终排好序的切片
	left := MergeSortDesc(data[0:i])
	right := MergeSortDesc(data[i:])
	//对这二段归并排序
	result := MergeDesc(left, right)
	return result
}
//并发归并排序(降序排序). (缺点,会多占空间)
func MergeSortGoDesc(data []*SliceCache) []*SliceCache {
	if len(data) <= 1 {
		return data
	}
	i := len(data) / 2
	//数据一分为二一直递归分割,再逐步合二为一排序归并.
	//分成二段,进行递归分解分成一个元素.然后排序合并成最终排好序的切片
	var wg sync.WaitGroup
	wg.Add(2)
	var left,right []*SliceCache
	go func(){
		left = MergeSortGoDesc(data[0:i])
		wg.Done()
	}()
	go func(){
		right = MergeSortGoDesc(data[i:])
		wg.Done()
	}()
	wg.Wait()
	//对这二段归并排序
	result := MergeDesc(left, right)
	return result
}
//归并排序(降序排序).
func MergeDesc(one, two []*SliceCache) []*SliceCache {
	i, j := 0, 0
	oneLen := len(one)
	twoLen := len(two)
	result := make([]*SliceCache, 0, oneLen+twoLen)
	for i < oneLen && j < twoLen {
		if one[i].SortColumn > two[j].SortColumn {
			result = append(result, one[i])
			i++
		} else if one[i].SortColumn < two[j].SortColumn {
			result = append(result, two[j])
			j++
		} else {
			result = append(result, one[i])
			i++
			result = append(result, two[j])
			j++
		}
	}
	for i < oneLen {
		result = append(result, one[i])
		i++
	}
	for j < twoLen {
		result = append(result, two[j])
		j++
	}
	return result
}


//---------------------并发简单快速排序(升序排序)----------------------------------------------------
//并发简单快速排序(升序排序),一般的快速排序.没优化.(缺点,会多占空间)
func SimpleQuickSortGoAsc(data []*SliceCache) (result []*SliceCache) {
	if len(data) <= 1 {
		return data
	}

	var wg sync.WaitGroup
	t := data[0]
	var left, mid, right []*SliceCache
	mid = append(mid, t)
	//以第一个为标准,比t小的放左边,比t大的放右边.
	for i, v := range data {
		if i == 0 {
			continue
		}
		if v.SortColumn > t.SortColumn {
			right = append(right, v)
		} else if v.SortColumn < t.SortColumn {
			left = append(left, v)
		} else {
			mid = append(mid, v)
		}
	}
	wg.Add(2)
	//并发以t为标准的左边
	go func() {
		left = SimpleQuickSortGoAsc(left)
		wg.Done()
	}()
	//并发以t为标准的右边
	go func() {
		right = SimpleQuickSortGoAsc(right)
		wg.Done()
	}()
	wg.Wait()
	//先放左边.
	if len(left) > 0 {
		result = append(result, left...)
	}
	//再放中间
	result = append(result, mid...)
	//最后放右边.
	if len(right) > 0 {
		result = append(result, right...)
	}
	return result
}
//---------------------并发简单快速排序(降序排序)----------------------------------------------------
//并发简单快速排序(降序排序).一般的快速排序.没优化.(缺点,会多占空间)
func SimpleQuickSortGoDesc(data []*SliceCache) (result []*SliceCache) {
	if len(data) <= 1 {
		return data
	}

	var wg sync.WaitGroup
	t := data[0]
	var left, mid, right []*SliceCache
	mid = append(mid, t)
	//以第一个为标准,比t小的放左边,比t大的放右边.
	for i, v := range data {
		if i == 0 {
			continue
		}
		if v.SortColumn < t.SortColumn {
			right = append(right, v)
		} else if v.SortColumn > t.SortColumn {
			left = append(left, v)
		} else {
			mid = append(mid, v)
		}
	}
	wg.Add(2)
	//并发以t为标准的左边
	go func() {
		left = SimpleQuickSortGoDesc(left)
		wg.Done()
	}()
	//并发以t为标准的右边
	go func() {
		right = SimpleQuickSortGoDesc(right)
		wg.Done()
	}()
	wg.Wait()
	//先放左边.
	if len(left) > 0 {
		result = append(result, left...)
	}
	//再放中间
	result = append(result, mid...)
	//最后放右边.
	if len(right) > 0 {
		result = append(result, right...)
	}
	return result
}
