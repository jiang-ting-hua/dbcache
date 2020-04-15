package test

import (
	"dbcache/cache"
	"dbcache/conf"
	"dbcache/db"
	"dbcache/logs"
	"fmt"
	"strconv"
	"sync"
	"testing"
)


func BenchmarkGetRow(t *testing.B) {
	//初始化日志库
	logs.InitLog()
	defer logs.Close()

	//连接数据库,初始化调用.
	db, err := db.ConnectDB()
	if err != nil {
		logs.Fatal("a", "dbcache.ConnectDB().连接数据库失败, err: %s", err)
		return
	}
	defer db.Close()

	//初始化需要缓存的数据,&conf.Users{}是conf包配置的数据库users表的结构体.
	UsersCache, err := cache.InitCache(db, &conf.Users{})
	if err != nil {
		logs.Fatal("a", "dbcache.InitCache().初始化缓存失败, err: %s", err)
		return
	}
	defer UsersCache.Close()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		UsersCache.GetRow(strconv.FormatInt(int64(i),10))
	}
}

// 第一种
func BenchmarkOne(t *testing.B) {
	//初始化日志库
	logs.InitLog()
	defer logs.Close()

	//连接数据库,初始化调用.
	db, err := db.ConnectDB()
	if err != nil {
		logs.Fatal("a", "dbcache.ConnectDB().连接数据库失败, err: %s", err)
		return
	}
	defer db.Close()

	//初始化需要缓存的数据,&conf.Users{}是conf包配置的数据库users表的结构体.
	UsersCache, err := cache.InitCache(db, &conf.Users{})
	if err != nil {
		logs.Fatal("a", "dbcache.InitCache().初始化缓存失败, err: %s", err)
		return
	}
	defer UsersCache.Close()


	t.ResetTimer()
	pkeyValue:="00YS0SW2N4NT7K8HP13E"
	for i := 0; i < t.N; i++ {
		target := make([]*sync.Map, 0, len(UsersCache.SliceDbCache))
		for _, row := range UsersCache.SliceDbCache {
			n := UsersCache.GetRowNum(pkeyValue)
			if n != -1 {
				target = append(target, row)
			}
		}
	}
}

// 第二种
func BenchmarkTwo(t *testing.B) {
	//初始化日志库
	logs.InitLog()
	defer logs.Close()

	//连接数据库,初始化调用.
	db, err := db.ConnectDB()
	if err != nil {
		logs.Fatal("a", "dbcache.ConnectDB().连接数据库失败, err: %s", err)
		return
	}
	defer db.Close()

	//初始化需要缓存的数据,&conf.Users{}是conf包配置的数据库users表的结构体.
	UsersCache, err := cache.InitCache(db, &conf.Users{})
	if err != nil {
		logs.Fatal("a", "dbcache.InitCache().初始化缓存失败, err: %s", err)
		return
	}
	defer UsersCache.Close()


	t.ResetTimer()
	pkeyValue:="00YS0SW2N4NT7K8HP13E"
	for i := 0; i < t.N; i++ {
		target := UsersCache.SliceDbCache[:0]
		for _, row := range UsersCache.SliceDbCache {
			n := UsersCache.GetRowNum(pkeyValue)
			if n != -1 {
				target = append(target, row)
			}
		}
	}
}

// 第三种
func BenchmarkThree(t *testing.B) {
	//初始化日志库
	logs.InitLog()
	defer logs.Close()

	//连接数据库,初始化调用.
	db, err := db.ConnectDB()
	if err != nil {
		logs.Fatal("a", "dbcache.ConnectDB().连接数据库失败, err: %s", err)
		return
	}
	defer db.Close()

	//初始化需要缓存的数据,&conf.Users{}是conf包配置的数据库users表的结构体.
	UsersCache, err := cache.InitCache(db, &conf.Users{})
	if err != nil {
		logs.Fatal("a", "dbcache.InitCache().初始化缓存失败, err: %s", err)
		return
	}
	defer UsersCache.Close()


	t.ResetTimer()
	pkeyValue:="00YS0SW2N4NT7K8HP13E"
	for i := 0; i < t.N; i++ {
		for _, _ = range UsersCache.SliceDbCache {
			n := UsersCache.GetRowNum(pkeyValue)
			if n != -1 {
				UsersCache.SliceDbCache = append(UsersCache.SliceDbCache[:i], UsersCache.SliceDbCache[i+1:]...)
				i--
				break
			}
		}
	}

}

//测试链表删除
func BenchmarkLinkDel(t *testing.B) {
	//初始化日志库
	logs.InitLog()
	defer logs.Close()

	//连接数据库,初始化调用.
	db, err := db.ConnectDB()
	if err != nil {
		logs.Fatal("a", "dbcache.ConnectDB().连接数据库失败, err: %s", err)
		return
	}
	defer db.Close()

	//初始化需要缓存的数据,&conf.Users{}是conf包配置的数据库users表的结构体.
	UsersCache, err := cache.InitCache(db, &conf.Users{})
	if err != nil {
		logs.Fatal("a", "dbcache.InitCache().初始化缓存失败, err: %s", err)
		return
	}
	defer UsersCache.Close()

	t.ResetTimer()
	pkeyValue:="00YS0SW2N4NT7K8HP13E"
	for i := 0; i < t.N; i++ {
		UsersCache.LinkDbCache.DeleteNodePkey(pkeyValue)
	}

}
//测试SliceDel删除
func BenchmarkSliceDel(t *testing.B) {
	//初始化日志库
	logs.InitLog()
	defer logs.Close()

	//连接数据库,初始化调用.
	db, err := db.ConnectDB()
	if err != nil {
		logs.Fatal("a", "dbcache.ConnectDB().连接数据库失败, err: %s", err)
		return
	}
	defer db.Close()

	//初始化需要缓存的数据,&conf.Users{}是conf包配置的数据库users表的结构体.
	UsersCache, err := cache.InitCache(db, &conf.Users{})
	if err != nil {
		logs.Fatal("a", "dbcache.InitCache().初始化缓存失败, err: %s", err)
		return
	}
	defer UsersCache.Close()

	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		pkeyValue:="00YS0SW2N4NT7K8HP13E"+string(strconv.FormatInt(int64(i),10))
		_ ,err:= UsersCache.DelRow(pkeyValue)
		if err != nil {
				fmt.Println(err)
		}
	}

}
