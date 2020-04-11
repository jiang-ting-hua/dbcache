package test

import (
	"dbcache/dbcache"
	"strconv"
	"testing"
)

func BenchmarkGetRow(b *testing.B) {

	for i := 0; i < b.N; i++ {
		dbcache.GetRow(strconv.FormatInt(int64(i),10))
	}
}
