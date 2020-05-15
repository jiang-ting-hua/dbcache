package comm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strings"
)

//将数据是map转为struct,结构体必须是指针.注意:结构体首字母要大写.
func MapToStruct(mapData interface{}, structData interface{}) (err error) {
	mapV := reflect.ValueOf(mapData)
	structV := reflect.ValueOf(structData)
	if structV.Kind() != reflect.Ptr {
		err = fmt.Errorf("第二个参数,结构体必须为指针")
		return err
	}
	structV = structV.Elem()
	if structV.Kind() != reflect.Struct {
		err = fmt.Errorf("第二个参数,接收对象必须为结构体")
		return err
	}
	if mapV.Kind() == reflect.Ptr {
		mapV = mapV.Elem()
	}
	if mapV.Kind() != reflect.Map {
		err = fmt.Errorf("第一个参数,对象必须为MAP")
		return err
	}
	keys := mapV.MapKeys()
	if len(keys) < 1 {
		err = fmt.Errorf("第一个参数,MAP无数据")
		return err
	}
	if keys[0].Kind() != reflect.String {
		err = fmt.Errorf("第一个参数,MAP的Key必须是字符串类型")
		return err
	}

	for i := 0; i < structV.NumField(); i++ {
		fieldName := structV.Type().Field(i).Name
		structField := structV.Field(i)
		tag := structV.Type().Field(i).Tag.Get("json")
		//查找map中,key与字段名是否相同的,取出map中的值,赋值给struct
		for _, key := range keys {
			keyStr := key.String()
			keyStr = strings.TrimSpace(keyStr)
			if strings.ToLower(keyStr) == strings.ToLower(fieldName) || strings.ToLower(keyStr) == tag {
				mapValue := mapV.MapIndex(key)
				if mapValue.Kind() != structField.Kind() {
					continue
				}
				switch mapValue.Kind() {
				case reflect.String:
					value := mapValue.String()
					structField.SetString(value)
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					value := mapValue.Int()
					structField.SetInt(value)
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					value := mapValue.Uint()
					structField.SetUint(value)
				case reflect.Float32, reflect.Float64:
					value := mapValue.Float()
					structField.SetFloat(value)
				}
			}
		}
	}
	return nil
}

//将数据是[]map转为[]struct.注意:结构体首字母要大写.第二个参数要初始化空的跟第一个参数一样大小.
func SliceMapToStruct(sliceMapData interface{}, sliceStructData interface{}) (err error) {
	slicemapV := reflect.ValueOf(sliceMapData)
	sliceStructV := reflect.ValueOf(sliceStructData)
	//如果切片为指针
	if sliceStructV.Kind() == reflect.Ptr {
		sliceStructV = sliceStructV.Elem()
	}
	//如果切片的值不为切片
	if sliceStructV.Kind() != reflect.Slice {
		err = fmt.Errorf("第二个参数,接收对象必须为切片,切片的值为Struct")
		return err
	}
	if slicemapV.Kind() == reflect.Ptr {
		slicemapV = slicemapV.Elem()
	}
	if slicemapV.Kind() != reflect.Slice {
		err = fmt.Errorf("第一个参数,对象必须为切片,切片的值为map")
		return err
	}
	//两个切片要长度一样,第二个参数要初始化跟第一个参数一样大小.
	//第二个参数要初始化空的跟第一个参数一样大小.
	if slicemapV.Len() != sliceStructV.Len() {
		err = fmt.Errorf("两个参数切片大小要一样,第二个参数要初始化空的跟第一个参数一样大小.")
		return err
	}

	for i := 0; i < slicemapV.Len(); i++ {
		mapV := slicemapV.Index(i)
		structV := sliceStructV.Index(i)
		if structV.Kind()== reflect.Ptr{
			structV=structV.Elem()
		}
		if structV.Kind()!= reflect.Struct{
			err = fmt.Errorf("第二个参数,接收对象必须为切片结构体[]struct{}")
			return err
		}

		keys := mapV.MapKeys()
		if len(keys) < 1 {
			continue
		}
		if keys[0].Kind() != reflect.String {
			err = fmt.Errorf("第一个参数,MAP的Key必须是字符串类型")
			return err
		}

		for k := 0; k < structV.NumField(); k++ {
			fieldName := structV.Type().Field(k).Name
			structField := structV.Field(k)
			tag := structV.Type().Field(k).Tag.Get("json")
			//查找map中,key与字段名是否相同的,取出map中的值,赋值给struct
			for _, key := range keys {
				keyStr := key.String()
				keyStr = strings.TrimSpace(keyStr)
				if strings.ToLower(keyStr) == strings.ToLower(fieldName) || strings.ToLower(keyStr) == tag {
					mapValue := mapV.MapIndex(key)
					if mapValue.Kind() != structField.Kind() {
						continue
					}
					switch mapValue.Kind() {
					case reflect.String:
						value := mapValue.String()
						structField.SetString(value)
					case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						value := mapValue.Int()
						structField.SetInt(value)
					case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
						value := mapValue.Uint()
						structField.SetUint(value)
					case reflect.Float32, reflect.Float64:
						value := mapValue.Float()
						structField.SetFloat(value)
					}
				}
			}

		}
	}
	return nil
}

//int转二进制byte字节
func IntToByte(n int)[]byte{
	byteBuffer:=bytes.NewBuffer([]byte{}) // 用于保存字节集合
	data:=int64(n)
	//把整数转换为二进制,写入byteBuffer字节集合
	binary.Write(byteBuffer,binary.BigEndian,data)
	return byteBuffer.Bytes() //返加字节集合.
}
//二进制byte字节转换为int
func BytetoInt(bs []byte)int{
	byteBuffer:=bytes.NewBuffer(bs) // 用于保存字节集合
	var data int64
	//从byte二进制字节中,读取数据转换,保存在data中.
	binary.Read(byteBuffer,binary.BigEndian,&data)
	return int(data)
}
//二进制byte字节转Float64
func ByteToFloat64(bs []byte)float64{
	bits:=binary.LittleEndian.Uint64(bs) //解码
	return math.Float64frombits(bits)
}
//Float64转二进制byte字节
func Float64ToByte(f64 float64)[]byte{
	bits:=math.Float64bits(f64)
	bytes:=make([]byte,8) //float64占8个字节
	binary.LittleEndian.PutUint64(bytes,bits) //无符号填充
	return bytes
}
//二进制byte字节转Float32
func ByteToFloat32(bs []byte)float32{
	bits:=binary.LittleEndian.Uint32(bs) //解码
	return math.Float32frombits(bits)
}
//Float32转二进制byte字节
func Float32ToByte(f32 float32)[]byte{
	bits:=math.Float32bits(f32)
	bytes:=make([]byte,4) //float32占4个字节
	binary.LittleEndian.PutUint32(bytes,bits) //无符号填充
	return bytes
}