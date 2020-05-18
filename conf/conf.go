package conf

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
)

const (
	CONFIG_FILE string = `./config.conf` //配置文件
	TABLES_CONF string = `./cache.conf`  //需要缓存的表信息
	TABLE_FIELD_NAME string = `table_name`   //配置表中,表名字段名
)

func GetCacheTable()(result []string,err error){
	result, err = ParseConfField(TABLES_CONF, TABLE_FIELD_NAME)
	return
}

//从配置文件中,取出指定指段的所有值
func ParseConfField(fileName string,fieldName string ) (result []string,err error){
	//打开文件
	var index int = 0
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		err = fmt.Errorf("打开配置文件[%s]失败, err: %s\n", fileName, err)
		return nil,err
	}
	defer f.Close()
	fileScanner := bufio.NewScanner(f)
	for fileScanner.Scan() {
		index++
		line := fileScanner.Text()
		//去除字符串首尾的空白
		line = strings.TrimSpace(line)
		// 以#或;开头视为注释,空行和注释不读取
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, ";") {
			continue
		}
		//检查是否前缀是[,后缀是]的分组,并取出group组名称.
		if len(line) > 2 && line[0:1] == "[" && line[len(line)-1:] == "]" {
			continue
		}
		//判断是不是具体配置项,判断是不是有等号.
		equalIndex := strings.Index(line, "=")
		if equalIndex == -1 {
			err = fmt.Errorf("第%行语法错误:%w\n", index, err)
			return nil,err
		}
		//按照等号=分割,左边是KEY,右边是VALUE
		key := line[:equalIndex]
		value := line[equalIndex+1:]
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(key) == 0 {
			err = fmt.Errorf("第%行语法错误:%w\n", index, err)
			return nil,err
		}
		if key == fieldName{
			result=append(result,value)
		}
	}
	return result,nil
}
//从配置文件中,取出数据,保存于结构体.
func ParseConfTable(fileName string,groupName string, result interface{}) (err error) {
	t := reflect.TypeOf(result)
	v := reflect.ValueOf(result)
	//Elem()是获取引用类型指针的指向的对象,如果是值类型不需要。
	tElem := t.Elem()
	vElem := v.Elem()
	//result必须是一个指针
	if t.Kind() != reflect.Ptr {
		fmt.Println("必须是一个指针")
		return
	}
	//result必须是一个结构体,并且结构名与配置文件中分段[]名要一样.
	if tElem.Kind() != reflect.Struct {
		fmt.Println("必须是一个结构体")
		return
	}

	//1.打开文件
	var index int = 0

	f, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		err = fmt.Errorf("打开配置文件[%s]失败, err: %s\n", fileName, err)
		return err
	}
	defer f.Close()
	fileScanner := bufio.NewScanner(f)
	group := ""
	//2.将读取的文件数据按照行读取
	for fileScanner.Scan() {
		index++
		line := fileScanner.Text()
		//去除字符串首尾的空白
		line = strings.TrimSpace(line)
		// 以#或;开头视为注释,空行和注释不读取
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, ";") {
			continue
		}
		//检查是否前缀是[,后缀是]的分组,并取出group组名称.
		if len(line) > 2 && line[0:1] == "[" && line[len(line)-1:] == "]" {
			group = line[1 : len(line)-1]
			group = strings.TrimSpace(group)
			continue
		}

		//判断组名与表名是否相等.
		if strings.ToUpper(group) != strings.ToUpper(groupName) {
			continue
		}
		//判断是不是具体配置项,判断是不是有等号.
		equalIndex := strings.Index(line, "=")
		if equalIndex == -1 {
			fmt.Printf("第%行语法错误:%w\n", index, err)
			return
		}
		//按照等号=分割,左边是KEY,右边是VALUE
		key := line[:equalIndex]
		value := line[equalIndex+1:]
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(key) == 0 {
			fmt.Printf("第%行语法错误:%w\n", index, err)
			return
		}
		//利用反射给result赋值
		//遍历结构体的每一个字段和KEY比较,匹配上就赋值.
		for i := 0; i < tElem.NumField(); i++ {
			field := tElem.Field(i)      //取得结构体字段
			tag := field.Tag.Get("conf") //到得该字段的Tag
			//如果配置文件中的Key等于该结构体字段的Tag,就把value值赋给结构体对应字段.
			if key == tag {
				fieldType := field.Type // 拿到每个字段的类型
				//根据字段的类型,对应赋值
				switch fieldType.Kind() {
				case reflect.String:
					vElem.Field(i).SetString(value)
					////也可这样赋值,根据(reflect.ValueOf)中用字段名找到对应的值对象.
					//fieldValue := vElem.FieldByName(field.Name)
					////将配置文件中的value值,赋值给对应的结构体字段
					//fieldValue.SetString(value)
				case reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
					valueInt64, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						err = fmt.Errorf("conf.ParseConf(),strconv.ParseInt(),err:%s \n", err)
						return err
					}
					vElem.Field(i).SetInt(valueInt64)
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					valueUint64, err := strconv.ParseUint(value, 10, 64)
					if err != nil {
						err = fmt.Errorf("conf.ParseConf(),strconv.ParseUint(),err:%s \n", err)
						return err
					}
					vElem.Field(i).SetUint(valueUint64)
				case reflect.Float32, reflect.Float64:
					valueFloat64, err := strconv.ParseFloat(value, 64)
					if err != nil {
						err = fmt.Errorf("conf.ParseConf(),strconv.ParseFloat(),err:%s \n", err)
						return err
					}
					vElem.Field(i).SetFloat(valueFloat64)
				case reflect.Bool:
					valueBool, err := strconv.ParseBool(value)
					if err != nil {
						err = fmt.Errorf("conf.ParseConf(),strconv.ParseBool(),err:%s \n", err)
						return err
					}
					vElem.Field(i).SetBool(valueBool)
				}
			}
		}
	}
	return
}

//从配置文件中,取出数据,保存于结构体.
func ParseConf(fileName string, result interface{}) (err error) {
	t := reflect.TypeOf(result)
	v := reflect.ValueOf(result)
	//Elem()是获取引用类型指针的指向的对象,如果是值类型不需要。
	tElem := t.Elem()
	vElem := v.Elem()
	//result必须是一个指针
	if t.Kind() != reflect.Ptr {
		fmt.Println("必须是一个指针")
		return
	}
	//result必须是一个结构体,并且结构名与配置文件中分段[]名要一样.
	if tElem.Kind() != reflect.Struct {
		fmt.Println("必须是一个结构体")
		return
	}

	//1.打开文件
	var index int = 0

	f, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		err = fmt.Errorf("打开配置文件[%s]失败, err: %s\n", fileName, err)
		return err
	}
	defer f.Close()
	fileScanner := bufio.NewScanner(f)
	group := ""
	//2.将读取的文件数据按照行读取
	for fileScanner.Scan() {
		index++
		line := fileScanner.Text()
		//去除字符串首尾的空白
		line = strings.TrimSpace(line)
		// 以#或;开头视为注释,空行和注释不读取
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, ";") {
			continue
		}
		//检查是否前缀是[,后缀是]的分组,并取出group组名称.
		if len(line) > 2 && line[0:1] == "[" && line[len(line)-1:] == "]" {
			group = line[1 : len(line)-1]
			group = strings.TrimSpace(group)
			continue
		}

		//判断与传进来的结构体名称相等.
		if strings.ToUpper(group) != strings.ToUpper(tElem.Name()) {
			continue
		}
		//判断是不是具体配置项,判断是不是有等号.
		equalIndex := strings.Index(line, "=")
		if equalIndex == -1 {
			fmt.Printf("第%行语法错误:%w\n", index, err)
			return
		}
		//按照等号=分割,左边是KEY,右边是VALUE
		key := line[:equalIndex]
		value := line[equalIndex+1:]
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(key) == 0 {
			fmt.Printf("第%行语法错误:%w\n", index, err)
			return
		}
		//利用反射给result赋值
		//遍历结构体的每一个字段和KEY比较,匹配上就赋值.
		for i := 0; i < tElem.NumField(); i++ {
			field := tElem.Field(i)      //取得结构体字段
			tag := field.Tag.Get("conf") //到得该字段的Tag
			//如果配置文件中的Key等于该结构体字段的Tag,就把value值赋给结构体对应字段.
			if key == tag {
				fieldType := field.Type // 拿到每个字段的类型
				//根据字段的类型,对应赋值
				switch fieldType.Kind() {
				case reflect.String:
					vElem.Field(i).SetString(value)
					////也可这样赋值,根据(reflect.ValueOf)中用字段名找到对应的值对象.
					//fieldValue := vElem.FieldByName(field.Name)
					////将配置文件中的value值,赋值给对应的结构体字段
					//fieldValue.SetString(value)
				case reflect.Int64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
					valueInt64, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						err = fmt.Errorf("conf.ParseConf(),strconv.ParseInt(),err:%s \n", err)
						return err
					}
					vElem.Field(i).SetInt(valueInt64)
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					valueUint64, err := strconv.ParseUint(value, 10, 64)
					if err != nil {
						err = fmt.Errorf("conf.ParseConf(),strconv.ParseUint(),err:%s \n", err)
						return err
					}
					vElem.Field(i).SetUint(valueUint64)
				case reflect.Float32, reflect.Float64:
					valueFloat64, err := strconv.ParseFloat(value, 64)
					if err != nil {
						err = fmt.Errorf("conf.ParseConf(),strconv.ParseFloat(),err:%s \n", err)
						return err
					}
					vElem.Field(i).SetFloat(valueFloat64)
				case reflect.Bool:
					valueBool, err := strconv.ParseBool(value)
					if err != nil {
						err = fmt.Errorf("conf.ParseConf(),strconv.ParseBool(),err:%s \n", err)
						return err
					}
					vElem.Field(i).SetBool(valueBool)
				}
			}
		}
	}
	return
}

//修改配置文件中的值,根据组名,查找对应的key,修改v的值.
func SetConf(groupName string, k string, v string) (err error) {
	//1.打开文件
	var index int = 0
	f, err := os.OpenFile(CONFIG_FILE, os.O_RDWR, 0666)
	if err != nil {
		fmt.Printf("打开配置文件%s失败:%w/n", CONFIG_FILE, err)
		return err
	}
	defer f.Close()
	var newConf string
	group := ""
	fileScanner := bufio.NewScanner(f)
	//一行一行的查找.
	for fileScanner.Scan() {
		index++
		line := fileScanner.Text()
		// 以#或;开头视为注释,空行和注释不读取
		if line == "" {
			newConf = newConf + line + "\n"
			continue
		}
		if strings.HasPrefix(line, "#") {
			newConf = newConf + line + "\n"
			continue
		}
		if strings.HasPrefix(line, ";") {
			newConf = newConf + line + "\n"
			continue
		}
		//line = strings.TrimSpace(line)
		//检查是否前缀是[,后缀是]的分组,并取出group组名称.
		if len(line) > 2 && line[0:1] == "[" && line[len(line)-1:] == "]" {
			group = line[1 : len(line)-1]
			group = strings.TrimSpace(group)
			newConf = newConf + line + "\n"
			continue
		}
		group = strings.ToUpper(group)
		groupName := strings.ToUpper(groupName)
		if group != groupName {
			newConf = newConf + line + "\n"
			continue
		}
		//判断是不是具体配置项,判断是不是有等号.
		index := strings.Index(line, "=")
		if index == -1 {
			newConf = newConf + line + "\n"
			continue
		}
		//按照等号=分割,左边是KEY,右边是VALUE
		key := line[:index]
		value := line[index+1:]
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(key) == 0 {
			newConf = newConf + line + "\n"
			continue
		}
		if key == k {
			newConf = newConf + k + " = " + v + "\n"
			continue
		} else {
			newConf = newConf + line + "\n"
		}
	}
	//重新写入新的配置
	f.Seek(0, 0)
	_, err = f.Write([]byte(newConf))
	if err != nil {
		return err
	}
	return nil
}


