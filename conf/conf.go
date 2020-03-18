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
)

//数据库数据同步.
type DataSync struct {
	RealTime            bool   `conf:"real_time"`              //是否实时同步更新,true:实时更新,false:异步更新.
	AsyncMaxChan        int    `conf:"async_max_chan"`         //异步更新管道,最大缓存数.
	AsyncFilePath       string `conf:"async_file_path"`        //异步保存需要更新的SQL语句文件路径
	AsyncFileName       string `conf:"async_file_name"`        //异步保存需要更新的SQL语句文件名
	AsyncFailedFileName string `conf:"async_failed_file_name"` //异步保存失败的需要更新的SQL语句文件名
	MaxAsyncFileSize    int64  `conf:"max_async_file_size"`    //异步保存需要更新的SQL语句文件和失败,单个文件最大大小
}

//注意,配置文件config.conf中的组名[DbConfig]和结构体名称一样.
type DbConfig struct {
	User         string `conf:"user_name"`    //数据库登录用户名
	Pwd          string `conf:"password"`     //数据库密码
	Ip           string `conf:"ip_address"`   //IP地址
	Port         string `conf:"ip_port"`      //端口
	DatabaseName string `conf:"db_name"`      //数据库名
	ConnectInfo  string `conf:"connect_info"` //连接时其它配置.
}

//注意,配置文件cache.conf中的组名[Table]和结构名称一样.
type Table struct {
	TableName         string `conf:"table_name"`          //缓存的表名
	Columns           string `conf:"columns"`             //缓存的多列,以分号隔开
	Pkey              string `conf:"pkey"`                //缓存表的主键
	Where             string `conf:"where"`               //缓存表,取数据时,加的where条件.
	PkeyAutoIncrement bool   `conf:"pkey_auto_increment"` //缓存表,主键是否为自增列
}

func (t *Table) GetPkey() (Pkey string) {
	return t.Pkey
}
func (t *Table) GetTableName() (tableName string) {
	return t.TableName
}
func (t *Table) GetWhere() (where string) {
	return t.Where
}
func (t *Table) GetColumn() (Columns string) {
	return t.Columns
}
func (t *Table) GetColumns() (Columns []string) {
	if t.Columns == "" {
		return nil
	}
	Columns = strings.Split(t.Columns, ",")
	for i, _ := range Columns {
		Columns[i] = strings.TrimSpace(Columns[i])
	}

	return Columns
}
func (t *Table) PkeyIsIncrement() (isIncrement bool) {
	return t.PkeyAutoIncrement
}

//从配置文件中,取出数据,保存于结构体.
func ParseConf(fileName string, result interface{}) (err error) {
	t := reflect.TypeOf(result)
	v := reflect.ValueOf(result)
	//Elem()是获取引用类型指针的指向的对象,如果是值类型不需要。
	tElem := t.Elem()
	vElem := v.Elem()

	if t.Kind() != reflect.Ptr {
		fmt.Println("result必须是一个指针")
		return
	}

	if tElem.Kind() != reflect.Struct {
		fmt.Println("result必须是一个结构体")
		return
	}

	//1.打开文件
	var index int = 0

	f, err := os.OpenFile(fileName, os.O_RDONLY, 0)
	if err != nil {
		fmt.Printf("打开配置文件%s失败:%w\n", fileName, err)
		return err
	}
	defer f.Close()
	fileScanner := bufio.NewScanner(f)
	group := ""
	for fileScanner.Scan() {
		index++
		line := fileScanner.Text()
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, ";") {
			continue
		}
		if len(line) > 2 && line[0:1] == "[" && line[len(line)-1:] == "]" {
			group = line[1 : len(line)-1]
			group = strings.TrimSpace(group)
			continue
		}

		if strings.ToUpper(group) != strings.ToUpper(tElem.Name()) {
			continue
		}

		equalIndex := strings.Index(line, "=")
		if equalIndex == -1 {
			fmt.Printf("第%行语法错误:%w\n", index, err)
			return
		}

		key := line[:equalIndex]
		value := line[equalIndex+1:]
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(key) == 0 {
			fmt.Printf("第%行语法错误:%w\n", index, err)
			return
		}

		for i := 0; i < tElem.NumField(); i++ {
			field := tElem.Field(i)
			tag := field.Tag.Get("conf")
			if key == tag {
				fieldType := field.Type
					switch fieldType.Kind() {
				case reflect.String:
					vElem.Field(i).SetString(value)
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

	for fileScanner.Scan() {
		index++
		line := fileScanner.Text()

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

		index := strings.Index(line, "=")
		if index == -1 {
			newConf = newConf + line + "\n"
			continue
		}

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
