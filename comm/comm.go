package comm

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	crRand "crypto/rand"
	"errors"
	"fmt"
	"github.com/axgle/mahonia"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

//根据时区和字符串时间,得到指定时区的时间
//GetZoneTime("America/Cordoba", str)
//GetZoneTime("Asia/Shanghai", str)
func GetZoneTime(zoneName string, timeStr string) (lt time.Time, err error) {
	location, err := time.LoadLocation(zoneName)
	if err != nil {
		err = fmt.Errorf("time.LoadLocation(%s) err: %v", zoneName, err)
		return time.Time{}, err
	}
	lt, err = time.ParseInLocation("2006-01-02 15:04:05", timeStr, location)
	if err != nil {
		err = fmt.Errorf("time.ParseInLocation() err: %v", err)
		return time.Time{}, err
	}
	return lt, nil
}

//判断文件是否存在
func IsFileExist(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil || os.IsExist(err)
}

//判断目录是否存在
func IsPathExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

//创建目录，如果没有就创建。
func MkDir(dir string) (err error) {
	exist, err := IsPathExist(dir)
	if err != nil {
		return fmt.Errorf("get dir error!: %s", err)
	}
	if !exist {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("mkdir failed![%v]\n", err)
		}
	}
	return nil
}

//获取程序运行路径
func GetCurrentDir()(string,error) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return "",err
	}
	return strings.Replace(dir, "\\", "/", -1),nil
}

//获取目录下的所有文件
func GetAllfiles(dirPath string, files []string) ([]string, error) {
	infos, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return files, errors.New("read file dir failed.")
	}
	fullDir := dirPath
	if runtime.GOOS == "windows" {
		fullDir = fullDir + "\\"
	} else {
		fullDir = fullDir + "/"
	}

	for _, f := range infos {
		fullDir := fullDir + f.Name()
		if f.IsDir() {
			files, _ = GetAllfiles(fullDir, files)
		} else {
			files = append(files, fullDir)
		}
	}
	return files, nil
}

//获取当前目录下的文件
func GetDirFiles(dir string) (files []string, err error) {
	f, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, file := range f {
		if !file.IsDir() {
			fileName := file.Name()
			files = append(files, fileName)
		}
	}
	return files, nil
}
//获取文件的总行数
func GetFileLineCount(file io.Reader) (count int, err error) {
	buf := make([]byte, 1024*1024)
	lineTag := []byte{'\n'}
	for {
		n, err := file.Read(buf)
		count = count + bytes.Count(buf[:n], lineTag)
		if err == io.EOF{
			return count, nil
		}
		if err!=nil{
			return count, err
		}
	}
}
//得到有效的文件名.
func GetValidFileName(name string, extName string) string {
	rep := strings.NewReplacer("\n", " ", "/", " ", "|", "-", ": ", "：", ":", "：", "'", "’")
	name = rep.Replace(name)
	if runtime.GOOS == "windows" {
		rep = strings.NewReplacer("\"", " ", "?", " ", "*", " ", "\\", " ", "<", " ", ">", " ")
		name = rep.Replace(name)
	}
	runeName := []rune(name)
	if len(runeName) > 80 {
		name=string(runeName[:80])
	}
	if extName == "" {
		return name
	} else {
		return name+"."+extName
	}
}
//获取文件大小
func GetFileSize(filePath string) (fileSize int64,err error) {
	file, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			err=fmt.Errorf("文件不存在")
			return 0,  err
		}else{
			return 0,  err
		}
	}
	return file.Size(), nil
}
//根据正则,判断是否有数据.
func isRegExists(regexpStr, data string) bool {
	re := regexp.MustCompile(regexpStr)
	allString := re.FindAllStringSubmatch(data, -1)
	if len(allString) > 0 {
		return true
	}
	return false
}

//AES128解密
func DecryptAES128(data, key []byte) ([]byte, error) {
	if len(key) < 1 {
		err := fmt.Errorf("Not a AES(128) Key")
		return nil, err
	}
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("DecryptAES128 panic:", err)
		}
	}()
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()

	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	result := make([]byte, len(data))
	blockMode.CryptBlocks(result, data)
	result = PKCS7UnPadding(result)
	return result, nil
}

//去补码
func PKCS7UnPadding(data []byte) []byte {
	length := len(data)
	unpadding := int(data[length-1])
	return data[:length-unpadding]
}

func pkcs5Padding(cipherText []byte, blockSize int) []byte {
	padding := blockSize - len(cipherText)%blockSize
	padText := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(cipherText, padText...)
}

//AES128加密
func EncryptAES128(data, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	if len(iv) == 0 {
		iv = key
	}
	data = pkcs5Padding(data, blockSize)
	blockMode := cipher.NewCBCEncrypter(block, iv[:blockSize])
	crypted := make([]byte, len(data))
	blockMode.CryptBlocks(crypted, data)
	return crypted, nil
}

//转换字符编码, "GBK", "UTF-8",GB18030 ,HZGB2312的转换
//s := ConvertToString(result, "GBK", "UTF-8")
//s := ConvertToString(result, "UTF-8", "GBK")
func ConvertToString(src string, srcCode string, tagCode string) string {
	srcCoder := mahonia.NewDecoder(srcCode)
	srcResult := srcCoder.ConvertString(src)
	tagCoder := mahonia.NewDecoder(tagCode)
	_, cdata, _ := tagCoder.Translate([]byte(srcResult), true)
	result := string(cdata)
	return result
}

//得到[start,end]之间的随机整数,加锁，1纳秒执行。
func GetRandomInt(start, end int) int {
	var mutex sync.Mutex
	mutex.Lock()
	<-time.After(1 * time.Nanosecond)                    //延时1纳秒
	r := rand.New(rand.NewSource(time.Now().UnixNano())) //根据时间戳生成随机数
	v := start + r.Intn(end-start)
	mutex.Unlock()
	return v
}

//根据时间戳,生成随机数文件名.
func GetRandomName() string {
	timestamp := strconv.Itoa(int(time.Now().UTC().UnixNano()))
	randomNum := strconv.Itoa(GetRandomInt(1, 1000))
	return timestamp + "_" + randomNum
}

//返回随机n个字符串
func GetRandomString(n int) string {
	var byteStr = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	result := make([]byte, n)
	rand.Seed(time.Now().UnixNano())
	for i := range result {
		result[i] = byteStr[rand.Intn(len(byteStr))]
	}
	return string(result)
}

//获取程序执行的路径.
func GetCurrentPath() (string, error) {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return "", err
	}
	path, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}

	if runtime.GOOS == "windows" {
		path = strings.Replace(path, "\\", "/", -1)
	}

	i := strings.LastIndex(path, "/")
	if i < 0 {
		return "", fmt.Errorf(`Can't find "/" or "\".\n`)
	}
	return string(path[0 : i+1]), nil
}

func NewUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(crRand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	// variant bits:see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

// 翻转字符串
func ReverseString(str string) string {
	runes := []rune(str)
	left:=0
	right:= len(runes)-1
	for left < right{
		runes[left], runes[right] = runes[right], runes[left]
		left++
		right--
	}
	return string(runes)
}
//提取以substr分割的中间数据,只取第一个数据.
func ExtractStr(str string, substr string) string {
	i := strings.Index(str, substr)
	if i >= 0 {
		str = str[i+len(substr):]
	}
	i = strings.Index(str, substr)
	if i >= 0 {
		str = str[:i]
	}
	return str
}