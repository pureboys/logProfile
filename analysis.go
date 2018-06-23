package main

import (
	"flag"
	"time"
	"github.com/sirupsen/logrus"
	"os"
	"bufio"
	"io"
	"strings"
	"github.com/mgutz/str"
	"net/url"
	"crypto/md5"
	"encoding/hex"
	"strconv"
	"github.com/mediocregopher/radix.v2/pool"
)

const HANDLE_DIG = " /dig?"
const HANDLE_MOVIE = "/movie/"
const HANDLE_LIST = "/list/"
const HANDLE_HTML = ".html"

type cmdParams struct {
	logFilePath string
	routineNum  int
}

type digData struct {
	time  string
	url   string
	refer string
	ua    string
}

type urlData struct {
	data  digData
	uid   string
	unode urlNode
}

type urlNode struct {
	unType string
	unRid  int
	unUrl  string
	unTime string
}

type storageBlock struct {
	counterType  string
	storageModel string
	unode        urlNode
}

var log = logrus.New()
//var redisCli redis.Client

func init() {
	log.Out = os.Stdout
	log.SetLevel(logrus.DebugLevel)
	/*
	redisCli,err := redis.Dial("tcp","localhost:6379")
	if err != nil {
		log.Fatalln("Redis connect failed")
	} else {
		defer redisCli.Close()
	}
	*/
}

func main() {
	// 获取参数
	logFilePath := flag.String("logFilePath", "./dig.log", "log file path")
	routineNum := flag.Int("routineNum", 5, "consumer number by go routing")
	l := flag.String("l", "./log/log_file", "this program runtime log target file path")
	flag.Parse()

	params := cmdParams{
		logFilePath: *logFilePath,
		routineNum:  *routineNum,
	}

	// 打印日志
	logFd, err := os.OpenFile(*l, os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		log.Out = logFd
		defer logFd.Close()
	}
	log.Info("Exec start ...")
	log.Infof("Params:logFilePath=%s,routineNum=%d", params.logFilePath, params.routineNum)

	// 初始化channel，用户数据传递
	var logChannel = make(chan string, params.routineNum*3)
	var pvChannel = make(chan urlData, params.routineNum)
	var uvChannel = make(chan urlData, params.routineNum)
	var storageChannel = make(chan storageBlock, params.routineNum)

	//Redis Pool
	redisPool, err := pool.New("tcp", "localhost:6379", params.routineNum*2)
	if err != nil {
		log.Fatalln("Redis pool created failed")
		panic(err)
	} else {
		go func() {
			for {
				redisPool.Cmd("PING")
				time.Sleep(3 * time.Second)
			}
		}()
	}

	// 日志消费
	go readFileLineByLine(params, logChannel)

	// 创建一组日志处理
	for i := 0; i < params.routineNum; i++ {
		go logConsumer(logChannel, pvChannel, uvChannel)
	}
	// 创建pv uv 统计器
	go pvCounter(pvChannel, storageChannel)
	go uvCounter(uvChannel, storageChannel, redisPool)
	// 创建 存储器
	go dataStorage(storageChannel, redisPool)

	time.Sleep(1000 * time.Second)
}

func dataStorage(storageChannel chan storageBlock, redisPool *pool.Pool) {
	for block := range storageChannel {
		prefix := block.counterType + "_"
		setKeys := []string{
			prefix + "day_" + getTime(block.unode.unTime, "day"),
			prefix + "hour_" + getTime(block.unode.unTime, "hour"),
			prefix + "min_" + getTime(block.unode.unTime, "min"),
			prefix + block.unode.unType + "_day_" + getTime(block.unode.unTime, "day"),
			prefix + block.unode.unType + "_hour_" + getTime(block.unode.unTime, "hour"),
			prefix + block.unode.unType + "_min_" + getTime(block.unode.unTime, "min"),
		}

		rowId := block.unode.unRid
		for _, key := range setKeys {
			ret, err := redisPool.Cmd(block.storageModel, key, 1, rowId).Int()
			if ret < 0 || err != nil {
				log.Errorln("dataStorage redis storage error.", block.storageModel,key,
					rowId,)
			}
		}

	}
}

func uvCounter(uvChannel chan urlData, storageChannel chan storageBlock, redisPool *pool.Pool) {
	for data := range uvChannel {
		//HyperLoglog redis
		hyperLogLogKey := "uv_hpll_" + getTime(data.data.time, "day")
		ret, err := redisPool.Cmd("PFADD", hyperLogLogKey, data.uid).Int()
		if err != nil {
			log.Warningln("uvCounter check redis hyperloglog failed, ", err)
		}
		if ret != 1 { // 整型，如果至少有个元素被添加返回 1， 否则返回 0。
			continue
		}
		sItem := storageBlock{
			"uv",
			"ZINCRBY",
			data.unode,
		}
		storageChannel <- sItem
	}
}

func getTime(logTime, timeType string) string {
	var item string
	switch timeType {
	case "day":
		item = "2006-01-02"
		break
	case "hour":
		item = "2006-01-02 15"
		break
	case "min":
		item = "2006-01-02 15:04"
		break
	}
	t, _ := time.Parse(item, time.Now().Format(item))
	return strconv.FormatInt(t.Unix(), 10)
}

func pvCounter(pvChannel chan urlData, storageChannel chan storageBlock) {
	for data := range pvChannel {
		sItem := storageBlock{
			"pv",
			"ZINCRBY",
			data.unode,
		}
		storageChannel <- sItem
	}
}

func logConsumer(logChannel chan string, pvChannel, uvChannel chan urlData) error {
	for logStr := range logChannel {
		// 切割日志字符串，扣出打点上报的数据
		data := cutLogFetchData(logStr)
		// uid 模拟生成uid， md5(refer+ua)
		hasher := md5.New()
		hasher.Write([]byte(data.refer + data.ua))
		uid := hex.EncodeToString(hasher.Sum(nil))

		// 解析工作放到此处
		uData := urlData{data, uid, formatUrl(data.url, data.time),}

		pvChannel <- uData
		uvChannel <- uData
		//log.Infoln(uData)
	}
	return nil
}

func formatUrl(url, t string) urlNode {
	// 量大的着手 详情页>列表页>首页
	pos1 := str.IndexOf(url, HANDLE_MOVIE, 0)
	if pos1 != -1 { // http://localhost:8888/movie/9054.html
		pos1 += len(HANDLE_MOVIE)
		pos2 := str.IndexOf(url, HANDLE_HTML, 0)
		idStr := str.Substr(url, pos1, pos2-pos1)
		id, _ := strconv.Atoi(idStr)
		return urlNode{"movie", id, url, t,}
	} else {
		pos1 = str.IndexOf(url, HANDLE_LIST, 0)
		if pos1 != -1 {
			pos1 += len(HANDLE_LIST)
			pos2 := str.IndexOf(url, HANDLE_HTML, 0)
			idStr := str.Substr(url, pos1, pos2-pos1)
			id, _ := strconv.Atoi(idStr)
			return urlNode{"list", id, url, t,}
		} else {
			return urlNode{"home", 1, url, t,}
		}
	}
}

func cutLogFetchData(logStr string) digData {
	logStr = strings.TrimSpace(logStr)
	pos1 := str.IndexOf(logStr, HANDLE_DIG, 0)
	if pos1 == -1 {
		return digData{}
	}
	pos1 += len(HANDLE_DIG)
	pos2 := str.IndexOf(logStr, " HTTP/", pos1)
	d := str.Substr(logStr, pos1, pos2-pos1)

	urlInfo, err := url.Parse("http://localhost/?" + d)
	if err != nil {
		return digData{}
	}
	data := urlInfo.Query()
	return digData{
		data.Get("time"),
		data.Get("url"),
		data.Get("refer"),
		data.Get("ua"),
	}
}

func readFileLineByLine(params cmdParams, logChannel chan string) error {
	fd, err := os.Open(params.logFilePath)
	if err != nil {
		log.Warningf("ReadFileLineByLine can't open file:%s", params.logFilePath)
		return err
	}
	defer fd.Close()

	count := 0
	reader := bufio.NewReader(fd)
	for {
		line, err := reader.ReadString('\n')
		logChannel <- line
		log.Infof("line:%s", line)
		count++

		if count%(1000*params.routineNum) == 0 { // 每1000*n输出一个日志
			log.Infof("ReadFileLineByLine line:%d", count)
		}
		if err != nil {
			if err == io.EOF {
				time.Sleep(3 * time.Second)
				log.Infof("ReadFileLineByLine wait, readline:%d", count)
			} else {
				log.Warningf("ReadFileLineByLine read log error")
			}
		}
	}

	return nil
}
