package kodo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/qiniu/api.v7/auth/qbox"
	"github.com/qiniu/api.v7/storage"
)

const (
	defaultRetBody = `{"key":"$(key)","hash":"$(etag)","fsize":"$(fsize)","bucket":"$(bucket)","name":"$(x:name)"}`
)

var (
	DefaultClient *Client
	ErrMissFops   = errors.New("persistent operations should not be empty")
)

// State 描述上传文件后的操作状态
type State struct {
	Key    string
	Hash   string
	FSize  string
	Bucket string
	Name   string
}

// FopState 文件处理状态描述
type FopState struct {
	ID   string
	Code int
	Desc string
	Res  []FopResult
}

type FopResult struct {
	Code   int
	Desc   string
	ResKey string
	Cmd    string
	Err    string
}

// Client 七牛对象存储客户端
type Client struct {
	accessKey  string
	secretKey  string
	bucket     string
	domain     string
	config     *storage.Config
	mac        *qbox.Mac
	bktManager *storage.BucketManager
	fopManager *storage.OperationManager
	uploader   *storage.FormUploader
}

func InitDefaultClient(ak, sk, bucket, domain string, args ...interface{}) {
	DefaultClient = NewClient(ak, sk, bucket, domain, args...)
}

// NewClient args 是可选参数
// args[0] = useHTTPS bool
// args[1] = useCDN   bool
// args[2] = zoneConf     *conf.ZoneConf 在私有环境部署中需要指定 zone
func NewClient(ak, sk, bucket, domain string, args ...interface{}) *Client {

	var (
		useHTTPS bool
		useCDN   bool
		zone     *storage.Zone
	)

	switch len(args) {
	case 3:
		zoneConf := args[2].(*ZoneConf)
		if zoneConf != nil {
			zone = &storage.Zone{
				SrcUpHosts: zoneConf.SrcUpHosts,
				CdnUpHosts: zoneConf.CdnUpHosts,
				RsfHost:    zoneConf.RsfHost,
				RsHost:     zoneConf.RsHost,
				ApiHost:    zoneConf.ApiHost,
				IovipHost:  zoneConf.IoVipHost,
			}
		}
		fallthrough
	case 2:
		useCDN = args[1].(bool)
		fallthrough
	case 1:
		useHTTPS = args[0].(bool)
	}

	mac := qbox.NewMac(ak, sk)

	cfg := storage.Config{
		UseHTTPS:      useHTTPS,
		UseCdnDomains: useCDN,
		Zone:          zone,
	}

	bktManager := storage.NewBucketManager(mac, &cfg)
	fopManager := storage.NewOperationManager(mac, &cfg)
	uploader := storage.NewFormUploader(&cfg)

	return &Client{
		accessKey:  ak,
		secretKey:  sk,
		bucket:     bucket,
		domain:     domain,
		config:     &cfg,
		mac:        mac,
		bktManager: bktManager,
		uploader:   uploader,
		fopManager: fopManager,
	}

}

// extra 用于构造上传到七牛云存储上的扩展名
func (_ *Client) extra(p map[string]string) storage.PutExtra {
	return storage.PutExtra{
		Params: p,
	}
}

// Writer 把 reader 中的数据流写入七牛对象存储 args 是可选参数
// pipName  = args[0] string 七牛多媒体处理队列名字
// fops = args[1] string 文件操作指令，详情看注
// extraParams   = args[2] map[string]string 用户自定义的扩展信息，格式形如: map[string]string{"x:name":"zihua","x:password":"123456"}
// retBody       = args[3] string 是用户自定义回调结构体，格式形如: `{"key":"$(key)","hash":"$(etag)","fsize":$(fsize),"bucket":"$(bucket)","name":"$(x:name)"}`
// cbURL         = args[4] string 客户自定义的回调地址，格式形如: "http://www.aabb.com"

// 注:更多操作指令请看 https://developer.qiniu.com/kodo/sdk/1238/go#8
func (client *Client) Writer(path string, reader io.Reader, fSize int64, overWriter bool, args ...interface{}) (*State, error) {
	var (
		pipName     string
		fops        string
		retBody     = defaultRetBody
		extraParams = make(map[string]string)
		cbURL       string
		cbBody      string
		scope       = client.bucket
	)

	if overWriter {
		scope = fmt.Sprintf("%s:%s", client.bucket, path)
	}

	switch len(args) {
	case 1:
		return nil, ErrMissFops
	case 5:
		cbURL = args[4].(string)
		fallthrough
	case 4:
		retBody = args[3].(string)
		cbBody = retBody
		fallthrough
	case 3:
		extraParams = args[2].(map[string]string)
		fallthrough
	case 2:
		pipName = args[0].(string)
		fops = args[1].(string)
	}

	extraParams["x:name"] = path

	policy := &storage.PutPolicy{
		Scope:              scope,
		CallbackURL:        cbURL,
		ReturnBody:         retBody,
		PersistentOps:      fops,
		CallbackBody:       cbBody,
		PersistentPipeline: pipName,
	}
	ret := State{}
	extra := client.extra(extraParams)
	err := client.uploader.Put(
		context.Background(),
		&ret,
		policy.UploadToken(client.mac),
		path,
		reader,
		fSize,
		&extra,
	)
	return &ret, err
}

// args 参数请看 Writer 参数说明
func (client *Client) Push(path string, data []byte, overWriter bool, args ...interface{}) (*State, error) {
	return client.Writer(path, bytes.NewReader(data), int64(len(data)), overWriter, args...)
}

func (client *Client) Bucket() string {
	return client.bucket
}

func (client *Client) URLFor(key string) string {
	deadline := time.Now().Add(time.Minute * 60).Unix()
	return storage.MakePrivateURL(client.mac, client.domain, key, deadline)
}

// KeyURL 为自定域名下的 key 生成下载 url
func (client *Client) KeyURL(domain, key string) string {
	deadline := time.Now().Add(time.Minute * 60).Unix()
	return storage.MakePrivateURL(client.mac, domain, key, deadline)
}

// KeyList 通前缀列举文件
// prefix 指定前缀，args 为可变参数列表
// args[0]=n int，默认 n = math.MaxInt64 表示查询所有的文件
// args[1]=limit int，默认 limit = 1000
// args[2]=marker string，默认 marker = ""
// args[3]=delimiter string，默认 delimiter = ""
func (client *Client) KeyList(prefix string, args ...interface{}) ([]string, string) {
	var (
		limit     = 1000
		marker    = ""
		delimiter = ""
		keys      = make([]string, 0)
		n         = math.MaxInt64
	)
	switch len(args) {
	case 4:
		delimiter = args[3].(string)
		fallthrough
	case 3:
		marker = args[2].(string)
		fallthrough
	case 2:
		limit = args[1].(int)
		fallthrough
	case 1:
		n = args[0].(int)
	}

	for {
		items, _, nextMarker, hashNext, err := client.bktManager.ListFiles(client.bucket, prefix, delimiter, marker, limit)
		if err != nil {
			break
		}

		for _, item := range items {
			keys = append(keys, item.Key)
			if len(keys) >= n {
				return keys, nextMarker
			}
		}

		if hashNext {
			marker = nextMarker
		} else {
			break
		}

	}
	return keys, marker
}

func (client *Client) KeyExist(key string) bool {
	_, err := client.bktManager.Stat(client.bucket, key)
	if err != nil {
		return false
	}
	return true
}

// Fop 文件处理操作
// key 要处理的文件的 key
// fops 文件操作指令
// pipName 七牛多媒体处理队列名字
// args[0]=notifyURL 通知回调 url，
// args[1]=force 是否强制重新执行数据处理任务
func (client *Client) Fop(key, fops, pipName string, args ...interface{}) (string, error) {
	var (
		force     = true
		notifyURL string
	)

	switch len(args) {
	case 2:
		force = args[1].(bool)
		fallthrough
	case 1:
		notifyURL = args[0].(string)
	}

	return client.fopManager.Pfop(
		client.bucket,
		key,
		fops,
		pipName,
		notifyURL,
		force,
	)
}

func (client *Client) PreFop(pid string) (*FopState, error) {
	ret, err := client.fopManager.Prefop(pid)
	if err != nil {
		return nil, err
	}
	fopState := &FopState{
		Code: ret.Code,
		ID:   ret.ID,
		Desc: ret.Desc,
	}

	res := make([]FopResult, 0)

	for _, item := range ret.Items {
		tmp := FopResult{
			Code:   item.Code,
			Desc:   item.Desc,
			ResKey: item.Key,
			Cmd:    item.Cmd,
			Err:    item.Error,
		}
		res = append(res, tmp)
	}

	fopState.Res = res
	return fopState, err
}
