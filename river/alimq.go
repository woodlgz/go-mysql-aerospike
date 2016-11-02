package river

import (
	"fmt"
	"strings"
	"net/http"
	"encoding/hex"
	"encoding/base64"
	"crypto/md5"
	"crypto/sha1"
	"crypto/hmac"
	"github.com/siddontang/go/log"
	"github.com/juju/errors"
	"hash"
	"../aerospike"
)

type MqService struct{
	md5Ctx hash.Hash
	sha1Ctx hash.Hash
	mqConfig *MQConfig
	httpClient *http.Client
}

func NewMqService(config *MQConfig) *MqService{
	s:=new(MqService)
	s.mqConfig = config
	s.md5Ctx = md5.New()
	s.sha1Ctx = hmac.New(sha1.New,[]byte(s.mqConfig.SecretKey))
	s.httpClient = http.DefaultClient
	return s
}

func (s *MqService) makeMQMessage(req *AeroSpike.BulkRequest) string{
	switch req.Id.(type){
	case int64:
		return fmt.Sprintf("update:%s:%s:%d",req.Namespace,req.Set,req.Id)
	case string:
		return fmt.Sprintf("update:%s:%s:%s",req.Namespace,req.Set,req.Id)
	default:
		return ""
	}
}

func (s *MqService)calcMd5(input string) string{
	s.md5Ctx.Write([]byte(input))
	result:=s.md5Ctx.Sum(nil)
	s.md5Ctx.Reset()
	return hex.EncodeToString(result)
}

func (s *MqService)calcSignature(input string) string{
	s.sha1Ctx.Write([]byte(input))
	result:=s.sha1Ctx.Sum(nil)
	s.sha1Ctx.Reset()
	return strings.TrimRight(base64.StdEncoding.EncodeToString(result)," ")
}

func (s *MqService)makeMQMessageSignature(topic string,producerId string,msgBody string,t int64) string{
	return s.calcSignature(fmt.Sprintf("%s\n%s\n%s\n%d",topic,producerId,s.calcMd5(msgBody),t))
}

func (s *MqService)makeUrl(baseUrl string,topic string,t int64) string{
	return fmt.Sprintf("%s/message/?topic=%s&time=%d&tag=http&key=http",baseUrl,topic,t);
}

func (s *MqService)SendMessage(msgBody string,timestamp int64) error{
	url:=s.makeUrl(s.mqConfig.Url,s.mqConfig.Topic,timestamp)
	httpReq, err:= http.NewRequest("POST", url,strings.NewReader(msgBody))
	if err!=nil {
		log.Error("error occured when sending message ",err.Error())
		return errors.Errorf("send mq message error")
	}
	httpReq.Header.Add("AccessKey",s.mqConfig.AccessKey)
	httpReq.Header.Add("Signature",s.makeMQMessageSignature(s.mqConfig.Topic,s.mqConfig.ProducerId,msgBody,timestamp))
	httpReq.Header.Add("ProducerId",s.mqConfig.ProducerId)
	//fmt.Printf("ak:%s sign:%s pid:%s",httpReq.Header.Get("AccessKey"),httpReq.Header.Get("Signature"),httpReq.Header.Get("ProducerId"))
	resp,err := s.httpClient.Do(httpReq)
	if err!=nil {
		log.Errorf("error occured when sending message")
		return errors.Errorf("send mq message error %s",err.Error())

	}
	defer resp.Body.Close()
	if resp.StatusCode!=201 {
		log.Errorf("send message failed with status code:%d",resp.StatusCode)
		return errors.Errorf("send message failed,response status %d",resp.StatusCode)
	}
	return nil
}
