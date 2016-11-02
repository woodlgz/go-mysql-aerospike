package river

import (
	"testing"
	"time"
	"fmt"
)

func makeMqConfig() *MQConfig{
	return &MQConfig{Url:"beijing-rest-internal.ons.aliyun.com",AccessKey:"mEbjOEonoo5TREFS",SecretKey:"xZRP6rejrDjxLxGFHbDfppfJt1S0VJ",ProducerId:"PID_vincent_producer_111",Topic:"guozetest"}
}

func TestSendMessage(t *testing.T){
	service:=NewMqService(makeMqConfig())
	currentTime:=time.Now().Unix()
	service.sendMessage(fmt.Sprintf("test %d",currentTime),currentTime*1000);
}

