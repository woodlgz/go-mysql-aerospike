package river

import (
	"testing"
	"time"
	"fmt"
)

func makeRBMqConfig() *MQConfig{
	return &MQConfig{Url:"",UseRabbitMq:true,RabbitMqExchange:"",RabbitMqKey:"",RabbitMqQueue:""}
}

func TestRbMqSendMessage(t *testing.T){
	service:=NewRabbitMqService(makeMqConfig())
	currentTime:=time.Now().Unix()
	service.sendMessage(fmt.Sprintf("test %d",currentTime),currentTime*1000);
}
