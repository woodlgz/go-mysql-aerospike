package river

import(
	"fmt"
	"github.com/siddontang/go/log"
	"github.com/streadway/amqp"
	"../aerospike"
)

type RabbitMqService struct{
	mqConfig *MQConfig
	mqConn *amqp.Connection
	mqChannel *amqp.Channel
	mqQueue *amqp.Queue
}

func NewRabbitMqService(config *MQConfig) *RabbitMqService{
	service:=new(RabbitMqService)
	service.mqConfig = config
	if mqConn,err:=amqp.Dial(config.Url);err!=nil{
		log.Errorf("couldn't connect to rabbit mq %s",config.Url)
		return nil
	}else {
		service.mqConn = mqConn
	}
	if mqChannel,err:= service.mqConn.Channel();err!=nil{
		log.Errorf("failed to get channel of mq")
		return nil
	}else{
		service.mqChannel = mqChannel
	}
	if queue,err:=service.mqChannel.QueueDeclare(config.RabbitMqKey,true,false,false,false,nil);err!=nil{
		log.Errorf("failed to declare queue")
		return nil
	}else{
		service.mqQueue = &queue
	}
	return service
}

func (s *RabbitMqService) makeMQMessage(req *AeroSpike.BulkRequest) string{
	switch req.Id.(type){
	case int64:
		return fmt.Sprintf("update:%s:%s:%d",req.Namespace,req.Set,req.Id)
	case string:
		return fmt.Sprintf("update:%s:%s:%s",req.Namespace,req.Set,req.Id)
	default:
		return ""
	}
}

func (s *RabbitMqService)SendMessage(msgBody string,timestamp int64) error{
	err := s.mqChannel.Publish(s.mqConfig.RabbitMqExchange,s.mqQueue.Name,false,true,
			amqp.Publishing{
				ContentType:"text/plain",
				Body : []byte(msgBody),
			})
	return err
}
