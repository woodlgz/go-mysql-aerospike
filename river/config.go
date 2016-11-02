package river

import (
	"io/ioutil"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"../aerospike"
)

type MQConfig struct{
	UseRabbitMq bool `toml:"useRabbitMq"`
	Url string `toml:"mqUrl"`
	AccessKey string `toml:"mqAccessKey"`
	SecretKey string `toml:"mqSecretKey"`
	ProducerId string `toml:"mqProducerId"`
	Topic	   string `toml:"mqTopic"`
	RabbitMqKey string `toml:"rabbitMqKey"`
	RabbitMqExchange string `toml:"rabbitMqExchange"`
	RabbitMqQueue string `toml:"rabbitMqQueue"`
}

type SourceConfig struct {
	Schema string   `toml:"schema"`
	Tables []string `toml:"tables"`
}


type Config struct {
	MyAddr     string `toml:"my_addr"`
	MyUser     string `toml:"my_user"`
	MyPassword string `toml:"my_pass"`

	ESAddr string `toml:"es_addr"`

	StatAddr string `toml:"stat_addr"`

	ServerID uint32 `toml:"server_id"`
	Flavor   string `toml:"flavor"`
	DataDir  string `toml:"data_dir"`

	DumpExec string `toml:"mysqldump"`

	Sources []SourceConfig `toml:"source"`

	ASConfig []*AeroSpike.AeroSpikeConfig `toml:"aerospike"`

	Rules []*Rule `toml:"rule"`
	Mq *MQConfig  `toml:"messagequeue"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}
