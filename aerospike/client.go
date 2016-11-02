package AeroSpike

import (
	"github.com/aerospike/aerospike-client-go"
	"github.com/juju/errors"
	"fmt"
	"github.com/siddontang/go/log"
)

type AeroSpikeConfig struct {
	Servers []string `toml:"servers"`
	Port     int16   `toml:"as_port"`
	UserName string  `toml:"username"`
	Password string  `toml:"passwd"`
}

type Client struct{
	config *AeroSpikeConfig
	client *aerospike.Client
	policy *aerospike.WritePolicy
}

const (
	INSERT = 	"insert"
	UPDATE =	"update"
	DELETE =	"delete"
)

type BulkRequest struct {
	Action string
	Namespace  string
	Set   string
	Id     interface{}
	Data map[string]interface{}
}

type BulkResponse struct{
	Code int
}

func (c *Client) DoBulk(reqs []*BulkRequest) (*BulkResponse,error){
	for _,req := range reqs{
		switch req.Action {
		case UPDATE, INSERT:
			c.setMultiValues(req.Namespace,req.Set,req.Id,req.Data)
		case DELETE:
			c.del(req.Namespace,req.Set,req.Id)
		}
	}
	return nil,nil
}

func (c *Client) setMultiValues(ns string ,set string,key interface{},values map[string]interface{}){
	k,_:=aerospike.NewKey(ns,set,key)
	bins := make([]*aerospike.Bin,0,20)
	for mk,mv:=range values{
		if mv == nil{
			mv = ""
		}
		bin:=aerospike.NewBin(mk,mv)
		bins = append(bins,bin)
	}
	err:=c.client.PutBins(c.policy,k,bins...)
	if err!=nil{
		fmt.Errorf("error occured when setMultiValues in %s.%s,",ns,set,err.Error())
	}
}

func (c *Client) set(ns string,set string,key interface{},value string) error{
	k,_ := aerospike.NewKey(ns,set,key)
	bin := aerospike.NewBin("value",value)
	c.client.PutBins(c.policy,k,bin)
	return nil
}

func (c *Client) get(ns string,set string,key interface{}) (interface{},error){
	k,_:=aerospike.NewKey(ns,set,key)
	record,err:=c.client.Get(&(c.policy.BasePolicy),k,"value")
	if err!=nil{
		return nil,err
	}
	if record == nil {
		return nil, errors.Errorf("key doesn't exist,key:", key)
	}
	if value,ok:=record.Bins["value"];ok{
		return value,nil
	}else{
		return nil,errors.Errorf("get key ",key," failed")
	}
}

func (c *Client) getField(ns string,set string,key interface{},field string) (interface{},error){
	k,_:=aerospike.NewKey(ns,set,key)
	record,err:=c.client.Get(&(c.policy.BasePolicy),k,field)
	if err!=nil{
		return nil,err
	}
	if record == nil {
		return nil, errors.Errorf("key doesn't exist,key:", key)
	}
	if value,ok := record.Bins[field];ok{
		return value,nil
	}else{
		return nil,errors.Errorf("get key ",key," with field ",field," failed")
	}
}

func (c *Client) del(ns string,set string,key interface{}) error{
	k,_ := aerospike.NewKey(ns,set,key)
	c.client.Delete(c.policy,k)
	return nil
}

func (c *Client) scan(ns string,set string,callback func(*aerospike.Result)) error{
	spolicy := aerospike.NewScanPolicy()
	spolicy.ConcurrentNodes = true
	spolicy.Priority = aerospike.LOW
	spolicy.IncludeBinData = false

	recs, err := c.client.ScanAll(spolicy, ns, set)
	// deal with the error here
	if err!=nil{
		log.Error("scan failed,",err);
		return err
	}
	defer recs.Close()
	for res := range recs.Results() {
		if res.Err != nil {
			log.Error("read record error,",res.Err)
		} else {
			callback(res)
		}
	}
	return nil;
}


func (c *Client) close(){
	c.client.Close()
}

func NewAeroSpike(config *AeroSpikeConfig) (*Client,error) {
	client := new(Client)
	client.config = config
	hosts := make([]*aerospike.Host,0,len(config.Servers))
	for _,h := range config.Servers{
		hosts = append(hosts,aerospike.NewHost(h,int(config.Port)))
	}
	var err error
	client.policy = aerospike.NewWritePolicy(0,0)
	client.policy.CommitLevel = aerospike.COMMIT_ALL
	client.policy.ConsistencyLevel = aerospike.CONSISTENCY_ALL
	client.client,err = aerospike.NewClientWithPolicyAndHost(aerospike.NewClientPolicy(),hosts...)

	return client,err
}