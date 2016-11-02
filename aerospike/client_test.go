package AeroSpike

import (
	"testing"
	"log"
)

func makeAerospikeConfig() *AeroSpikeConfig{
	return &AeroSpikeConfig{
		Servers:[]string{"192.168.2.31","192.168.2.32"},
		Port:3000,
		UserName:"mtty",
		Password:""}
}

func tryGet(asClient *Client,namespace string,set string,key interface{}){
	value,err:=asClient.get(namespace,set,key)
	if(err!=nil){
		log.Fatalf("get value error",err)
	}else {
		log.Print("get value success,value for key 1:",value)
	}
}

func tryGetField(asClient *Client,namespace string,set string,key interface{},field string){
	value,err:=asClient.getField(namespace,set,key,field)
	if(err!=nil){
		log.Fatalf("get value error",err)
	}else {
		log.Print("get value success,value for key 1:",value)
	}
}

func TestAerospikeSet(t *testing.T){
	t.Logf("testing AerospikeSet")
	config:=makeAerospikeConfig()
	asClient,err:=NewAeroSpike(config)
	if err != nil{
		t.Fatalf("can not create aerospike client",err)
	}
	asClient.set("testas","test",1,"helloworld")
	asClient.set("testas","test","abc","hahaha")
	tryGet(asClient,"testas","test",1)
	tryGet(asClient,"testas","test","abc")
	multivalues:=make(map[string]interface{},10)
	multivalues["field1"] = "abc"
	multivalues["field2"] = 3;
	asClient.setMultiValues("testas","test","ccc",multivalues)
	tryGetField(asClient,"testas","test","ccc","field1")
	tryGetField(asClient,"testas","test","ccc","field2")
	t.Logf("TestAerospikeSet ok")
}

func TestAerospikeDel(t *testing.T){
	t.Logf("testing AerospikeDel")
	config:=makeAerospikeConfig()
	asClient,err:=NewAeroSpike(config)
	if err != nil{
		t.Fatalf("can not create aerospike client",err)
	}
	asClient.set("testas","test","delkey","1")
	asClient.del("testas","test","delkey")
	tryGet(asClient,"testas","test","delKey")
	t.Logf("TestAerospikeDel ok")
}
