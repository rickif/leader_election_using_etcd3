package main

import (
	"context"
	"log"
	"os"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

const (
	HOST           = "host1"
	ETCD_ENDPOINTS = "http://123.456.789.10:2379"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

type Server struct {
	client         *clientv3.Client
	lease          clientv3.Lease
	leaseGrantResp *clientv3.LeaseGrantResponse
}

func NewServer(endpoints []string) *Server {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 2 * time.Second,
	}
	c, err := clientv3.New(cfg)
	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}
	srv := &Server{}
	srv.client = c

	lease := clientv3.NewLease(srv.client)
	srv.lease = lease

	leaseGrantResp, err := lease.Grant(context.TODO(), 10)
	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}
	srv.leaseGrantResp = leaseGrantResp

	leaseKeepAliveChan, err := lease.KeepAlive(context.TODO(), leaseGrantResp.ID)
	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}

	go func() {
		for {
			select {
			case <-leaseKeepAliveChan:
			}
		}
	}()

	return srv
}

func (srv *Server) register(key string) (bool, error) {
	kv := clientv3.NewKV(srv.client)
	resp, err := kv.Txn(context.TODO()).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, HOST, clientv3.WithLease(srv.leaseGrantResp.ID))).
		Commit()
	if err != nil {
		return false, err
	}
	if resp.Succeeded {
		log.Println("i am the primary")
	} else {
		resp, err := srv.client.Get(context.TODO(), key)
		if err != nil {
			log.Println(err)
		}
		for _, kv := range resp.Kvs {
			log.Printf("%s is the primary\n", kv.Value)
		}
	}
	return resp.Succeeded, nil
}

func (srv *Server) watch(key string) bool {
	wch := srv.client.Watch(context.TODO(), key)
	for wresp := range wch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.DELETE:
				return true
			}
		}
	}
	return false
}

func (srv *Server) Register() {
	go func() {
		srv.register("service")
		for {
			if srv.watch("service") {
				srv.register("service")
			}
		}
	}()
}

func main() {
	log.Println("start...")
	endpoints := []string{ETCD_ENDPOINTS}
	srv := NewServer(endpoints)
	srv.Register()
	log.Println("end...")
	select {}
}
