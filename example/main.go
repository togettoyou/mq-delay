package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"github.com/togettoyou/mq-delay"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"
)

const (
	url               = "amqp://guest:guest@127.0.0.1:5672/"
	delayExchangeName = "delayed.exchange"
	routingKey        = "hello-delayed-routingKey"
	queueName         = "hello-queue"
	consumerTag       = "hello-consumer"
)

func main() {
	// 创建客户端
	cli, err := mq_delay.NewClient(url)
	fail(err)

	// 创建延时交换机
	fail(cli.CreateDelayExchange(delayExchangeName))

	// 构建消费者
	consumer := cli.GetConsumer()

	// 创建队列
	queue, err := consumer.CreateQueue(queueName)
	fail(err)

	// 绑定队列
	fail(consumer.BindQueue(queue.Name, delayExchangeName, routingKey, false, nil))

	// 实时接受消息
	fail(consumer.Receive(queue.Name, consumerTag, func(d amqp.Delivery) {
		log.Println(d.DeliveryTag, string(d.Body))
	}))

	//go func() {
	//	time.Sleep(10 * time.Second)
	//	fmt.Println("停止接受消息")
	//	consumer.StopReceive()
	//}()

	// 构建生产者
	mu := sync.Mutex{}
	cond := sync.NewCond(&mu)
	producer := cli.GetProducer()
	for i := 0; i < 100000; i++ {
		go func() {
			cond.L.Lock()
			defer cond.L.Unlock()
			cond.Wait()
			fmt.Println("开始")
			fail(producer.SendTimeoutMsg(delayExchangeName, routingKey, []byte("你好-"+time.Now().String()), 5))
		}()
	}
	go func() {
		time.Sleep(10 * time.Second)
		cond.Broadcast()
	}()
	http.ListenAndServe(":8999", nil)
}

func fail(err error) {
	if err != nil {
		log.Fatalln(err.Error())
	}
}
