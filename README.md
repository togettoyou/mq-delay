# 基于RabbitMQ实现的延时消息队列

# 使用
```shell
go get -v github.com/togettoyou/mq-delay
```

# 应用场景

1. 对消息生产和消费有时间窗口要求的场景。例如，在电商交易中超时未支付关闭订单的场景，在订单创建时会发送一条延时消息。这条消息将会在30分钟以后投递给消费者，消费者收到此消息后需要判断对应的订单是否已完成支付。如支付未完成，则关闭订单。如已完成支付则忽略。

1. 通过消息触发延时任务的场景。例如，在指定时间段之后向用户发送提醒消息。

# 实现机制

| 机制 | 实现 |
| ------------ | ------------- |
| 死信Exchange+Queue的消息存活时间 | |
| 死信Exchange+消息的消息存活时间 | |
| [rabbitmq-delayed-message-exchange插件](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange) | ✔️ |

# Example

```go
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
	fail(http.ListenAndServe(":8999", nil))
}

func fail(err error) {
	if err != nil {
		log.Fatalln(err.Error())
	}
}
```

