package mq_delay

import (
	"github.com/streadway/amqp"
)

// 消费者
type consumer struct {
	cli  *client
	done chan bool
}

// 队列配置
type QueueConfig struct {
	Durable, AutoDelete, Exclusive, NoWait bool
	Args                                   amqp.Table
}

// 接受消息配置
type ReceiveConfig struct {
	AutoAck, Exclusive, NoLocal, NoWait bool
	Args                                amqp.Table
}

// 创建队列
func (c *consumer) CreateQueue(queueName string, queueConfig ...QueueConfig) (amqp.Queue, error) {
	if len(queueConfig) > 0 {
		return c.cli.Ch.QueueDeclare(
			queueName,
			queueConfig[0].Durable,
			queueConfig[0].AutoDelete,
			queueConfig[0].Exclusive,
			queueConfig[0].NoWait,
			queueConfig[0].Args,
		)
	}
	return c.cli.Ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
}

// 绑定交换机
func (c *consumer) BindQueue(queueName, exchangeName, routingKey string, noWait bool, args amqp.Table) error {
	return c.cli.Ch.QueueBind(queueName, routingKey, exchangeName, noWait, args)
}

// 接受消息
func (c *consumer) Receive(queueName, tag string, handle func(amqp.Delivery), receiveConfig ...ReceiveConfig) error {
	var deliveries <-chan amqp.Delivery
	var err error
	if len(receiveConfig) > 0 {
		deliveries, err = c.cli.Ch.Consume(queueName,
			tag,
			receiveConfig[0].AutoAck,
			receiveConfig[0].Exclusive,
			receiveConfig[0].NoLocal,
			receiveConfig[0].NoWait,
			receiveConfig[0].Args,
		)
	} else {
		deliveries, err = c.cli.Ch.Consume(queueName,
			tag,
			false,
			false,
			false,
			false,
			nil,
		)
	}
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case d, ok := <-deliveries:
				if ok {
					go handle(d)
					d.Ack(true)
				} else {
					return
				}
			case <-c.done:
				return
			}
		}
	}()
	return nil
}

// 停止接受消息
func (c *consumer) StopReceive() {
	c.done <- true
}
