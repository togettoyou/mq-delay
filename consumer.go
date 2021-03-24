package mq_delay

import (
	"github.com/streadway/amqp"
)

type consumer struct {
	cli  *Cli
	done chan bool
}

// 创建队列
func (c *consumer) CreateQueue(queueName string) (amqp.Queue, error) {
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
func (c *consumer) BindQueue(queueName, delayExchangeName, routingKey string) error {
	return c.cli.Ch.QueueBind(queueName, routingKey, delayExchangeName, false, nil)
}

// 接受消息
func (c *consumer) Receive(queueName, tag string, handle func(amqp.Delivery)) error {
	deliveries, err := c.cli.Ch.Consume(queueName,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
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
