package mq_delay

import (
	"github.com/streadway/amqp"
)

type producer struct {
	cli *Cli
}

// delayExchangeName 交换机名称
// routingKey 路由键
// body 发送内容
// delay 延时时间，秒
func (p *producer) SendTimeoutMsg(delayExchangeName, routingKey, body string, delay int) error {
	return p.cli.Ch.Publish(
		delayExchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				"x-delay": delay * 1000,
			},
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		},
	)
}
