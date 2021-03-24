package mq_delay

import (
	"github.com/streadway/amqp"
)

// 生产者
type producer struct {
	cli *client
}

// 发送普通消息
func (p *producer) SendMsg(delayExchangeName, routingKey string, mandatory, immediate bool, msg amqp.Publishing) error {
	return p.cli.Ch.Publish(delayExchangeName, routingKey, mandatory, immediate, msg)
}

// 发送延时消息
// delayExchangeName 交换机名称
// routingKey 路由键
// body 发送内容
// delay 延时时间，秒
func (p *producer) SendTimeoutMsg(delayExchangeName, routingKey string, body []byte, delay int) error {
	return p.cli.Ch.Publish(
		delayExchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				"x-delay": delay * 1000,
			},
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}
