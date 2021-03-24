package mq_delay

import (
	"crypto/tls"
	"github.com/streadway/amqp"
)

// 客户端
type client struct {
	Conn *amqp.Connection
	Ch   *amqp.Channel
}

// 交换机配置
type ExchangeConfig struct {
	Durable, AutoDelete, Internal, NoWait bool
	Args                                  amqp.Table
}

// 创建一个客户端
func NewClient(url string, tlsConfig ...*tls.Config) (cli *client, err error) {
	cli = new(client)
	// 连接到RabbitMQ
	if len(tlsConfig) > 0 {
		cli.Conn, err = amqp.DialTLS(url, tlsConfig[0])
	} else {
		cli.Conn, err = amqp.Dial(url)
	}
	if err != nil {
		return
	}
	// 获取信道
	cli.Ch, err = cli.Conn.Channel()
	return
}

// 构建生产者
func (cli *client) GetProducer() *producer {
	return &producer{cli: cli}
}

// 构建消费者
func (cli *client) GetConsumer() *consumer {
	return &consumer{
		cli:  cli,
		done: make(chan bool),
	}
}

// 创建延时交换机
// Args需指定x-delayed-type，默认为direct类型
func (cli *client) CreateDelayExchange(delayExchangeName string, exchangeConfig ...ExchangeConfig) error {
	if len(exchangeConfig) > 0 {
		return cli.Ch.ExchangeDeclare(
			delayExchangeName,
			"x-delayed-message",
			exchangeConfig[0].Durable,
			exchangeConfig[0].AutoDelete,
			exchangeConfig[0].Internal,
			exchangeConfig[0].NoWait,
			exchangeConfig[0].Args)
	}
	return cli.Ch.ExchangeDeclare(
		delayExchangeName,
		"x-delayed-message",
		true,
		false,
		false,
		false,
		amqp.Table{"x-delayed-type": "direct"})
}

// 关闭客户端
func (cli *client) Close() {
	cli.Conn.Close()
	cli.Ch.Close()
}
