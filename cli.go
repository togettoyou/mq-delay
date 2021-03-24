package mq_delay

import (
	"github.com/streadway/amqp"
)

// 客户端
type Cli struct {
	Conn *amqp.Connection
	Ch   *amqp.Channel
}

// 创建一个客户端
func NewCli(url string) (cli *Cli, err error) {
	cli = new(Cli)
	// 连接到RabbitMQ
	cli.Conn, err = amqp.Dial(url)
	if err != nil {
		return
	}
	// 获取信道
	cli.Ch, err = cli.Conn.Channel()
	return
}

// 构建生产者
func (cli *Cli) GetProducer() *producer {
	return &producer{cli: cli}
}

// 构建消费者
func (cli *Cli) GetConsumer() *consumer {
	return &consumer{
		cli:  cli,
		done: make(chan bool),
	}
}

// 创建延时交换机，Direct类型
// 将消息中的RoutingKey与该Exchange关联的所有Binding中的RoutingKey进行比较
// 如果相等，则发送到该Binding对应的Queue中
func (cli *Cli) CreateDelayDirectExchange(delayExchangeName string) error {
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
func (cli *Cli) Close() {
	cli.Conn.Close()
	cli.Ch.Close()
}
