package rabbitmq

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zeromicro/go-zero/core/logx"
)

type (
	RabbitListenerEx struct {
		conn     *amqp.Connection
		channels map[string]*amqp.Channel
		forevers map[string]chan bool
		queues   RabbitListenerConf
	}
)

func MustNewListenerEx(listenerConf RabbitListenerConf) *RabbitListenerEx {
	listener := &RabbitListenerEx{queues: listenerConf}
	conn, err := amqp.Dial(getRabbitURL(listenerConf.RabbitConf))
	if err != nil {
		log.Fatalf("failed to connect rabbitmq, error: %v", err)
	}

	listener.conn = conn
	listener.channels = make(map[string]*amqp.Channel)
	listener.forevers = make(map[string]chan bool)
	for _, queue := range listenerConf.ListenerQueues {
		channel, err := listener.conn.Channel()
		if err != nil {
			log.Fatalf("failed to open a channel: %v", err)
		}

		listener.channels[queue.Name] = channel
		listener.forevers[queue.Name] = make(chan bool)
	}
	return listener
}

func (q RabbitListenerEx) Start(queueName string, handler ConsumeHandler) {
	if channel, ok := q.channels[queueName]; ok {
		var que *ConsumerConf
		for _, queue := range q.queues.ListenerQueues {
			if queue.Name == queueName {
				que = &queue
				break
			}
		}
		if que == nil {
			log.Fatalf("failed to find the responding queue: %s", queueName)
		}

		msg, err := channel.Consume(
			que.Name,
			"",
			que.AutoAck,
			que.Exclusive,
			que.NoLocal,
			que.NoWait,
			nil,
		)
		if err != nil {
			log.Fatalf("failed to listener, error: %v", err)
		}

		go func() {
			for d := range msg {
				if err := handler.Consume(string(d.Body)); err != nil {
					logx.Errorf("Error on consuming: %s, error: %v", string(d.Body), err)
				}
			}
		}()

		<-q.forevers[queueName]
	}
}

func (q RabbitListenerEx) Stop(queueName string) {
	if channel, ok := q.channels[queueName]; ok {
		channel.Close()
		close(q.forevers[queueName])
	}

	q.conn.Close()
}
