package rabbitmq

import (
	"log"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zeromicro/go-zero/core/logx"
)

type (
	RabbitListenerEx struct {
		conn     *amqp.Connection
		channels map[string]*amqp.Channel
		forevers map[string]chan bool
		queues   RabbitListenerConf
		qStopped map[string]int32
		// stopped is set to 1 when stopped
		stopped int32
	}
)

func MustNewListenerEx(listenerConf RabbitListenerConf, c chan *amqp.Error) *RabbitListenerEx {
	listener := &RabbitListenerEx{queues: listenerConf}
	conn, err := amqp.Dial(getRabbitURL(listenerConf.RabbitConf))
	if err != nil {
		log.Fatalf("failed to connect rabbitmq, error: %v", err)
	}
	conn.NotifyClose(c)

	listener.conn = conn
	listener.channels = make(map[string]*amqp.Channel)
	listener.forevers = make(map[string]chan bool)
	listener.qStopped = make(map[string]int32)
	for _, queue := range listenerConf.ListenerQueues {
		channel, errQ := listener.conn.Channel()
		if errQ != nil {
			log.Fatalf("failed to open a channel: %v", errQ)
		}

		listener.channels[queue.Name] = channel
		listener.forevers[queue.Name] = make(chan bool)
		listener.qStopped[queue.Name] = 0
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

func (q RabbitListenerEx) StartWithNotifyError(queueName string, handler ConsumeHandler, c chan *amqp.Error) {
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

		channel.NotifyClose(c)

		go func() {
			for d := range msg {
				if err := handler.Consume(string(d.Body)); err != nil {
					logx.Errorf("Error on consuming: %s, error: %v", string(d.Body), err)
				}
			}
		}()

		qStopped := q.qStopped[queueName]
		atomic.CompareAndSwapInt32(&qStopped, 1, 0)

		<-q.forevers[queueName]
	}
}

func (q RabbitListenerEx) IsChannelClosed(queueName string) bool {
	if channel, ok := q.channels[queueName]; ok {
		return channel.IsClosed()
	}

	return false
}

func (q RabbitListenerEx) IsConnClosed() bool {
	return q.conn.IsClosed()
}

func (q RabbitListenerEx) StopQueue(queueName string) {
	if channel, ok := q.channels[queueName]; ok {
		qStopped := q.qStopped[queueName]
		if !atomic.CompareAndSwapInt32(&qStopped, 0, 1) {
			return
		}
		q.qStopped[queueName] = qStopped

		channel.Close()
		close(q.forevers[queueName])
	}
}

func (q RabbitListenerEx) Stop() {
	if !atomic.CompareAndSwapInt32(&q.stopped, 0, 1) {
		return
	}

	for queueName, channel := range q.channels {
		qStopped := q.qStopped[queueName]
		if !atomic.CompareAndSwapInt32(&qStopped, 0, 1) {
			continue
		}
		q.qStopped[queueName] = qStopped

		channel.Close()
		close(q.forevers[queueName])
	}

	q.conn.Close()
}
