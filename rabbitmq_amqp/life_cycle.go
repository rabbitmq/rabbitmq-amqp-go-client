package rabbitmq_amqp

import (
	"fmt"
	"sync"
)

const (
	Open         = iota
	Reconnecting = iota
	Closing      = iota
	Closed       = iota
)

func statusToString(status int) string {
	switch status {
	case Open:
		return "Open"
	case Reconnecting:
		return "Reconnecting"
	case Closing:
		return "Closing"
	case Closed:
		return "Closed"
	}
	return "Unknown"

}

type StatusChanged struct {
	From int
	To   int
}

func (s StatusChanged) String() string {
	return fmt.Sprintf("From: %s, To: %s", statusToString(s.From), statusToString(s.To))
}

type LifeCycle struct {
	status          int
	chStatusChanged chan *StatusChanged
	mutex           *sync.Mutex
}

func NewLifeCycle() *LifeCycle {
	return &LifeCycle{
		status: Closed,
		mutex:  &sync.Mutex{},
	}
}

func (l *LifeCycle) Status() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.status
}

func (l *LifeCycle) SetStatus(value int) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.status == value {
		return
	}

	oldState := l.status
	l.status = value

	if l.chStatusChanged == nil {
		return
	}
	l.chStatusChanged <- &StatusChanged{
		From: oldState,
		To:   value,
	}
}
