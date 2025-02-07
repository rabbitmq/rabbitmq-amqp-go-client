package rabbitmq_amqp

import (
	"fmt"
	"sync"
)

type LifeCycleState interface {
	getState() int
}

type StateOpen struct {
}

func (o *StateOpen) getState() int {
	return open
}

type StateReconnecting struct {
}

func (r *StateReconnecting) getState() int {
	return reconnecting
}

type StateClosing struct {
}

func (c *StateClosing) getState() int {
	return closing
}

type StateClosed struct {
	error error
}

func (c *StateClosed) GetError() error {
	return c.error
}

func (c *StateClosed) getState() int {
	return closed
}

const (
	open         = iota
	reconnecting = iota
	closing      = iota
	closed       = iota
)

func statusToString(status LifeCycleState) string {
	switch status.getState() {
	case open:
		return "open"
	case reconnecting:
		return "reconnecting"
	case closing:
		return "closing"
	case closed:
		return "closed"
	}
	return "unknown"

}

type StateChanged struct {
	From LifeCycleState
	To   LifeCycleState
}

func (s StateChanged) String() string {
	switch s.From.(type) {
	case *StateClosed:

	}

	switch s.To.(type) {
	case *StateClosed:
		return fmt.Sprintf("From: %s, To: %s, Error: %s", statusToString(s.From), statusToString(s.To), s.To.(*StateClosed).error)

	}
	return fmt.Sprintf("From: %s, To: %s", statusToString(s.From), statusToString(s.To))

}

type LifeCycle struct {
	state           LifeCycleState
	chStatusChanged chan *StateChanged
	mutex           *sync.Mutex
}

func NewLifeCycle() *LifeCycle {
	return &LifeCycle{
		state: &StateClosed{},
		mutex: &sync.Mutex{},
	}
}

func (l *LifeCycle) State() LifeCycleState {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.state
}

func (l *LifeCycle) SetState(value LifeCycleState) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.state == value {
		return
	}

	oldState := l.state
	l.state = value

	if l.chStatusChanged == nil {
		return
	}

	l.chStatusChanged <- &StateChanged{
		From: oldState,
		To:   value,
	}
}
