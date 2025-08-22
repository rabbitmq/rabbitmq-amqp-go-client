package rabbitmqamqp

import (
	"fmt"
	"sync"
)

type LifeCycleState interface {
	GetState() int
}

type StateOpen struct {
}

func (o *StateOpen) GetState() int {
	return open
}

type StateReconnecting struct {
}

func (r *StateReconnecting) GetState() int {
	return reconnecting
}

type StateClosing struct {
}

func (c *StateClosing) GetState() int {
	return closing
}

type StateClosed struct {
	error error
}

func (c *StateClosed) GetError() error {
	return c.error
}

func (c *StateClosed) GetState() int {
	return closed
}

const (
	open         = iota
	reconnecting = iota
	closing      = iota
	closed       = iota
)

func statusToString(status LifeCycleState) string {
	switch status.GetState() {
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
		if s.To.(*StateClosed).error == nil {
			return fmt.Sprintf("From: %s, To: %s", statusToString(s.From), statusToString(s.To))
		}
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

func (l *LifeCycle) notifyStatusChange(channel chan *StateChanged) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.chStatusChanged = channel
}
