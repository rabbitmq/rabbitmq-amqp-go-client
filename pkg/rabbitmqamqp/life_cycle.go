package rabbitmqamqp

import (
	"fmt"
	"sync"
)

const (
	open         = iota
	reconnecting = iota
	closing      = iota
	closed       = iota
)

// ILifeCycleState defines the connection state
// see the iota constants for possible states: open, reconnecting, closing, closed
type ILifeCycleState interface {
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

func statusToString(status ILifeCycleState) string {
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

// StateChanged define the connection life cycle
// See ILifeCycleState for more details about the possible states
// Every time the connection state changes,
// a StateChanged struct is sent to the channel defined in LifeCycle.notifyStatusChange method.
// You can use it to manage your connection and react to the state changes, for example,
// by blocking the publishing messages during the reconnection, ex:
// <code>
//
//	 signalBlock := sync.Cond{L: &sync.Mutex{}}
//		stateChanged := make(chan *rmq.StateChanged, 1)
//		go func(ch chan *rmq.StateChanged) {
//			for statusChanged := range ch {
//				rmq.Info("[connection]", "Status changed", statusChanged)
//				switch statusChanged.To.(type) {
//				case *rmq.StateOpen:
//					signalBlock.Broadcast()
//				case *rmq.StateReconnecting:
//					rmq.Info("[connection]", "Reconnecting to the AMQP 1.0 server")
//				case *rmq.StateClosed:
//					StateClosed := statusChanged.To.(*rmq.StateClosed)
//					if errors.Is(StateClosed.GetError(), rmq.ErrMaxReconnectAttemptsReached) {
//						rmq.Error("[connection]", "Max reconnect attempts reached. Closing connection", StateClosed.GetError())
//						signalBlock.Broadcast()
//						isRunning = false
//					}
//
//				}
//			}
//		}(stateChanged)
//
// </code>
// see examples/reliable/reliable.go example for more details.
type StateChanged struct {
	From ILifeCycleState
	To   ILifeCycleState
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
	state           ILifeCycleState
	chStatusChanged chan *StateChanged
	mutex           *sync.Mutex
}

func NewLifeCycle() *LifeCycle {
	return &LifeCycle{
		state: &StateClosed{},
		mutex: &sync.Mutex{},
	}
}

func (l *LifeCycle) State() ILifeCycleState {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.state
}

func (l *LifeCycle) SetState(value ILifeCycleState) {
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
