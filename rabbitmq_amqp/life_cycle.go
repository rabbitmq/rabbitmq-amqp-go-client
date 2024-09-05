package rabbitmq_amqp

const (
	Open         = iota
	Reconnecting = iota
	Closing      = iota
	Closed       = iota
)

type StatusChanged struct {
	from int
	to   int
}

type LifeCycle struct {
	status          int
	chStatusChanged chan *StatusChanged
}

func NewLifeCycle() *LifeCycle {
	return &LifeCycle{
		status: Closed,
	}
}

func (l *LifeCycle) Status() int {
	return l.status
}

func (l *LifeCycle) SetStatus(value int) {

	if l.status == value {
		return
	}

	oldState := l.status
	l.status = value

	if l.chStatusChanged == nil {
		return
	}
	l.chStatusChanged <- &StatusChanged{
		from: oldState,
		to:   value,
	}
}
