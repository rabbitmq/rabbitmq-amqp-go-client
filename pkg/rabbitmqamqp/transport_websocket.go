package rabbitmqamqp

import (
	"io"
	"net"
	"time"

	"github.com/gorilla/websocket"
)

type WebSocketConn struct {
	conn   *websocket.Conn
	reader io.Reader
}

func NewWebSocketConn(conn *websocket.Conn) *WebSocketConn {
	return &WebSocketConn{conn: conn}
}

func (ws *WebSocketConn) Read(b []byte) (n int, err error) {
	if ws.reader == nil {
		messageType, reader, err := ws.conn.NextReader()
		if err != nil {
			return 0, err
		}
		if messageType != websocket.BinaryMessage {
			return 0, io.ErrUnexpectedEOF
		}
		ws.reader = reader
	}
	n, err = ws.reader.Read(b)
	if err == io.EOF {
		ws.reader = nil
		err = nil
	}
	return n, err
}

func (ws *WebSocketConn) Write(b []byte) (n int, err error) {
	err = ws.conn.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}

	return len(b), nil
}

func (ws *WebSocketConn) Close() error {
	return ws.conn.Close()
}

func (ws *WebSocketConn) LocalAddr() net.Addr {
	return ws.conn.LocalAddr()
}

func (ws *WebSocketConn) RemoteAddr() net.Addr {
	return ws.conn.RemoteAddr()
}

func (ws *WebSocketConn) SetDeadline(t time.Time) error {
	err := ws.conn.SetReadDeadline(t)
	if err != nil {
		return err
	}
	return ws.conn.SetWriteDeadline(t)
}

func (ws *WebSocketConn) SetReadDeadline(t time.Time) error {
	return ws.conn.SetReadDeadline(t)
}

func (ws *WebSocketConn) SetWriteDeadline(t time.Time) error {
	return ws.conn.SetWriteDeadline(t)
}
