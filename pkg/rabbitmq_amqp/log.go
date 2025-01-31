package rabbitmq_amqp

import "log/slog"

func Info(msg string, args ...any) {
	slog.Info(msg, args...)
}

func Debug(msg string, args ...any) {
	slog.Debug(msg, args...)
}

func Error(msg string, args ...any) {
	slog.Error(msg, args...)
}

func Warn(msg string, args ...any) {
	slog.Warn(msg, args...)
}
