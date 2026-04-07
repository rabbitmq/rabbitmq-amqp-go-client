// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// Performance harness: publishes and consumes on a quorum queue while printing
// per-second publish and consume rates (with colored console output).
//
// Run from the repository root:
//
//	go run ./docs/perf_test -mode publish
//	go run ./docs/perf_test -mode publishAsync -address amqp://guest:guest@localhost:5672/ -queue my-queue
//
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/perf_test/main.go

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// ANSI colors (works in most modern terminals).
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

// formatInt renders n with thousands separators (e.g. 1_234_567 in locale style: 1,234,567).
func formatInt(n uint64) string {
	s := strconv.FormatUint(n, 10)
	if len(s) <= 3 {
		return s
	}
	nDigits := len(s)
	first := nDigits % 3
	if first == 0 {
		first = 3
	}
	var b strings.Builder
	b.Grow(nDigits + (nDigits-1)/3)
	b.WriteString(s[:first])
	for i := first; i < nDigits; i += 3 {
		b.WriteByte(',')
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func main() {
	var (
		address      = flag.String("address", "amqp://guest:guest@localhost:5672/", "AMQP 1.0 broker URI")
		mode         = flag.String("mode", "publish", "Publishing API: publish (sync) or publishAsync")
		queueName    = flag.String("queue", "perf-amqp10-go-queue", "Queue name to declare and use")
		maxInFlight  = flag.Int("maxInFlight", 256, "MaxInFlight for publishAsync (back-pressure limit)")
		pubTimeout   = flag.Duration("publishTimeout", 0, "Broker confirmation timeout for publishAsync (0 = library default)")
		printSummary = flag.Bool("summary", true, "Print a final summary line after shutdown")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nModes:")
		fmt.Fprintln(os.Stderr, "  publish       synchronous Publish() — one in-flight confirmation at a time")
		fmt.Fprintln(os.Stderr, "  publishAsync  asynchronous PublishAsync() — up to -maxInFlight confirmations in parallel")
	}
	flag.Parse()

	m := strings.ToLower(strings.TrimSpace(*mode))
	if m != "publish" && m != "publishasync" {
		fmt.Fprintf(os.Stderr, "%serror:%s unknown -mode %q (use publish or publishAsync)\n", colorRed, colorReset, *mode)
		os.Exit(2)
	}

	var publishTotal, consumeTotal, publishErrors atomic.Uint64
	var running atomic.Bool
	running.Store(true)

	statsStop := make(chan struct{})
	go traceRates(statsStop, &publishTotal, &consumeTotal, &publishErrors, *queueName, m)

	stateChanged := make(chan *rmq.StateChanged, 8)
	go func() {
		for sc := range stateChanged {
			fmt.Printf("%s[connection]%s %v -> %v\n", colorBlue, colorReset, sc.From, sc.To)
		}
	}()

	env := rmq.NewEnvironment(*address, nil)
	amqpConnection, err := env.NewConnection(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sconnection error:%s %v\n", colorRed, colorReset, err)
		os.Exit(1)
	}
	amqpConnection.NotifyStatusChange(stateChanged)

	management := amqpConnection.Management()
	queueInfo, err := management.DeclareQueue(context.Background(), &rmq.QuorumQueueSpecification{
		Name: *queueName,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sdeclare queue:%s %v\n", colorRed, colorReset, err)
		os.Exit(1)
	}

	consumer, err := amqpConnection.NewConsumer(context.Background(), *queueName, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sconsumer:%s %v\n", colorRed, colorReset, err)
		os.Exit(1)
	}

	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	go runConsumer(consumeCtx, consumer, &consumeTotal, &running)

	pubOpts := &rmq.PublisherOptions{}
	if m == "publishasync" {
		pubOpts = &rmq.PublisherOptions{
			MaxInFlight: *maxInFlight,
		}
		if *pubTimeout > 0 {
			pubOpts.PublishTimeout = *pubTimeout
		}
	}

	publisher, err := amqpConnection.NewPublisher(context.Background(),
		&rmq.QueueAddress{Queue: *queueName},
		pubOpts,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%spublisher:%s %v\n", colorRed, colorReset, err)
		cancelConsume()
		os.Exit(1)
	}

	var asyncWG sync.WaitGroup
	startRun := time.Now()
	if m == "publishasync" {
		go runPublisherAsync(context.Background(), publisher, &running, &asyncWG, &publishTotal, &publishErrors)
	} else {
		go runPublisherSync(context.Background(), publisher, &running, &publishTotal, &publishErrors)
	}

	fmt.Printf("%s%sperf_test%s queue=%s mode=%s — press Enter to stop…%s\n",
		colorBold, colorYellow, colorReset, *queueName, m, colorReset)
	_, _ = fmt.Scanln()

	running.Store(false)
	close(statsStop)
	cancelConsume()

	if m == "publishasync" {
		asyncWG.Wait()
	}

	_ = consumer.Close(context.Background())
	_ = publisher.Close(context.Background())

	if *printSummary {
		elapsed := time.Since(startRun)
		fmt.Printf("%s[summary]%s published(accepted)=%s consumed=%s publish_errors=%s elapsed=%s\n",
			colorBold+colorCyan, colorReset,
			formatInt(publishTotal.Load()), formatInt(consumeTotal.Load()), formatInt(publishErrors.Load()),
			elapsed.Round(time.Millisecond))
	}

	purged, err := management.PurgeQueue(context.Background(), queueInfo.Name())
	if err != nil {
		fmt.Fprintf(os.Stderr, "%spurge:%s %v\n", colorRed, colorReset, err)
	} else {
		fmt.Printf("%s[purge]%s removed %s messages\n", colorGreen, colorReset, formatInt(uint64(purged)))
	}
	if err = management.DeleteQueue(context.Background(), queueInfo.Name()); err != nil {
		fmt.Fprintf(os.Stderr, "%sdelete queue:%s %v\n", colorRed, colorReset, err)
	}
	if err = env.CloseConnections(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "%sclose:%s %v\n", colorRed, colorReset, err)
	}
	close(stateChanged)
}

func traceRates(
	done <-chan struct{},
	publishTotal, consumeTotal, publishErrors *atomic.Uint64,
	queueName, mode string,
) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastPub, lastCons, lastErr uint64
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			pt := publishTotal.Load()
			ct := consumeTotal.Load()
			pe := publishErrors.Load()

			pubPerSec := pt - lastPub
			consPerSec := ct - lastCons
			errPerSec := pe - lastErr
			lastPub, lastCons, lastErr = pt, ct, pe

			// Publish line: cyan; consume: green; errors: red fragment.
			fmt.Printf("%s[PUBLISH]%s %s msg/s  total %s  %s[CONSUME]%s %s msg/s  total %s",
				colorCyan+colorBold, colorReset, formatInt(pubPerSec), formatInt(pt),
				colorGreen+colorBold, colorReset, formatInt(consPerSec), formatInt(ct))
			if errPerSec > 0 || pe > 0 {
				fmt.Printf("  %s(pub err/s %s, total err %s)%s", colorRed, formatInt(errPerSec), formatInt(pe), colorReset)
			}
			fmt.Printf("  %s(%s %s)%s\n", colorYellow, queueName, mode, colorReset)
		}
	}
}

func runConsumer(
	ctx context.Context,
	consumer *rmq.Consumer,
	consumeTotal *atomic.Uint64,
	running *atomic.Bool,
) {
	for {
		if !running.Load() {
			return
		}
		deliveryCtx, err := consumer.Receive(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			if running.Load() {
				fmt.Printf("%s[consumer]%s receive error: %v\n", colorRed, colorReset, err)
				time.Sleep(200 * time.Millisecond)
			}
			continue
		}
		consumeTotal.Add(1)
		if err = deliveryCtx.Accept(context.Background()); err != nil && running.Load() {
			fmt.Printf("%s[consumer]%s accept error: %v\n", colorRed, colorReset, err)
		}
	}
}

func runPublisherSync(
	ctx context.Context,
	publisher *rmq.Publisher,
	running *atomic.Bool,
	publishTotal, publishErrors *atomic.Uint64,
) {
	body := []byte("x")
	for running.Load() {
		res, err := publisher.Publish(ctx, rmq.NewMessage(body))
		if err != nil {
			publishErrors.Add(1)
			// in case of network error, just wait a bit before trying to publish the next message
			time.Sleep(500 * time.Millisecond)
			continue
		}
		switch res.Outcome.(type) {
		case *rmq.StateAccepted:
			publishTotal.Add(1)
		default:
			publishErrors.Add(1)
		}
	}
}

func runPublisherAsync(
	ctx context.Context,
	publisher *rmq.Publisher,
	running *atomic.Bool,
	asyncWG *sync.WaitGroup,
	publishTotal, publishErrors *atomic.Uint64,
) {
	body := []byte("x")
	for running.Load() {
		asyncWG.Add(1)
		msg := rmq.NewMessage(body)
		err := publisher.PublishAsync(ctx, msg, func(result *rmq.PublishResult, cbErr error) {
			defer asyncWG.Done()
			if cbErr != nil {
				publishErrors.Add(1)
				return
			}
			switch result.Outcome.(type) {
			case *rmq.StateAccepted:
				publishTotal.Add(1)
			default:
				publishErrors.Add(1)
			}
		})
		if err != nil {
			asyncWG.Done()
			publishErrors.Add(1)
			// in case of network error, just wait a bit before trying to publish the next message
			time.Sleep(500 * time.Millisecond)
			if errors.Is(err, context.Canceled) {
				return
			}
		}
	}
}
