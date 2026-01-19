// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rabbitmqamqp

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
)

var (
	errURIScheme     = errors.New("AMQP scheme must be either 'amqp://', 'amqps://', 'ws://' or 'wss://'")
	errURIWhitespace = errors.New("URI must not contain whitespace")
)

var schemePorts = map[string]int{
	"amqp":  5672,
	"amqps": 5671,
	"wss":   15675,
	"ws":    15675,
}

var defaultURI = URI{
	Scheme:   "amqp",
	Host:     "localhost",
	Port:     5672,
	Username: "guest",
	Password: "guest",
	Vhost:    "/",
}

// URI represents a parsed AMQP URI string.
type URI struct {
	Scheme   string
	Host     string
	Port     int
	Username string
	Password string
	Vhost    string
}

// ParseURI attempts to parse the given AMQP URI according to the spec.
// See http://www.rabbitmq.com/uri-spec.html.
//
// Default values for the fields are:
//
//	Scheme: amqp
//	Host: localhost
//	Port: 5672
//	Username: guest
//	Password: guest
//	Vhost: /
func ParseURI(uri string) (URI, error) {
	builder := defaultURI

	if strings.Contains(uri, " ") {
		return builder, errURIWhitespace
	}

	u, err := url.Parse(uri)
	if err != nil {
		return builder, err
	}

	defaultPort, okScheme := schemePorts[u.Scheme]

	if okScheme {
		builder.Scheme = u.Scheme
	} else {
		return builder, errURIScheme
	}

	host := u.Hostname()
	port := u.Port()

	if host != "" {
		builder.Host = host
	}

	if port != "" {
		port32, err := strconv.ParseInt(port, 10, 32)
		if err != nil {
			return builder, err
		}
		builder.Port = int(port32)
	} else {
		builder.Port = defaultPort
	}

	if u.User != nil {
		builder.Username = u.User.Username()
		if password, ok := u.User.Password(); ok {
			builder.Password = password
		}
	}

	if u.Path != "" {
		if strings.HasPrefix(u.Path, "/") {
			if u.Host == "" && strings.HasPrefix(u.Path, "///") {
				// net/url doesn't handle local context authorities and leaves that up
				// to the scheme handler.  In our case, we translate amqp:/// into the
				// default host and whatever the vhost should be
				if len(u.Path) > 3 {
					builder.Vhost = u.Path[3:]
				}
			} else if len(u.Path) > 1 {
				builder.Vhost = u.Path[1:]
			}
		} else {
			builder.Vhost = u.Path
		}
	}

	return builder, nil
}

// Extract the Uri by omitting the password

func ExtractWithoutPassword(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid-url>"
	}

	if u.User != nil {
		u.User = url.User(u.User.Username())
	}

	return u.String()
}
