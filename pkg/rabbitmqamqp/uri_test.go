// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rabbitmqamqp

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Test matrix defined on http://www.rabbitmq.com/uri-spec.html
type testURI struct {
	url      string
	username string
	password string
	host     string
	port     int
	vhost    string
	canon    string
}

var uriTests = []testURI{
	{
		url:      "amqp://user:pass@host:10000/vhost",
		username: "user",
		password: "pass",
		host:     "host",
		port:     10000,
		vhost:    "vhost",
		canon:    "amqp://user:pass@host:10000/vhost",
	},

	{
		url:      "amqp://",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     defaultURI.Host,
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://localhost/",
	},

	{
		url:      "amqp://:@/",
		username: "",
		password: "",
		host:     defaultURI.Host,
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://:@localhost/",
	},

	{
		url:      "amqp://user@",
		username: "user",
		password: defaultURI.Password,
		host:     defaultURI.Host,
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://user@localhost/",
	},

	{
		url:      "amqp://user:pass@",
		username: "user",
		password: "pass",
		host:     defaultURI.Host,
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://user:pass@localhost/",
	},

	{
		url:      "amqp://guest:pass@",
		username: "guest",
		password: "pass",
		host:     defaultURI.Host,
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://guest:pass@localhost/",
	},

	{
		url:      "amqp://host",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "host",
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://host/",
	},

	{
		url:      "amqp://:10000",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     defaultURI.Host,
		port:     10000,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://localhost:10000/",
	},

	{
		url:      "amqp:///vhost",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     defaultURI.Host,
		port:     defaultURI.Port,
		vhost:    "vhost",
		canon:    "amqp://localhost/vhost",
	},

	{
		url:      "amqp://host/",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "host",
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://host/",
	},

	{
		url:      "amqp://host/%2F",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "host",
		port:     defaultURI.Port,
		vhost:    "/",
		canon:    "amqp://host/",
	},

	{
		url:      "amqp://host/%2F%2F",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "host",
		port:     defaultURI.Port,
		vhost:    "//",
		canon:    "amqp://host/%2F%2F",
	},

	{
		url:      "amqp://host/%2Fslash%2F",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "host",
		port:     defaultURI.Port,
		vhost:    "/slash/",
		canon:    "amqp://host/%2Fslash%2F",
	},

	{
		url:      "amqp://192.168.1.1:1000/",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "192.168.1.1",
		port:     1000,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://192.168.1.1:1000/",
	},

	{
		url:      "amqp://[::1]",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "::1",
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://[::1]/",
	},

	{
		url:      "amqp://[::1]:1000",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "::1",
		port:     1000,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://[::1]:1000/",
	},

	{
		url:      "amqp://[fe80::1]",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "fe80::1",
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://[fe80::1]/",
	},

	{
		url:      "amqp://[fe80::1]",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "fe80::1",
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://[fe80::1]/",
	},

	{
		url:      "amqp://[fe80::1%25en0]",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "fe80::1%en0",
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://[fe80::1%25en0]/",
	},

	{
		url:      "amqp://[fe80::1]:5671",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "fe80::1",
		port:     5671,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://[fe80::1]:5671/",
	},

	{
		url:      "amqps:///",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     defaultURI.Host,
		port:     schemePorts["amqps"],
		vhost:    defaultURI.Vhost,
		canon:    "amqps://localhost/",
	},

	{
		url:      "amqps://host:1000/",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "host",
		port:     1000,
		vhost:    defaultURI.Vhost,
		canon:    "amqps://host:1000/",
	},
}

var _ = Describe("Parse Test ", func() {
	It("ParseURI", func() {

		for _, test := range uriTests {
			uri, err := ParseURI(test.url)
			Expect(err).To(BeNil())
			Expect(uri.Username).To(Equal(test.username))
			Expect(uri.Password).To(Equal(test.password))
			Expect(uri.Host).To(Equal(test.host))
			Expect(uri.Port).To(Equal(test.port))
			Expect(uri.Vhost).To(Equal(test.vhost))
		}
	})

})
