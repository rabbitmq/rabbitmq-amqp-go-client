package pkg

const (
	responseCode200       = 200
	responseCode201       = 201
	responseCode204       = 204
	responseCode409       = 409
	commandPut            = "PUT"
	commandGet            = "GET"
	commandPost           = "POST"
	commandDelete         = "DELETE"
	commandReplyTo        = "$me"
	managementNodeAddress = "/management"
	linkPairName          = "management-link-pair"
)

const (
	Exchanges = "exchanges"
	Key       = "key"
	Queues    = "queues"
	Bindings  = "bindings"
)

func encodePathSegments(pathSegments string) string {
	//TODO: implement me
	return pathSegments
}

func queuePath(queueName string) string {
	return "/" + Queues + "/" + encodePathSegments(queueName)
}