package rabbitmqamqp

import (
	"fmt"
	"strconv"
	"time"
)

func generateNameWithDateTime(name string) string {
	return fmt.Sprintf("%s_%s", name, strconv.FormatInt(time.Now().Unix(), 10))

}
