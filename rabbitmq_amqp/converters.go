package rabbitmq_amqp

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	UnitMb              string = "mb"
	UnitKb              string = "kb"
	UnitGb              string = "gb"
	UnitTb              string = "tb"
	kilobytesMultiplier        = 1000
	megabytesMultiplier        = 1000 * 1000
	gigabytesMultiplier        = 1000 * 1000 * 1000
	terabytesMultiplier        = 1000 * 1000 * 1000 * 1000
)

func CapacityBytes(value int64) int64 {
	return int64(value)
}

func CapacityKB(value int64) int64 {
	return int64(value * kilobytesMultiplier)
}

func CapacityMB(value int64) int64 {
	return int64(value * megabytesMultiplier)
}

func CapacityGB(value int64) int64 {
	return int64(value * gigabytesMultiplier)
}

func CapacityTB(value int64) int64 {
	return int64(value * terabytesMultiplier)
}

func CapacityFrom(value string) (int64, error) {
	if value == "" || value == "0" {
		return 0, nil
	}

	match, err := regexp.Compile("^((kb|mb|gb|tb))")
	if err != nil {
		return 0,
			errors.New(fmt.Sprintf("Capacity, invalid unit size format:%s", value))
	}

	foundUnitSize := strings.ToLower(value[len(value)-2:])

	if match.MatchString(foundUnitSize) {

		size, err := strconv.Atoi(value[:len(value)-2])
		if err != nil {
			return 0, errors.New(fmt.Sprintf("Capacity, Invalid number format: %s", value))
		}
		switch foundUnitSize {
		case UnitKb:
			return CapacityKB(int64(size)), nil

		case UnitMb:
			return CapacityMB(int64(size)), nil

		case UnitGb:
			return CapacityGB(int64(size)), nil

		case UnitTb:
			return CapacityTB(int64(size)), nil
		}

	}

	return 0,
		errors.New(fmt.Sprintf("Capacity, Invalid unit size format: %s", value))
}
