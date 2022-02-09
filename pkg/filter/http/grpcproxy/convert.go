package grpcproxy

import (
	"fmt"
	"strconv"
)

// Float64 converts the given string representation into representation of a floating point number into float64.
func Float64(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}

// Float32 converts the given string representation of a floating point number into float32.
func Float32(val string) (float32, error) {
	f, err := strconv.ParseFloat(val, 32)
	if err != nil {
		return 0, err
	}
	return float32(f), nil
}

// Int64 converts the given string representation of an integer into int64.
func Int64(val string) (int64, error) {
	return strconv.ParseInt(val, 0, 64)
}

// Int32 converts the given string representation of an integer into int32.
func Int32(val string) (int32, error) {
	i, err := strconv.ParseInt(val, 0, 32)
	if err != nil {
		return 0, err
	}
	return int32(i), nil
}

// Uint64 converts the given string representation of an integer into uint64.
func Uint64(val string) (uint64, error) {
	return strconv.ParseUint(val, 0, 64)
}

// Uint32 converts the given string representation of an integer into uint32.
func Uint32(val string) (uint32, error) {
	i, err := strconv.ParseUint(val, 0, 32)
	if err != nil {
		return 0, err
	}
	return uint32(i), nil
}

// Enum converts the given string into an int32 that should be type casted into the
// correct enum proto type.
func Enum(val string, enumValMap map[string]int32) (int32, error) {
	e, ok := enumValMap[val]
	if ok {
		return e, nil
	}

	i, err := Int32(val)
	if err != nil {
		return 0, fmt.Errorf("%s is not valid", val)
	}
	for _, v := range enumValMap {
		if v == i {
			return i, nil
		}
	}
	return 0, fmt.Errorf("%s is not valid", val)
}
