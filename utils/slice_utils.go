package utils

import "reflect"

func IsSlice(v interface{}) bool {
	if v == nil {
		return false
	}

	return reflect.TypeOf(v).Kind() == reflect.Slice
}
