package greddis

import (
	"database/sql/driver"
	"fmt"
	"strconv"
)

func toBytesValue(value driver.Value, buf []byte) ([]byte, error) {
	switch d := value.(type) {
	case string:
		buf = append(buf, d...)
		return buf, nil
	case []byte:
		buf = append(buf, d...)
		return buf, nil
	case int:
		buf = strconv.AppendInt(buf, int64(d), 10)
		return buf, nil
	case *string:
		buf = append(buf, *d...)
		return buf, nil
	case *[]byte:
		buf = append(buf, *d...)
		return buf, nil
	case *int:
		buf = strconv.AppendInt(buf, int64(*d), 10)
		return buf, nil
	case driver.Valuer:
		var val, err = d.Value()
		if err != nil {
			return nil, err
		}
		return val.([]byte), err
	}
	return nil, fmt.Errorf("Got non-parseable value")
}
