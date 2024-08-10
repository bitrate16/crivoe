package pupupu_impl

import (
	"encoding/json"
	"strings"
	"time"

	"math/rand"
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	letterIdxBits = 6
	letterIdxMask = 1<<letterIdxBits - 1
	letterIdxMax  = 63 / letterIdxBits
)

var src = rand.NewSource(time.Now().UnixNano())

func RandStringRunes(n int) string {
	sb := strings.Builder{}
	sb.Grow(n)
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}

func ConvertToKVSItem(value interface{}) (*KVSItem, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	var holder KVSItem
	err = json.Unmarshal(bytes, &holder)
	if err != nil {
		return nil, err
	}

	return &holder, nil
}
