package connector

import (
	"os"
	"strconv"
)

const (
	isSecretEnable = "CONNECTOR_SECRET_ENABLE"
)

func IsSecretEnable() bool {
	isEnable, err := strconv.ParseBool(os.Getenv(isSecretEnable))
	if err != nil {
		return false
	}
	return isEnable
}
