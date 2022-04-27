package system

import "github.com/BrobridgeOrg/gravity-sdk/core"

func InternalServerErr() *core.Error {
	return &core.Error{
		Code:    55000,
		Message: "Internal server error",
	}
}
