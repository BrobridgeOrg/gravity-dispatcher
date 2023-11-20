package system

import "github.com/BrobridgeOrg/gravity-sdk/v2/core"

type ErrorRPCState struct {
	core.ErrorReply
}

func BadRequestErr() *core.Error {
	return &core.Error{
		Code:    44400,
		Message: "Bad Request",
	}
}

func InternalServerErr() *core.Error {
	return &core.Error{
		Code:    55000,
		Message: "Internal server error",
	}
}

func ForbiddenErr() *core.Error {
	return &core.Error{
		Code:    44403,
		Message: "Forbidden",
	}
}
