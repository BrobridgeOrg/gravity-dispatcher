package system

import (
	"fmt"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	"github.com/BrobridgeOrg/gravity-sdk/v2/core"
	"go.uber.org/zap"
)

type CoreRPC struct {
	RPC

	system    *System
	connector *connector.Connector
}

func NewCoreRPC(s *System) *CoreRPC {

	rpc := NewRPC(s.connector)

	crpc := &CoreRPC{
		RPC:       rpc,
		system:    s,
		connector: s.connector,
	}

	return crpc
}

func (crpc *CoreRPC) initialize() error {

	// Initialize RPC handlers
	prefix := fmt.Sprintf(core.CoreAPI, crpc.connector.GetDomain())

	logger.Info("Initializing Token RPC",
		zap.String("prefix", prefix),
	)

	route, _ := crpc.createRoute("admin", prefix)
	route.Use(RequiredAuth())
	route.Handle("AUTHENTICATE", crpc.authenticate)

	return nil
}

func (crpc *CoreRPC) authenticate(ctx *RPCContext) {

	// Prepare response message
	resp := &core.AuthenticateReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req core.AuthenticateRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	claims, err := DecodeToken(req.Token)
	if err != nil {
		resp.Error = &core.Error{
			Code:    44409,
			Message: "Invalid token",
		}
		return
	}

	// Getting token's permissions
	tokenInfo, err := system.tokenRPC.tokenManager.GetToken(claims.TokenID)
	if err != nil {
		resp.Error = &core.Error{
			Code:    44409,
			Message: "Failed to authenticate",
		}

		return
	}

	resp.Durable = claims.TokenID
	resp.Permissions = make([]string, 0)
	for perm, _ := range tokenInfo.Permissions {
		resp.Permissions = append(resp.Permissions, perm)
	}
}
