package system

import (
	"errors"
	"fmt"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	internal "github.com/BrobridgeOrg/gravity-dispatcher/pkg/system/internal"
	"github.com/BrobridgeOrg/gravity-sdk/core"
	"github.com/BrobridgeOrg/gravity-sdk/token"
	"go.uber.org/zap"
)

type TokenRPC struct {
	RPC

	system       *System
	connector    *connector.Connector
	tokenManager *internal.TokenManager
}

func NewTokenRPC(s *System) *TokenRPC {

	rpc := NewRPC(s.connector)

	trpc := &TokenRPC{
		RPC:       rpc,
		system:    s,
		connector: s.connector,
	}

	return trpc
}

func (trpc *TokenRPC) initialize() error {

	// Initialize token manager
	tokenManager := internal.NewTokenManager(
		trpc.connector.GetClient(),
		trpc.connector.GetDomain(),
	)

	if tokenManager == nil {
		return errors.New("Failed to create token client")
	}

	trpc.tokenManager = tokenManager

	// Initialize RPC handlers
	prefix := fmt.Sprintf(token.TokenAPI, trpc.connector.GetDomain())

	logger.Info("Initializing Token RPC",
		zap.String("prefix", prefix),
	)

	route, _ := trpc.createRoute("admin", prefix)
	route.Use(RequiredAuth())
	route.Handle("LIST_AVAILABLE_PERMISSIONS", trpc.getAvailablePermissions)
	route.Handle("LIST", RequiredPermissions("TOKEN.LIST"), trpc.list)
	route.Handle("CREATE", RequiredPermissions("TOKEN.CREATE"), trpc.create)
	route.Handle("UPDATE", RequiredPermissions("TOKEN.UPDATE"), trpc.update)
	route.Handle("DELETE", RequiredPermissions("TOKEN.DELETE"), trpc.delete)
	route.Handle("INFO", RequiredPermissions("TOKEN.INFO"), trpc.info)

	return nil
}

func (trpc *TokenRPC) getAvailablePermissions(ctx *RPCContext) {

	// Prepare response message
	resp := &token.ListAvailablePermissionsReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req token.ListAvailablePermissionsRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	resp.Permissions = availablePermissions
}

func (trpc *TokenRPC) list(ctx *RPCContext) {

	// Prepare response message
	resp := &token.ListTokensReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req token.ListTokensRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	// List tokens
	settings, err := trpc.tokenManager.ListTokens()
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	resp.Tokens = settings
}

func (trpc *TokenRPC) create(ctx *RPCContext) {

	// Prepare response message
	resp := &token.CreateTokenReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req token.CreateTokenRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	req.Setting.ID = req.TokenID

	// Create a new token
	setting, err := trpc.tokenManager.CreateToken(req.TokenID, req.Setting)
	if err != nil {
		ctx.Res.Error = err

		if err == internal.ErrTokenExistsAlready {
			resp.Error = &core.Error{
				Code:    44400,
				Message: err.Error(),
			}
		} else {
			resp.Error = InternalServerErr()
		}

		return
	}

	// Encode token to JWT
	jwtString, err := EncodeToken(trpc.system.sysConfig.secret.Key, req.TokenID)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	resp.Token = jwtString
	resp.Setting = setting
}

func (trpc *TokenRPC) update(ctx *RPCContext) {

	// Prepare response message
	resp := &token.UpdateTokenReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req token.UpdateTokenRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	// Update specific token
	setting, err := trpc.tokenManager.UpdateToken(req.TokenID, req.Setting)
	if err != nil {
		ctx.Res.Error = err

		if err == internal.ErrTokenNotFound {
			resp.Error = &core.Error{
				Code:    44404,
				Message: err.Error(),
			}
		} else {
			resp.Error = InternalServerErr()
		}

		return
	}

	resp.Setting = setting
}

func (trpc *TokenRPC) delete(ctx *RPCContext) {

	// Prepare response message
	resp := &token.DeleteTokenReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req token.DeleteTokenRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	// Delete specific token
	err = trpc.tokenManager.DeleteToken(req.TokenID)
	if err != nil {
		ctx.Res.Error = err

		if err == internal.ErrTokenNotFound {
			resp.Error = &core.Error{
				Code:    44404,
				Message: err.Error(),
			}
		} else {
			resp.Error = InternalServerErr()
		}

		return
	}
}

func (trpc *TokenRPC) info(ctx *RPCContext) {

	// Prepare response message
	resp := &token.InfoTokenReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req token.InfoTokenRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	// Get information of specific token
	setting, err := trpc.tokenManager.GetToken(req.TokenID)
	if err != nil {
		ctx.Res.Error = err

		if err == internal.ErrTokenNotFound {
			resp.Error = &core.Error{
				Code:    44404,
				Message: err.Error(),
			}
		} else {
			resp.Error = InternalServerErr()
		}

		return
	}

	resp.Setting = setting
}
