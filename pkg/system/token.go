package system

import (
	"errors"
	"fmt"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	internal "github.com/BrobridgeOrg/gravity-dispatcher/pkg/system/internal"
	"github.com/BrobridgeOrg/gravity-sdk/core"
	"github.com/BrobridgeOrg/gravity-sdk/token"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type TokenRPC struct {
	RPC

	connector    *connector.Connector
	tokenManager *internal.TokenManager
}

func NewTokenRPC(connector *connector.Connector) *TokenRPC {

	rpc := NewRPC(connector)

	trpc := &TokenRPC{
		RPC:       rpc,
		connector: connector,
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
	route.Handle("LIST", trpc.list)
	route.Handle("CREATE", trpc.create)
	route.Handle("UPDATE", trpc.update)
	route.Handle("DELETE", trpc.delete)
	route.Handle("INFO", trpc.info)

	return nil
}

func (trpc *TokenRPC) list(msg *nats.Msg) {

	// Prepare response message
	resp := &token.ListTokensReply{}
	defer func() {
		data, _ := json.Marshal(resp)

		// Response
		err := msg.Respond(data)
		if err != nil {
			logger.Error(err.Error())
			return
		}
	}()

	// Parsing request
	var req token.ListTokensRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	// List tokens
	settings, err := trpc.tokenManager.ListTokens()
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	resp.Tokens = settings
}

func (trpc *TokenRPC) create(msg *nats.Msg) {

	// Prepare response message
	resp := &token.CreateTokenReply{}
	defer func() {
		data, _ := json.Marshal(resp)

		// Response
		err := msg.Respond(data)
		if err != nil {
			logger.Error(err.Error())
			return
		}
	}()

	// Parsing request
	var req token.CreateTokenRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	req.Setting.ID = req.TokenID

	// Create a new token
	setting, err := trpc.tokenManager.CreateToken(req.TokenID, req.Setting)
	if err != nil {
		logger.Error(err.Error())

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

	resp.Setting = setting
}

func (trpc *TokenRPC) update(msg *nats.Msg) {

	// Prepare response message
	resp := &token.UpdateTokenReply{}
	defer func() {
		data, _ := json.Marshal(resp)

		// Response
		err := msg.Respond(data)
		if err != nil {
			logger.Error(err.Error())
			return
		}
	}()

	// Parsing request
	var req token.UpdateTokenRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	// Update specific token
	setting, err := trpc.tokenManager.UpdateToken(req.TokenID, req.Setting)
	if err != nil {
		logger.Error(err.Error())

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

func (trpc *TokenRPC) delete(msg *nats.Msg) {

	// Prepare response message
	resp := &token.DeleteTokenReply{}
	defer func() {
		data, _ := json.Marshal(resp)

		// Response
		err := msg.Respond(data)
		if err != nil {
			logger.Error(err.Error())
			return
		}
	}()

	// Parsing request
	var req token.DeleteTokenRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	// Delete specific token
	err = trpc.tokenManager.DeleteToken(req.TokenID)
	if err != nil {
		logger.Error(err.Error())

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

func (trpc *TokenRPC) info(msg *nats.Msg) {

	// Prepare response message
	resp := &token.InfoTokenReply{}
	defer func() {
		data, _ := json.Marshal(resp)

		// Response
		err := msg.Respond(data)
		if err != nil {
			logger.Error(err.Error())
			return
		}
	}()

	// Parsing request
	var req token.InfoTokenRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	// Get information of specific token
	setting, err := trpc.tokenManager.GetToken(req.TokenID)
	if err != nil {
		logger.Error(err.Error())

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
