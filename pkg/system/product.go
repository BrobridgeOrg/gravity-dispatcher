package system

import (
	"errors"
	"fmt"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	internal "github.com/BrobridgeOrg/gravity-dispatcher/pkg/system/internal"
	"github.com/BrobridgeOrg/gravity-sdk/v2/core"
	"github.com/BrobridgeOrg/gravity-sdk/v2/product"
	"github.com/BrobridgeOrg/gravity-sdk/v2/token"
	"go.uber.org/zap"
)

type ProductRPC struct {
	RPC

	system         *System
	connector      *connector.Connector
	productManager *internal.ProductManager
}

func NewProductRPC(s *System) *ProductRPC {

	rpc := NewRPC(s.connector)

	prpc := &ProductRPC{
		RPC:       rpc,
		system:    s,
		connector: s.connector,
	}

	return prpc
}

func (prpc *ProductRPC) initialize() error {

	// Initialize product manager
	productManager := internal.NewProductManager(
		prpc.connector.GetClient(),
		prpc.connector.GetDomain(),
	)

	if productManager == nil {
		return errors.New("Failed to create product client")
	}

	prpc.productManager = productManager

	// Initialize RPC handlers
	prefix := fmt.Sprintf(product.ProductAPI, prpc.connector.GetDomain())

	logger.Info("Initializing Product RPC",
		zap.String("prefix", prefix),
	)

	route, _ := prpc.createRoute("admin", prefix)
	route.Use(RequiredAuth())
	route.Handle("LIST", RequiredPermissions("PRODUCT.LIST"), prpc.list)
	route.Handle("CREATE", RequiredPermissions("PRODUCT.CREATE"), prpc.create)
	route.Handle("UPDATE", RequiredPermissions("PRODUCT.UPDATE"), prpc.update)
	route.Handle("DELETE", RequiredPermissions("PRODUCT.DELETE"), prpc.delete)
	route.Handle("INFO", RequiredPermissions("PRODUCT.INFO"), prpc.info)
	route.Handle("PURGE", RequiredPermissions("PRODUCT.PURGE"), prpc.purge)
	route.Handle("PREPARE_SUBSCRIPTION", RequiredPermissions("PRODUCT.SUBSCRIPTION"), prpc.prepareSubscription)

	return nil
}

func (prpc *ProductRPC) list(ctx *RPCContext) {

	// Prepare response message
	resp := &product.ListProductsReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req product.ListProductsRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	// List products
	settings, err := prpc.productManager.ListProducts()
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	products := make([]*product.ProductInfo, 0)
	for _, setting := range settings {

		// Getting product state
		state, err := prpc.productManager.GetProductState(setting)
		if err != nil {
			resp.Error = InternalServerErr()
			return
		}

		p := &product.ProductInfo{}
		p.Setting = setting
		p.State = state

		products = append(products, p)
	}

	resp.Products = products
}

func (prpc *ProductRPC) create(ctx *RPCContext) {

	// Prepare response message
	resp := &product.CreateProductReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req product.CreateProductRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	// Create a new product
	setting, err := prpc.productManager.CreateProduct(req.Setting)
	if err != nil {
		ctx.Res.Error = err

		if err == internal.ErrProductExistsAlready {
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

func (prpc *ProductRPC) update(ctx *RPCContext) {

	// Prepare response message
	resp := &product.UpdateProductReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req product.UpdateProductRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	// Update specific product
	setting, err := prpc.productManager.UpdateProduct(req.Name, req.Setting)
	if err != nil {
		ctx.Res.Error = err

		if err == internal.ErrProductNotFound {
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

func (prpc *ProductRPC) delete(ctx *RPCContext) {

	// Prepare response message
	resp := &product.DeleteProductReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req product.DeleteProductRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	// Delete specific product
	err = prpc.productManager.DeleteProduct(req.Name)
	if err != nil {
		ctx.Res.Error = err

		if err == internal.ErrProductNotFound {
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

func (prpc *ProductRPC) info(ctx *RPCContext) {

	// Prepare response message
	resp := &product.InfoProductReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req product.InfoProductRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	// Get information of specific product
	setting, err := prpc.productManager.GetProduct(req.Name)
	if err != nil {
		ctx.Res.Error = err

		if err == internal.ErrProductNotFound {
			resp.Error = &core.Error{
				Code:    44404,
				Message: err.Error(),
			}
		} else {
			resp.Error = InternalServerErr()
		}

		return
	}

	// Getting product state
	state, err := prpc.productManager.GetProductState(setting)
	if err != nil {
		resp.Error = InternalServerErr()
		return
	}

	resp.Setting = setting
	resp.State = state
}

func (prpc *ProductRPC) purge(ctx *RPCContext) {

	// Prepare response message
	resp := &product.PurgeProductReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req product.PurgeProductRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	// Purge specific product
	err = prpc.productManager.PurgeProduct(req.Name)
	if err != nil {
		ctx.Res.Error = err

		if err == internal.ErrProductNotFound {
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

func (prpc *ProductRPC) prepareSubscription(ctx *RPCContext) {

	// Prepare response message
	resp := &product.PrepareSubscriptionReply{}
	ctx.Res.Data = resp

	// Parsing request
	var req product.PrepareSubscriptionRequest
	err := json.Unmarshal(ctx.Req.Data, &req)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}

	// Getting token information
	if ctx.Req.Header["tokenInfo"] == nil {
		return
	}

	tokenInfo := ctx.Req.Header["tokenInfo"].(*token.TokenSetting)

	//TODO: Check permission

	// Purge specific product
	err = prpc.productManager.PrepareSubscription(req.Product, tokenInfo.ID, 0)
	if err != nil {
		ctx.Res.Error = err
		resp.Error = InternalServerErr()
		return
	}
}
