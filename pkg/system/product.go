package system

import (
	"errors"
	"fmt"

	"github.com/BrobridgeOrg/gravity-dispatcher/pkg/connector"
	internal "github.com/BrobridgeOrg/gravity-dispatcher/pkg/system/internal"
	"github.com/BrobridgeOrg/gravity-sdk/core"
	"github.com/BrobridgeOrg/gravity-sdk/product"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

const (
	ProductAPI = "$GVT.%s.API.PRODUCT"
)

type ProductRPC struct {
	RPC

	connector      *connector.Connector
	productManager *internal.ProductManager
}

func NewProductRPC(connector *connector.Connector) *ProductRPC {

	rpc := NewRPC(connector)

	prpc := &ProductRPC{
		RPC:       rpc,
		connector: connector,
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
	prefix := fmt.Sprintf(ProductAPI, prpc.connector.GetDomain())

	logger.Info("Initializing Product RPC",
		zap.String("prefix", prefix),
	)

	route, _ := prpc.createRoute("admin", prefix)
	route.Handle("LIST", prpc.list)
	route.Handle("CREATE", prpc.create)
	route.Handle("UPDATE", prpc.update)
	route.Handle("DELETE", prpc.delete)
	route.Handle("INFO", prpc.info)
	route.Handle("PURGE", prpc.purge)

	return nil
}

func (prpc *ProductRPC) list(msg *nats.Msg) {

	// Prepare response message
	resp := &product.ListProductsReply{}
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
	var req product.ListProductsRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	// List products
	settings, err := prpc.productManager.ListProducts()
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	resp.Products = settings
}

func (prpc *ProductRPC) create(msg *nats.Msg) {

	// Prepare response message
	resp := &product.CreateProductReply{}
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
	var req product.CreateProductRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	// Create a new product
	setting, err := prpc.productManager.CreateProduct(req.Setting)
	if err != nil {
		logger.Error(err.Error())

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

func (prpc *ProductRPC) update(msg *nats.Msg) {

	// Prepare response message
	resp := &product.UpdateProductReply{}
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
	var req product.UpdateProductRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	// Update specific product
	setting, err := prpc.productManager.UpdateProduct(req.Name, req.Setting)
	if err != nil {
		logger.Error(err.Error())

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

func (prpc *ProductRPC) delete(msg *nats.Msg) {

	// Prepare response message
	resp := &product.DeleteProductReply{}
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
	var req product.DeleteProductRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	// Delete specific product
	err = prpc.productManager.DeleteProduct(req.Name)
	if err != nil {
		logger.Error(err.Error())

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

func (prpc *ProductRPC) info(msg *nats.Msg) {

	// Prepare response message
	resp := &product.InfoProductReply{}
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
	var req product.InfoProductRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	// Get information of specific product
	setting, err := prpc.productManager.GetProduct(req.Name)
	if err != nil {
		logger.Error(err.Error())

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

func (prpc *ProductRPC) purge(msg *nats.Msg) {

	// Prepare response message
	resp := &product.PurgeProductReply{}
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
	var req product.PurgeProductRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		logger.Error(err.Error())
		resp.Error = InternalServerErr()
		return
	}

	// Purge specific product
	err = prpc.productManager.PurgeProduct(req.Name)
	if err != nil {
		logger.Error(err.Error())

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
