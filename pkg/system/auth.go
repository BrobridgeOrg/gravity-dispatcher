package system

import (
	"errors"

	"github.com/golang-jwt/jwt"
)

type Permissions map[string]string

var availablePermissions = Permissions{

	// Administrator
	"ADMIN": "Administrator",

	// Product
	"PRODUCT.LIST":          "List available products",
	"PRODUCT.CREATE":        "Create product",
	"PRODUCT.DELETE":        "Delete specific product",
	"PRODUCT.UPDATE":        "Update specific product",
	"PRODUCT.PURGE":         "Purge specific product",
	"PRODUCT.INFO":          "Get specific product information",
	"PRODUCT.SUBSCRIPTION":  "Subscribe to specific product",
	"PRODUCT.SNAPSHOT.READ": "Read snapshot of specific product",
	"PRODUCT.ACL":           "Update ACL of specific product",

	// Token
	"TOKEN.LIST":   "List available tokens",
	"TOKEN.CREATE": "Create token",
	"TOKEN.DELETE": "Delete specific token",
	"TOKEN.UPDATE": "Update specific token",
	"TOKEN.INFO":   "Get specific token information",
}

type Claims struct {
	jwt.StandardClaims
	TokenID string
}

func EncodeToken(secretKey string, tokenID string) (string, error) {

	t := jwt.New(jwt.GetSigningMethod("ES512"))

	t.Claims = &Claims{
		jwt.StandardClaims{
			//ExpiresAt: time.Now().Add(time.Minute * 1).Unix(),
		},
		tokenID,
	}

	return t.SignedString(secretKey)
}

func RequiredAuth() RPCHandler {
	return func(ctx *RPCContext) {

		// Getting token from header
		token, ok := ctx.Req.Header["Authorization"]
		if !ok {
			return
		}

		// Decode token
		tokenClaims, err := jwt.ParseWithClaims(token.(string), &Claims{}, func(token *jwt.Token) (i interface{}, err error) {
			return system.sysConfig.secret.Key, nil
		})
		if err != nil {
			ctx.Res.Error = err
			reply := &ErrorRPCState{}
			reply.Error = ForbiddenErr()
			ctx.Res.Data = reply
			return
		}

		ctx.Req.Header["token"] = tokenClaims
	}
}

func RequiredPermissions(permissions ...string) RPCHandler {

	return func(ctx *RPCContext) {

		v, ok := ctx.Req.Header["token"]
		if !ok {
			ctx.Res.Error = errors.New("Forbidden")
			reply := &ErrorRPCState{}
			reply.Error = ForbiddenErr()
			ctx.Res.Data = reply
			return
		}

		claims := v.(*Claims)

		// Getting token's permissions
		tokenInfo, err := system.tokenRPC.tokenManager.GetToken(claims.TokenID)
		if err != nil {
			ctx.Res.Error = err

			reply := &ErrorRPCState{}
			reply.Error = ForbiddenErr()
			ctx.Res.Data = reply

			return
		}

		// Pass directly for administrator permission
		if _, ok := tokenInfo.Permissions["ADMIN"]; ok {
			return
		}

		// Check whether token contains permission
		for _, perm := range permissions {
			if _, ok := tokenInfo.Permissions[perm]; ok {
				return
			}
		}
	}
}
