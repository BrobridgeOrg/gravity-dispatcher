package system

import (
	"errors"
	"fmt"

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
	TokenID string `json:"tokenID"`
	jwt.StandardClaims
}

func EncodeToken(tokenID string) (string, error) {

	claims := &Claims{
		tokenID,
		jwt.StandardClaims{
			Issuer: "Gravity",
			//ExpiresAt: time.Now().Add(time.Minute * 1).Unix(),
		},
	}

	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	return t.SignedString([]byte(system.sysConfig.GetEntry("secret").Secret().Key))
}

func DecodeToken(tokenString string) (*Claims, error) {

	// Decode token
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (i interface{}, err error) {
		return []byte(system.sysConfig.GetEntry("secret").Secret().Key), nil
	})

	return token.Claims.(*Claims), err
}

func RequiredAuth() RPCHandler {
	return func(ctx *RPCContext) {
		// Getting token from header
		tokens, ok := ctx.Req.Header["Authorization"]
		if !ok {
			return
		}

		if len(tokens.([]string)) == 0 {
			return
		}

		// Decode token
		claims, err := DecodeToken(tokens.([]string)[0])
		if err != nil {
			ctx.Res.Error = err
			reply := &ErrorRPCState{}
			reply.Error = ForbiddenErr()
			ctx.Res.Data = reply
			return
		}

		ctx.Req.Header["token"] = claims
	}
}

func RequiredPermissions(permissions ...string) RPCHandler {

	return func(ctx *RPCContext) {

		v, ok := ctx.Req.Header["token"]
		if !ok {

			if !system.sysConfig.GetEntry("auth").Auth().Enabled {
				return
			}

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

		fmt.Println(claims.TokenID)

		ctx.Req.Header["tokenInfo"] = tokenInfo

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
