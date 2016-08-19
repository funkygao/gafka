package gateway

import (
	"errors"

	"github.com/dgrijalva/jwt-go"
	"github.com/funkygao/golib/hack"
)

var errInvalidToken = errors.New("Invalid token")

func jwtToken(appid, secret string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"appid": appid,
	})

	// Sign and get the complete encoded token as a string using the secret
	tokenString, err := token.SignedString(hack.Byte(secret))
	if err != nil {
		return "", err
	}

	return tokenString, nil
}

func tokenDecode(tokenString string) (appid string, err error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		return "", nil
	})

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		return claims["appid"].(string), nil
	}

	return "", errInvalidToken
}
