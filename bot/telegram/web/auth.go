package web

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strings"

	"github.com/difof/goul/errors"
)

func ValidateHash(initData string, token string) (ok bool, err error) {
	var values url.Values
	values, err = url.ParseQuery(initData)
	if err != nil {
		err = errors.Newi(err, "Error parsing query string")
		return
	}

	queryId := values.Get("query_id")
	user := values.Get("user")
	authDate := values.Get("auth_date")
	hash := values.Get("hash")

	sb := strings.Builder{}
	sb.WriteString("auth_date=")
	sb.WriteString(authDate)
	sb.WriteString("\n")
	sb.WriteString("query_id=")
	sb.WriteString(queryId)
	sb.WriteString("\n")
	sb.WriteString("user=")
	sb.WriteString(user)
	dataCheckString := sb.String()

	secretKey := hmac.New(sha256.New, []byte("WebAppData"))
	secretKey.Write([]byte(token))

	dataHash := hmac.New(sha256.New, secretKey.Sum(nil))
	dataHash.Write([]byte(dataCheckString))

	resultHash := hex.EncodeToString(dataHash.Sum(nil))
	ok = resultHash == hash

	return
}
