package gateway

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"
)

func mockHttpRequest() (*http.Request, error) {
	httpReqRaw := strings.TrimSpace(fmt.Sprintf(`
POST /topics/foobar/v1 HTTP/1.1
Host: localhost:9191
User-Agent: Go-http-client/1.1
Content-Length: %d
Content-Type: application/x-www-form-urlencoded
Appid: app1
Pubkey: mypubkey
X-Forwarded-For: 1.1.1.12
Accept-Encoding: gzip`, 100)) + "\r\n\r\n"
	r, err := http.ReadRequest(bufio.NewReader(strings.NewReader(httpReqRaw)))
	if err != nil {
		return nil, err
	}

	return r, nil
}

func mockHttpRequestWithNestedForwardFor() (*http.Request, error) {
	httpReqRaw := strings.TrimSpace(fmt.Sprintf(`
POST /topics/foobar/v1 HTTP/1.1
Host: localhost:9191
User-Agent: Go-http-client/1.1
Content-Length: %d
Content-Type: application/x-www-form-urlencoded
Appid: app1
Pubkey: mypubkey
X-Forwarded-For: 1.1.1.12, 1.2.2.2
Accept-Encoding: gzip`, 100)) + "\r\n\r\n"
	r, err := http.ReadRequest(bufio.NewReader(strings.NewReader(httpReqRaw)))
	if err != nil {
		return nil, err
	}

	return r, nil
}
