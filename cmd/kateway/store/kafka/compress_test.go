package kafka

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/golang/snappy"
)

var src = []byte(`{"message":"2016/06/13 13:49:23 [notice] 143404#0: *10822304701 [lua] gateway.lua:155: log(): [GatewayMonV2] [200], [438], [200, 0.010999917984009, 0.0099999904632568, 1465796963.172, 43], [1465796963.171, 10.209.37.62, -, 775], [-, 10.209.240.142-1465796963.171-143404-957, 4.2.1], [true, -, -, -, -, -, puid=6A2DCC093DC74C55BC8B1953D16DCF47;gw_uid=15000000070284044;SHARE_STRING_ADID=GP1463643039404000000;CITY_ID=110100;up=bup;gw_puid=6A2DCC093DC74C55BC8B1953D16DCF47;sid=ebc6dfc07469ed5635cd2f5e3e75f789;psid=694488ac96648442d21ce9683d156bdd;uid=15000000070284044;gw_up=bup;PHPSESSID=deleted;uniqkey2=RZhczLgfkoQJoTrJFRDMSZ6tWoe3/QVU2IJb8eT4DgCjg9sKlB6UFGQ28JfBUyzzmK7d4I8KAlNbEgeS9XYdwWnsXgdTxpyBCBwNfwRjJCigCTZ5cNw9j3XSiwZUUGJjQZAgyD0yGfHOHNxI6wHsYTiAddaSWBaKqXmQg7w9u/r1HROFBY8aVq/e2bng+9MfgNLw;SESSIONID=deleted;g_adid=deleted;, -], [{}], [-, -, 10.209.37.62, 10.209.37.62], [-], [-], [-, -, -, -, -, -, -, -, -, -, -], [-End-] while sending to client, client: 10.209.37.62, server: localhost, request: \"GET /pay/v2/bankCards?memberId=15000000070284044&puid=6A2DCC093DC74C55BC8B1953D16DCF47&__trace_id=10.209.230.193-1465796963.164-141342-1271&__uni_source=4.2.1 HTTP/1.1\", host: \"api.ffan.com\"","@version":"1","@timestamp":"2016-06-13T05:49:23.185Z","type":"error_log","host":"CDM3E04-209240142","path":"/var/wd/gateway/nginx/logs/error.log"}`)

func TestSnappyCompression(t *testing.T) {
	r := snappy.Encode(nil, src)
	t.Logf("src: %d, compressed: %d", len(src), len(r))
}

func TestGzipCompression(t *testing.T) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write(src)
	w.Close()
	t.Logf("src: %d, compressed: %d", len(src), len(buf.Bytes()))
}
