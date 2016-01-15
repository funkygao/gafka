global
    maxconn  10000
    ulimit-n 82000
    log      127.0.0.1 local0
    #daemon

defaults
    log     global
    mode    http
    option  httplog
    option  dontlognull
    retries 3
    redispatch
    contimeout  5000
    clitimeout  50000
    srvtimeout  50000

frontend pub
    bind            0.0.0.0:9191
    mode            http
    log             global
    option          httplog
    option          dontlognull
    option          nolinger
    option          http_proxy
    option          forwardfor
    timeout client  30s
    default_backend pub-servers

 frontend sub
    bind            0.0.0.0:9192
    mode            http
    log             global
    option          httplog
    option          dontlognull
    option          nolinger
    option          http_proxy
    option          forwardfor
    timeout client  30s
    default_backend sub-servers

frontend man
    bind            0.0.0.0:9193
    mode            http
    log             global
    option          httplog
    option          dontlognull
    option          nolinger
    option          http_proxy
    option          forwardfor
    timeout client  30s

    default_backend man-servers


backend pub-servers
    timeout connect 5s
    timeout server  5s
    retries         2
    option          nolinger
    option          http_proxy
    balance         source
{{range .Pub}}     server {{.Name}} {{.Ip}}:{{.Port}}  weight 3 check
{{end}}

backend sub-servers
{{range .Sub}}     server {{.Name}} {{.Ip}}:{{.Port}}  weight 3 check
{{end}}

backend man-servers
{{range .Man}}     server {{.Name}} {{.Ip}}:{{.Port}}  weight 3 check
{{end}}
