global
    # maximum number of simultaneous active connections
    maxconn  51200

    ulimit-n 65535

    # Logging to syslog facility local0
    #log      127.0.0.1 local0 err #[err warning info debug]     
    #log /tmp/log    local0
    #log /dev/log    local1 notice

    # for restarts
    pidfile {{.HaproxyRoot}}/haproxy.pid

    # Distribute the health checks with a bit of randomness
    spread-checks 5

    #user  haproxy
    #group haproxy
    nbproc 4
    chroot {{.HaproxyRoot}}
    #daemon

    # Uncomment the statement below to turn on verbose logging
    #debug

    #quiet

# Settings in the defaults section apply to all services (unless you change it,
# this configuration defines one service, called rails).
defaults
    # apply log settings from the global section above to services
    log     global

    option log-separate-errors

    # Proxy incoming traffic as HTTP requests
    mode    http


    # Abort request if client closes its output channel while waiting for the 
    # request. HAProxy documentation has a long explanation for this option.
    option abortonclose

    # Log details about HTTP requests
    option  httplog

    option  dontlognull # 不记录健康检查的日志信息
    no option httpclose

    # If sending a request to one server fails, try to send it to another, 3 times
    # before aborting the request
    retries 3

    # Do not enforce session affinity (i.e., an HTTP session can be served by 
    # any Mongrel, not just the one that started the session
    #option redispatch

    # add X-Forwarded-For header
    option forwardfor

    # Maximum number of simultaneous active connections from an upstream web server
    # per service
    maxconn 15000

    # Keep timeouts at web speed, since this balancer sits in front of everything
    # Backends will force timeout faster if needed.
    timeout client  10m
    timeout connect 10m
    timeout server  10m

    # Amount of time after which a health check is considered to have timed out
    timeout check   5s

    balance roundrobin
    errorfile 400 {{.LogDir}}/400.http
    errorfile 403 {{.LogDir}}/403.http
    errorfile 408 {{.LogDir}}/408.http
    errorfile 500 {{.LogDir}}/500.http
    errorfile 502 {{.LogDir}}/502.http
    errorfile 503 {{.LogDir}}/503.http
    errorfile 504 {{.LogDir}}/504.http

listen admin_stats
    bind 0.0.0.0:8888
    option httplog
    stats refresh 30s
    stats uri /stats
    stats realm Haproxy Manager
    stats auth admin:admin
    #stats hide-version
 


frontend pub
    bind            0.0.0.0:8191
    mode            http
    log             global
    option          httplog
    option          dontlognull
    option          nolinger
    option          http_proxy
    
    timeout client  30s
    default_backend pub-servers

frontend sub
    bind            0.0.0.0:8192
    mode            http
    log             global
    option          httplog
    option          dontlognull
    option          nolinger
    option          http_proxy
    timeout client  30s
    default_backend sub-servers

frontend man
    bind            0.0.0.0:8193
    mode            http
    log             global
    option          httplog
    option          dontlognull
    option          nolinger
    option          http_proxy
    timeout client  30s

    default_backend man-servers


backend pub-servers
    timeout http-keep-alive 5m
    timeout connect 5s
    timeout server  5s
    retries         2
    option          nolinger
    option          http_proxy    
{{range .Pub}}     server {{.Name}} {{.Addr}} check weight 3 minconn 1 maxconn 3 check inter 5s rise 3 fall 3
{{end}}

backend sub-servers
    balance         source
    cookie SRV_ID prefix
{{range .Sub}}     server {{.Name}} {{.Addr}} check weight 3 minconn 1 maxconn 3 check inter 5s rise 3 fall 3
{{end}}

backend man-servers
    option httpchk GET /status HTTP/1.1\r\nHost:www.taobao.net
{{range .Man}}     server {{.Name}} {{.Addr}} check weight 3 minconn 1 maxconn 3 check inter 5s rise 3 fall 3
{{end}}
