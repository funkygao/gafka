global
    maxconn  51200
    ulimit-n 65535
    log      127.0.0.1 local0 err #[err warning info debug]     
    pidfile /tmp/haproxy.pid
    nbproc 4
    #chroot /usr/local/haproxy
    #daemon
    #debug
    quiet

defaults
    log     global
    mode    http    # instead of tcp
    option  httplog # 日志类别http日志格式 
    option  dontlognull # 不记录健康检查的日志信息
    no option httpclose
    retries 3 # 3次连续失败就认为服务不可用
    option redispatch # serverId对应的服务器挂掉后,强制定向到其他健康的服务器 
    option forwardfor
    timeout client  5000ms
    timeout connect  50000ms
    timeout server  50000ms
    timeout check 5000ms # 心跳检测超时
    balance roundrobin

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
    timeout connect 5s
    timeout server  5s
    retries         2
    option          nolinger
    option          http_proxy    
{{range .Pub}}     server {{.Name}} {{.Ip}}{{.Port}} check weight 3 minconn 1 maxconn 3 check inter 40000 rise 3 fall 3
{{end}}

backend sub-servers
    balance         source
    cookie SERVERID
{{range .Sub}}     server {{.Name}} {{.Ip}}{{.Port}} check weight 3 minconn 1 maxconn 3 check inter 40000 rise 3 fall 3
{{end}}

backend man-servers
    option httpchk GET /status HTTP/1.1\r\nHost:www.taobao.net
{{range .Man}}     server {{.Name}} {{.Ip}}{{.Port}} check weight 3 minconn 1 maxconn 3 check inter 40000 rise 3 fall 3
{{end}}
