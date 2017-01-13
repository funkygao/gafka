global    
    # logging to rsyslog facility local3 [err warning info debug]   
    log 127.0.0.1 local1 notice
    log 127.0.0.1 local3 warning
    stats bind-process {{.CpuNum}}
    stats socket /tmp/haproxy.sock mode 0600 level admin

    maxconn  51200
    ulimit-n 102434
    pidfile {{.HaproxyRoot}}/haproxy.pid
    daemon
    nbproc {{.CpuNum}}
    #cpu-map TODO
    spread-checks 5
    user  haproxy
    group haproxy
    #chroot {{.HaproxyRoot}}

defaults
    log global
    mode http # [tcp|http|health]
    backlog 10000
    retries 0
    maxconn 15000
    balance roundrobin
    errorfile 500 {{.HaproxyRoot}}/conf/500.http
    errorfile 502 {{.HaproxyRoot}}/conf/502.http
    errorfile 503 {{.HaproxyRoot}}/conf/503.http
    errorfile 504 {{.HaproxyRoot}}/conf/504.http
    
    no option httpclose
    option log-separate-errors
    option httplog
    option dontlognull  # 不记录健康检查的日志信息
    option abortonclose # 当服务器负载很高的时候，自动结束掉当前队列处理比较久的链接
    option redispatch   # 当服务器组中的某台设备故障后，自动将请求重定向到组内其他主机
    {{if .ForwardFor}}
    option forwardfor   # X-Forwarded-For: remote client ip
    {{end}}
    
    timeout client          1m   # 客户端侧最大非活动时间
    timeout server          10m  # 服务器侧最大非活动时间
    timeout connect         1s  # 连接服务器超时时间
    #timeout tunnel          10m
    timeout http-keep-alive 6m   # ?
    timeout queue           1m   # 一个请求在队列里的超时时间
    timeout check           5s
    #timeout http-request    5s

    default-server minconn 50 maxconn 5000 inter 80s rise 2 fall 3

{{range .Dashboard}}
listen 127.0.0.1:{{.Port}}
    bind 127.0.0.1:{{.Port}}
    bind-process {{.Name}}
    stats uri /stats
{{end}}
    
listen pub
    bind 0.0.0.0:{{.PubPort}}
    balance source
    timeout client 5m
    #cookie PUB insert indirect # indirect means not sending cookie to backend
    acl url_alive path_beg /alive
    use_backend alive if url_alive
{{range .Pub}}
    server {{.Name}} {{.Addr}} weight {{.Cpu}}
{{end}}

listen sub
    bind 0.0.0.0:{{.SubPort}}
    balance source
    timeout client 40s
    #balance source # uri
    #compression algo gzip
    #compression type text/html text/plain application/json
    #cookie SUB insert indirect
    acl url_alive path_beg /alive
    use_backend alive if url_alive
{{range .Sub}}
    server {{.Name}} {{.Addr}} weight {{.Cpu}}
{{end}}

listen man
    bind 0.0.0.0:{{.ManPort}}
    acl url_alive path_beg /alive
    use_backend alive if url_alive
{{range .Man}}
    server {{.Name}} {{.Addr}} weight {{.Cpu}}
{{end}}

backend alive
    server localhost :10894