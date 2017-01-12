global    
    # logging to rsyslog facility local3 [err warning info debug]   
    log 127.0.0.1 local1 notice
    log 127.0.0.1 local3 warning
    stats bind-process {{.CpuNum}}
    stats socket /tmp/haproxy.sock mode 0600 level admin

    maxconn  512
    ulimit-n 1024
    pidfile {{.HaproxyRoot}}/haproxy.pid
    daemon
    nbproc {{.CpuNum}}
    #cpu-map TODO
    spread-checks 5
    #user  haproxy
    #group haproxy
    #chroot {{.HaproxyRoot}}

defaults
    log global
    mode http # [tcp|http|health]
    backlog 10000
    # if "retries" and "option redispatch" are setted, according to haproxy's behavior, 
    # the last retries will trigger redispatch, redispatch will recalcuate the target 
    # server according to balance algorithm. However, balance source algorithm will get
    # the same(original) server if this server is not marked as "DOWN" by health check,
    # and subsequently, the redispatch procedure will be failed.
    # In order to make a successful redispatch, we need to make sure the original server
    # has been marked as "DOWN" before we begin to redispatch. Our health check config is
    # like "inter 5s rise 200000000 fall 3", so the sufficient time interval for distinguish 
    # the server as "DOWN" by health check will be 15s 
    # (5*3,   |---5s---|---5s---|---5s---|)
    # Whole Retry time interval should larger than 15s, so that when try to redispatch, 
    # the server has been "DOWN", and balance source algorithm will recalculate a new server 
    # for redipatch. A single retry interval between two retry is min(1s, connection.timeout), 
    # "connection.timeout" in our config is also 1s, so a single retry interval is 1s
    # To satisfy the condition that whole retry time interval > longest time interval for 
    # distinguish "DOWN" server, we set retry count to 20, so that 20*1s > 15s
    retries 20
    maxconn 15000
    balance roundrobin
    errorfile 503 {{.HaproxyRoot}}/conf/503.http
    
    no option httpclose
    option log-separate-errors
    option httplog
    option dontlognull  # 不记录健康检查的日志信息
    option abortonclose # 当服务器负载很高的时候，自动结束掉当前队列处理比较久的链接
    option redispatch   # 当服务器组中的某台设备故障后，自动将请求重定向到组内其他主机
    {{if .ForwardFor}}
    option forwardfor   # X-Forwarded-For: remote client ip
    {{end}}
    
    timeout client          10m  # 客户端侧最大非活动时间
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
    #cookie PUB insert indirect # indirect means not sending cookie to backend
    #set rise to 200000000 to disable kateway recover by haproxy, we use ehaproxy to recover kateway
{{range .Pub}}
    server {{.Name}} {{.Addr}} weight {{.Cpu}} check inter 5s rise 200000000 fall 3
{{end}}

listen sub
    bind 0.0.0.0:{{.SubPort}}
    balance source
    #balance source # uri
    #compression algo gzip
    #compression type text/html text/plain application/json
    #cookie SUB insert indirect
{{range .Sub}}
    server {{.Name}} {{.Addr}} weight {{.Cpu}} check inter 5s rise 200000000 fall 3
{{end}}

listen man
    bind 0.0.0.0:{{.ManPort}}
{{range .Man}}
    server {{.Name}} {{.Addr}} weight {{.Cpu}} check inter 5s rise 200000000 fall 3
{{end}}
