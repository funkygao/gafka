global    
    # logging to rsyslog facility local3 [err warning info debug]   
    log 127.0.0.1 local3 info

    maxconn  51200
    ulimit-n 102400    
    pidfile {{.HaproxyRoot}}/haproxy.pid
    #daemon
    #nbproc {{.CpuNum}}
    #user  haproxy
    #group haproxy    
    #chroot {{.HaproxyRoot}}    

defaults
    log global
    mode http    
    backlog 10000
    retries 3
    balance roundrobin
    
    no option httpclose
    option http-server-close # keep-alive on remote client side    
    option http_proxy
    option httplog
    option dontlognull # 不记录健康检查的日志信息
    option abortonclose    
    option redispatch
    option forwardfor
    
    timeout client          10m
    timeout server          10m
    timeout connect         1m
    timeout http-keep-alive 5m
    timeout http-request    15s
    timeout check           5s

    default-server weight 10 minconn 50 maxconn 5000 inter 3m rise 2 fall 3
    
    option log-separate-errors
    errorfile 400 {{.LogDir}}/400.http
    errorfile 500 {{.LogDir}}/500.http
    errorfile 502 {{.LogDir}}/502.http
    errorfile 503 {{.LogDir}}/503.http
    errorfile 504 {{.LogDir}}/504.http

listen admin_stats
    bind 0.0.0.0:8888
    stats refresh 30s
    stats uri /stats
    stats realm Haproxy Manager
    stats auth admin:admin 

frontend pub
    bind 0.0.0.0:8191
    default_backend pub-servers

frontend sub
    bind 0.0.0.0:8192   
    default_backend sub-servers

frontend man
    bind 0.0.0.0:8193
    default_backend man-servers   
    #stick-table type ip size 5000k expire 5m store conn_cur
    #stick on src table man-servers
    #tcp-request connection reject if { src_conn_cur ge 3 }
    #tcp-request connection track-sc1 src  
    
backend pub-servers     
{{range .Pub}}    server {{.Name}} {{.Addr}} check
{{end}}

backend sub-servers
    balance source
    cookie SUB insert
{{range .Sub}}    server {{.Name}} {{.Addr}} cookie {{.Name}} check
{{end}}

backend man-servers
    balance source
    cookie MAN insert indirect nocache  
    option httpchk GET /status HTTP/1.1\r\nHost:kman.ffan.com
{{range .Man}}    server {{.Name}} {{.Addr}} cookie {{.Name}} check
{{end}}
