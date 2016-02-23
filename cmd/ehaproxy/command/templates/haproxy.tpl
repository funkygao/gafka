global    
    # logging to rsyslog facility local3 [err warning info debug]   
    log 127.0.0.1 local0
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
    
    no option httpclose
    option log-separate-errors
    option httplog
    option dontlognull  # 不记录健康检查的日志信息
    option abortonclose # 当服务器负载很高的时候，自动结束掉当前队列处理比较久的链接
    option redispatch   # 当服务器组中的某台设备故障后，自动将请求重定向到组内其他主机
    #option forwardfor   # X-Forwarded-For: remote client ip
    
    timeout client          10m  # 客户端侧最大非活动时间
    timeout server          1m   # 服务器侧最大非活动时间
    timeout connect         10s  # 连接服务器超时时间
    timeout http-keep-alive 6m   # ?
    timeout queue           1m   # 一个请求在队列里的超时时间
    timeout check           5s
    #timeout http-request    5s

    default-server minconn 50 maxconn 5000 inter 80s rise 2 fall 3
            
listen dashboard
    bind 0.0.0.0:10890
    bind-process {{.CpuNum}}
    stats refresh 30s
    stats uri /stats
    stats realm Haproxy Manager
    stats auth eadmin:adminPassOfDashboard1

listen pub
    bind 0.0.0.0:10891
    cookie PUB insert indirect
    #option httpchk GET /alive HTTP/1.1\r\nHost:pub.ffan.com
{{range .Pub}}
    server {{.Name}} {{.Addr}} cookie {{.Name}} weight {{.Cpu}}
{{end}}

listen sub
    bind 0.0.0.0:10892
    #balance uri
    balance source
    #compression algo gzip
    #compression type text/html text/plain application/json
    cookie SUB insert indirect
    #option httpchk GET /alive HTTP/1.1\r\nHost:sub.ffan.com
{{range .Sub}}
    server {{.Name}} {{.Addr}} cookie {{.Name}} weight {{.Cpu}}
{{end}}

listen man
    bind 0.0.0.0:10893
    #option httpchk GET /alive HTTP/1.1\r\nHost:kman.ffan.com
{{range .Man}}
    server {{.Name}} {{.Addr}} weight {{.Cpu}}
{{end}}
