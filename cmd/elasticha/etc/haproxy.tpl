global
    maxconn 4096

defaults
    log global
    mode    http
    option  httplog
    option  dontlognull
    retries 3
    redispatch
    maxconn 2000
    contimeout  5000
    clitimeout  50000
    srvtimeout  50000

frontend http-in
    bind *:8000
    default_backend http

backend http
{{range .}}     server {{.Name}} {{.Ip}}:{{.Port}} maxconn 32
{{end}}