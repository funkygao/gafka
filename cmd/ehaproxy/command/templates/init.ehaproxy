#!/bin/sh
#
# Startup / shutdown script for the ehaproxy server
#
### BEGIN INIT INFO
# Provides:          ehaproxy
# Required-Start:    $network $local_fs
# Required-Stop:
# Should-Start:      $named
# Should-Stop:
# Default-Start:        2 3 4 5
# Default-Stop:         0 1 6
# Short-Description:    ehaproxy server
# Description:          ehaproxy server

### END INIT INFO

. /etc/init.d/functions

if [ "$(id -u)" != "0" ]; then
    echo "Must run as root"
    exit 1
fi

PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

EHAPROXY_HOME=/var/wd/ehaproxy
PIDFILE=$EHAPROXY_HOME/haproxy.pid
DAEMON=$EHAPROXY_HOME/sbin/ehaproxy

test -f $DAEMON || exit 0
test -f $CONFIGFILE || exit 0

# Otherwise, RHEL (and apparently Fedora) tries to "help" and set soft
# limit of core file size to 0 for daemons. It's done as part of
# daemon shell function shortly after changing user. See MB-6601
DAEMON_COREFILE_LIMIT=unlimited

start() {
    touch $PIDFILE 
    cd $EHAPROXY_HOME
    ulimit -n 40960
    ulimit -c unlimited
    ulimit -l unlimited
    daemon "/usr/sbin/daemonize $DAEMON start"
    errcode=$?
    return $errcode
}

stop() {
    daemon "$DAEMON -kill $PIDFILE"
    killall ehaproxy
    errcode=$?
    return $errcode
}

running() {
    pidofproc -p $PIDFILE $DAEMON >/dev/null
    errcode=$?
    return $errcode
}

case $1 in
    start)
        if running ; then
            warning && echo "ehaproxy is already started"
            exit 0
        fi
        echo -n $"Starting ehaproxy"
        start
        echo
        ;;
    stop)
        echo -n $"Stopping ehaproxy"
        stop
        echo
        ;;
    restart)
        echo -n $"Stopping ehaproxy"
        stop
        echo
        echo -n $"Starting ehaproxy"
        start
        echo
        ;;
    status)
        if running ; then
            echo "ehaproxy is running"
            exit 0
        else
            echo "ehaproxy is not running"
            exit 3
        fi
        ;;
    *)
        echo "Usage: /etc/init.d/ehaproxy {start|stop|restart|status}" >&2
        exit 3
esac
