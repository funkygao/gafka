#!/bin/bash
#chkconfig:2345 20 90
#description:zookeeper
#processname:zookeeper
case $1 in
          start) su root {{.RootPath}}/bin/zkServer.sh start;;
          stop) su root {{.RootPath}}/bin/zkServer.sh stop;;
          status) su root {{.RootPath}}/bin/zkServer.sh status;;
          restart) su root {{.RootPath}}/bin/zkServer.sh restart;;
          *)  echo "require start|stop|status|restart"  ;;
esac
