pidstat -d -p pid
smartctl -h
badblocks -nsv /dev/<device>
/proc/diskstats

# disable Transparent Huge Pages (THP)
if test -f /sys/kernel/mm/redhat_transparent_hugepage/defrag; then echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag fi

# reject connections for port 9999 above 100 from one source IP
iptables -A INPUT -p tcp --syn --dport 9999 -m connlimit --connlimit-above 100 --connlimit-mask 32 -j REJECT --reject-with tcp-reset 
