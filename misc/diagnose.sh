pidstat -d -p pid
smartctl -h
badblocks -nsv /dev/<device>
/proc/diskstats

# disable Transparent Huge Pages (THP)
if test -f /sys/kernel/mm/redhat_transparent_hugepage/defrag; then echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag fi

