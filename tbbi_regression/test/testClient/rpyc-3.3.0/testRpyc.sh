#python2.6 ./client.py 10.101.92.211 "cat /home/admin/alisatasknode/taskinfo/20141222/phoenix/20141221/3476194/14-59-48/3x44tvho4s72d8n8fqh9mc9b/main.sql"  
#exit

ipList="10.101.92.211	10.101.90.203	10.101.90.203"
for ip in $ipList
do
	echo $ip
	#python2.6 ./client.py  ${ip}  "ls /home/admin/alisatasknode/taskinfo/20141222/phoenix/20141221 -l"
	python2.6 ./client.py  ${ip} "grep FAILED /home/admin/alisatasknode/taskinfo/20141222/phoenix/20141221/*/*/*/*.log"
	#python2.6 ./client.py  ${ip} "ls /home/admin/alisatasknode/taskinfo/20141222/phoenix/20141221/3476194/*/*/*.log "
	#python2.6 ./client.py  ${ip}  "ls /home/admin/alisatasknode/taskinfo/20141222/phoenix/20141221/3476194/*/*/* "
	#python2.6 ./client.py  ${ip} "ls /home/admin/alisatasknode/taskinfo/*/phoenix/*/3476194/*/*/*.log "
done
