export PATH=/home/admin/mingchao.xiamc/python279/bin:$PATH
while [ 1 -eq 1 ] 
do
    if [ -f /home/admin/mingchao.xiamc/scatterPydata/restop ]
    then
        exit
    fi
    python2.7 ./ReportTransfer.py
    sleep 3600
done
