function checkjobmode()
{
    local resultlog=$1
    local expmode=$2
    local realmode=`cat $1 | grep "$expmode" | awk -F':' '{print $2}'| sed 's/^ //g' | sed 's/$//g'`
    echo "expect mode is $expmode"
    echo "real mode is $realmode"
    if [ "$realmode" == "$expmode" ]
    then
        echo "##########################################"
        echo "PASS $resultlog pass"
        echo "##########################################"
        return 0
    else
        echo "##########################################"
        echo "FAIL! $resultlog fail"
        echo "##########################################"
        return 1
    fi
}

function waitcmd()
{
    local cmd="$1"
    local expmode=$2
    $cmd &
    cmdpid=$!
    wtime=180
    counter=0
    while [ $counter -lt $wtime ]
    do
        sleep 1 
        counter=`expr $counter + 1`
        pnum=`ps aux|grep ${cmdpid}|grep -vc grep`
        if [ $pnum -eq 0 ]
        then
            echo "$cmd finished!"
            checkjobmode smoke/job.log ${expmode}
            return $?
        fi
    done 
    kill -9 $cmdpid
    echo "$cmd timeout"
}
waitcmd "./alifin_regression/check.sh" "service job, nvm"
