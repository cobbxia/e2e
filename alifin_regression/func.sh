function checkjobmode()
{
    local resultlog="$1"
    local expmode="$2"
    echo "${expmode}"
    #local realmode=`cat $1 | grep "$expmode" | awk -F':' '{print $2}'| sed 's/^ //g' | sed 's/$//g'`
    sleep 1
    local realmodenum=`cat $1 |grep -v "grep" |grep -ic "$expmode"`
    echo "expect mode is $expmode"
    echo "real mode number is $realmodenum"
    if [ $realmodenum -gt 0 ]
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
    local expmode="$2"
    local projectname="$3"
    $cmd "${expmode}" "${projectname}" &
    local cmdpid=" "$!" "
    local wtime=1200
    local counter=0
    while [ $counter -lt $wtime ]
    do
        sleep 1 
        counter=`expr $counter + 1`
        local pnum=`ps aux|grep ${cmdpid}|grep -vc grep`
        if [ $pnum -eq 0 ]
        then
            echo "$cmd finished!"
            echo "${expmode}"
            checkjobmode smoke/job.log "${expmode}"
            return $?
        fi
    done 
    kill -9 $cmdpid
    echo "$cmd timeout"
    return 1
}
