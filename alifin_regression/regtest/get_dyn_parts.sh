for i in `cat dynamic_part_query.list`; do
    taskid=`echo $i | cut -d. -f1`
    tbname=`cat table_part/$i`
    echo $taskid, $tbname
done