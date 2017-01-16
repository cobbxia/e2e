
res2node="./res2node.txt"

dirpath="/home/admin/mingchao.xiamc/pydata/verify/logs/renderSql_tbbi/"
for fname in `ls ${dirpath}`
do
    while read tname
    do
        num=`grep ${tname} ${dirpath}/${fname}|grep -ic "insert"`
        if [ $num -gt 0 ]
        then
            echo "${tname} ${fname}" >> ${res2node}
        fi
    done < ./tbl.txt
done
        
