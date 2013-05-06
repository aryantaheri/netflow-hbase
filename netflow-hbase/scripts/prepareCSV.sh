#!/bin/bash -x                                                                                                                                                                                          
NF_DAYS_LIST=$(ls -R ./*/*/*/*/ -d -1)
SRC_CNT="/home/aryan/data/src.cnt"
DST_CNT="/home/aryan/data/dst.cnt"
SRC_PORT_CNT="/home/aryan/data/srcport.cnt"
DST_PORT_CNT="/home/aryan/data/dstport.cnt"
FLOW_CNT="/home/aryan/data/flow.cnt"

CSV_DIR="/tmp/netflow/data/csv/"
OTSDB_DIR="/tmp/netflow/data/otsdb/"
OTSDB_AWK="./otsdb.awk"
NFDUMP="/home/aryan/Tools/nfdump-1.6.9/bin/nfdump   -O tstart -A srcip,srcport,dstip,dstport -N -q -R "
NFDUMP_FMT=" -o fmt:%ts,%te,%td,%sa,%da,%sp,%dp,%pr,%flg,%fwd,%stos,%ipkt,%ibyt,%opkt,%obyt,%fl,%in,%out,%sas,%das,%smk,%dmk,%dtos,%dir,%nh,%nhb,%svln,%dvln,%ismc,%odmc,%idmc,%osmc,%ra,%mpls1,%mpls2,%mpls3,%mpls4"


for day in $(echo $NF_DAYS_LIST | sed -e "s/|//g")
do
#    echo "next day to agg: " $day
#    srcno=$(echo $day | cut -d'/' -f 5- | sed "s/\//./g")"src.cnt"
#    srcportno=$(echo $day | cut -d'/' -f 5- | sed "s/\//./g")"srcport.cnt"
#    dstno=$(echo $day | cut -d'/' -f 5- | sed "s/\//./g")"dst.cnt"
#    dstportno=$(echo $day | cut -d'/' -f 5- | sed "s/\//./g")"dstport.cnt"
#    fno=$(echo $day | cut -d'/' -f 5- | sed "s/\//./g")"f.cnt"

#    echo $srcno $(nfdump -R $day -a  -A srcip | wc -l) >> $SRC_CNT
#    echo $srcportno $(nfdump -R $day -a  -A srcip,srcport | wc -l) >> $SRC_PORT_CNT
#    echo $dstno $(nfdump -R $day -a  -A dstip | wc -l) >> $DST_CNT
#    echo $dstportno $(nfdump -R $day -a  -A dstip,dstport | wc -l) >> $DST_PORT_CNT
#    echo $fno $(nfdump -R $day -b | wc -l) >> $FLOW_CNT
    

    # if -R is going to be used a few things must be fixed
    FN=$(echo $day | cut -d'/' -f 2- | sed "s/\//./g")"csv"
    OTSDB_FN=$(echo $day | cut -d'/' -f 2- | sed "s/\//./g")"otsdb"
    CWD=$CSV_DIR$day
    OTSDB_CWD=$OTSDB_DIR$day
    mkdir -p $CWD $OTSDB_CWD
    $NFDUMP $day $NFDUMP_FMT | gzip > $CWD$FN.gz
    awk -F, -f $OTSDB_AWK <(gzip -dc $CWD$FN.gz) | gzip > $OTSDB_CWD$OTSDB_FN.gz

#    gzip $CWD$FN
#    gzip $OTSDB_CWD$OTSDB_FN

done
