{   

#    print $0
    split($1, d, "-")
    split(d[3], t, ":")
    ts = mktime(d[1] " " d[2] " " t[1] " " t[2] " " t[3])

    gsub(/^[ \t]+|[ \t]+$/, "", $4)
#    print "sa="$4

    gsub(/^[ \t]+|[ \t]+$/, "", $6)
#    print "sp="$6

    gsub(/^[ \t]+|[ \t]+$/, "", $5)
#    print "da="$5

    gsub(/^[ \t]+|[ \t]+$/, "", $7)
#    print "dp="$7

    gsub(/^[ \t]+|[ \t]+$/, "", $12)
#    print "ipkt="$12

    gsub(/^[ \t]+|[ \t]+$/, "", $13)
#    print "ibyt="$13

    gsub(/^[ \t]+|[ \t]+$/, "", $14)
#    print "opkt="$14

    gsub(/^[ \t]+|[ \t]+$/, "", $15)
#    print "obyt="$15

    gsub(/^[ \t]+|[ \t]+$/, "", $16)
#    print "flow="$16

#    print ts

# Date first seen         Date last seen           Duration      Src IP Addr      Dst IP Addr Src Pt Dst Pt Proto  Flags Fwd STos   In Pkt  In Byte  Out Pkt Out Byte  Flows Input Output Src AS Dst AS SMask DMask\ DTos Dir  Next-hop IP  BGP next-hop IP SVlan DVlan   In src MAC Addr  Out dst MAC Addr   In dst MAC Addr  Out src MAC Addr        Router IP  MPLS lbl 1   MPLS lbl 2   MPLS lbl 3   MPLS lbl 4

#2013-05-03 11:58:42.041,2013-05-03 12:02:41.191, 239.150, 190.119.204.19, 190.118.238.66, 55515, 22,0 ,......, 0, 0, 246000,13320000, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, I, 0.0.0.0, 0.0.0.0, 0, 0,00:00:00:00:00:00,00\ :00:00:00:00:00,00:00:00:00:00:00,00:00:00:00:00:00, 0.0.0.0, 0-0-0, 0-0-0, 0-0-0, 0-0-0

#    proc.stat.cpu 1297574486 54.2 host=foo type=user

 print "no.uis.ux.netflow.ipkt", ts, $12, "sa="$4, "sp="$6, "da="$5, "dp="$7
 print "no.uis.ux.netflow.ibyt", ts, $13, "sa="$4, "sp="$6, "da="$5, "dp="$7
 print "no.uis.ux.netflow.opkt", ts, $14, "sa="$4, "sp="$6, "da="$5, "dp="$7
 print "no.uis.ux.netflow.obyt", ts, $15, "sa="$4, "sp="$6, "da="$5, "dp="$7
 print "no.uis.ux.netflow.flow", ts, $16, "sa="$4, "sp="$6, "da="$5, "dp="$7

}
