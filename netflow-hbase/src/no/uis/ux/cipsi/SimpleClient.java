package no.uis.ux.cipsi;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class SimpleClient {

    public static void main(String[] arg) throws IOException {
        Configuration config = HBaseConfiguration.create();
//        config.set("hbase.zookeeper.quorum", "rostam.ux.uis.no");
//        config.set("hbase.zookeeper.property.clientPort","2181");
//        config.set("hbase.master", "rostam.ux.uis.no:60000"); 
//        config.set("hbase.rootdir", "hdfs://rostam.ux.uis.no:8020/hbase");
        HTable testTable = new HTable(config, "test");
        
        byte[] family = Bytes.toBytes("cf");
        byte[] qual = Bytes.toBytes("a");

        Scan scan = new Scan();
        scan.addColumn(family, qual);
        ResultScanner rs = testTable.getScanner(scan);
        Result r = null;
        int c = 0;
        while ((r = rs.next()) != null) {
            byte[] valueObj = r.getValue(family, qual);
            String value = new String(valueObj);
            System.out.println(new String(r.getRow()));
            System.out.println(value);
            System.out.println(c++);
        }
        
        testTable.close();
    }
}