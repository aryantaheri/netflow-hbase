package no.uis.ux.cipsi;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.thirdparty.guava.common.base.CharMatcher;
import org.apache.hadoop.thirdparty.guava.common.base.Functions;
import org.apache.hadoop.thirdparty.guava.common.base.Splitter;
import org.apache.hadoop.thirdparty.guava.common.collect.ImmutableSet;
import org.apache.hadoop.thirdparty.guava.common.collect.ImmutableSortedMap;
import org.apache.hadoop.thirdparty.guava.common.collect.Ordering;
import org.apache.http.util.ByteArrayBuffer;

import com.google.common.collect.Lists;

public class Play {

	private static void date() {
		// TODO Auto-generated method stub
		String t = "2013-03-21 10:29:33.730";
//		String t = "2001.07.04 AD at 12:08:56 PDT";
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//		df.setTimeZone(TimeZone.getDefault());
//		df.setLenient(false);
	    Date date;
		try {
			date = df.parse(t);
			
			long epoch = date.getTime();
			System.out.println(TimeZone.getDefault());
			System.out.println(date.getHours());
			System.out.println(date.getMinutes());
			System.out.println(date.getSeconds());
			System.out.println();
			System.out.println(date.getMonth());
			System.out.println(date.getDate());
			System.out.println(date.getYear());
			System.out.println(epoch);
			System.out.println(new Date(epoch));
			System.out.println(Bytes.toBytes(epoch).length);
			for (int i = 0; i < Bytes.toBytes(epoch).length; i++) {
				System.out.println(Bytes.toBytes(epoch)[i]);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static void stringInt() {
		// TODO Auto-generated method stub
		String s = "";
		System.out.println(Bytes.toBytes(s).length);
		System.out.println(Bytes.toBytes(Integer.parseInt(s)).length);
		System.out.println(Bytes.toBytes(Short.parseShort(s)).length);
	}
	
	private static void stringIP(){
		try {
			InetAddress address = InetAddress.getByName("::1");
			System.out.println(address.getAddress().length);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
    private static void showStats( ByteBuffer b ) 
    {
    System.out.println( 
                 " bufferPosition: " +
                 b.position() +
                 " limit: " +
                 b.limit() +
                 " remaining: " +
                 b.remaining() +
                 " capacity: " +
                 b.capacity() );
    }
	private static void byteBuffer(){
		ByteBuffer buffer = ByteBuffer.allocate(4);
		InetAddress address;
		try {
			address = InetAddress.getByName("152.94.1.131");
			buffer.put(address.getAddress());
//			buffer.putChar('b');
//			showStats(buffer);
			byte[] tmp = buffer.array();
			System.out.println(InetAddress.getByAddress(tmp).getHostAddress());
			System.out.println(InetAddress.getByAddress(tmp).getCanonicalHostName());
			System.out.println(InetAddress.getByAddress(tmp).getHostName());
			System.out.println(InetAddress.getByAddress(tmp));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		ArrayUtils.			
//		InetAddress address;
//		try {
//			address = InetAddress.getByName("152.94.0.223");
//			System.out.println(address.getAddress().length);
//			ByteArrayBuffer arrayBuffer = new ByteArrayBuffer(10);
//			arrayBuffer.append(address.getAddress(), 0, address.getAddress().length);
//			arrayBuffer.append(5);
//			System.out.println(arrayBuffer.buffer().length);
//			for (int i = 0; i < arrayBuffer.length(); i++) {
//				System.out.println(InetAddress.getByAddress(Arrays.copyOfRange(arrayBuffer.toByteArray(), 1, 5)));
//			}
//			
//			List<Byte> list = new ArrayList<Byte>();
//			Byte[] tmp = ArrayUtils.toObject("Hello".getBytes());
//			
//			list.addAll(Arrays.asList(ArrayUtils.toObject("Hello".getBytes())));
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
	private static void hashmaps() {
		final HashMap<String, Long> srcDstTraffic = new HashMap<String, Long>();
		srcDstTraffic.put("ip1:ip2", 2l);
		srcDstTraffic.put("ip3:ip3", 4l);
		srcDstTraffic.put("ip3:ip4", 1l);
		srcDstTraffic.put("ip5:ip4", 5l);
		srcDstTraffic.put("ip6:ip4", 2l);
		
		int N = 3;
		
		Set<String> keySet = srcDstTraffic.keySet();
		String[] keyArray = keySet.toArray(new String[keySet.size()]);
		Arrays.sort(keyArray, new Comparator<String>(){
			@Override
		    public int compare(String o1, String o2) {
		      // sort descending
		      return Long.compare(srcDstTraffic.get(o2), srcDstTraffic.get(o1));
		    }
		});
		
		ArrayList<Text> texts = new ArrayList<Text>();
		ArrayList<LongWritable> values = new ArrayList<LongWritable>();
		Text key = new Text();
		LongWritable value = new LongWritable();
		for (int i = 0; i < Math.min(keyArray.length, N); i++) {
			System.out.print(keyArray[i] + " ");
			System.out.println(srcDstTraffic.get(keyArray[i]));
			key.set(keyArray[i]);
			value.set(srcDstTraffic.get(keyArray[i]));
			texts.add(key);
			values.add(value);
			
		}
		for (int i = 0; i < texts.size(); i++) {
			System.out.print(texts.get(i));
			System.out.println(values.get(i));
		}
//		System.out.println((LongWritable) 13l);
//		Ordering valueComparator = Ordering.natural().onResultOf(Functions.forMap(srcDstTraffic));
//		ImmutableSortedMap sortedMap = ImmutableSortedMap.copyOf(srcDstTraffic, valueComparator);
//		for (int i = 0; i < sortedMap.entrySet().size(); i++) {
//			System.out.println(sortedMap.entrySet().toArray()[i]);
//		}
	}
	
	private static void lists(){
		List<String> list1 = new ArrayList<String>();
		list1.add("1");
		list1.add("2");
		list1.add("3");
		list1.add("4");
		list1.add("5");
		list1.add("6");
		list1.add("7");
		for (int i = 0; i < list1.size(); i++) {
			System.out.println(i + ": " + list1.get(i));
		}
//		list1.remove(2);
//		list1.remove(5);
		list1.set(2, null);
		list1.set(5, null);
		System.out.println();
		for (int i = 0; i < list1.size(); i++) {
			System.out.println(i + ": " + list1.get(i));
		}
		
		list1.remove(null);
		list1.remove(null);
		System.out.println();
		for (int i = 0; i < list1.size(); i++) {
			System.out.println(i + ": " + list1.get(i));
		}
		
		
		List<String> list2 = new ArrayList<String>();
		
	}
	
	private static void longop(){
		long l =  9223372036854775807l;
		System.out.println(l);
		System.out.println(Bytes.toBytes(l).length);
		for (int i = 0; i < Bytes.toBytes(l).length; i++) {
			System.out.println(Bytes.toBytes(l)[i]);
		}
		byte[] bl = new byte[8];
		System.arraycopy(bl, 0, Bytes.toBytes(l), 3, 4);
		for (int i = 0; i < bl.length; i++) {
			System.out.println(bl[i]);
		}
		System.out.println(Bytes.toLong(bl));
	}
	private static void byteop() {
		byte dtos = Byte.parseByte("127");
		System.out.println(Byte.toString((new byte[]{dtos})[0]));
	}
	
    /**
     * Get a row
     */
    public static void getOneRecord (Configuration conf, String tableName, byte[] rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey);
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            NetFlowCSVParser.printRowKeyT1(kv.getRow());
            System.out.print(kv.getTimestamp() + " " );
            NetFlowV5Record.decodeCFQValues(new CFQValue(kv.getFamily(), kv.getQualifier(), kv.getValue()));
        }
    }
    
    /**
     * Get a set of rows
     */
    public static void getRecords (Configuration conf, String tableName, byte[] rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Scan s = new Scan();
        System.out.println(bytesToBits(rowKey));
        s.setStartRow(rowKey);
        s.setMaxVersions();

//        ByteBuffer buffer = ByteBuffer.allocate(9);
//        buffer.put(rowKey);
//        buffer.put((byte) -1);
//        System.out.println(bytesToBits(buffer.array()));
//        s.setStopRow(buffer.array());

        
        byte[] rowKeyStop = new byte[rowKey.length];
        System.arraycopy(rowKey, 0, rowKeyStop, 0, rowKey.length - 1);
        rowKeyStop[rowKey.length -1 ] = (byte) (rowKey[rowKey.length -1 ] + 1);
        System.out.println(bytesToBits(rowKeyStop));
        s.setStopRow(rowKeyStop);
        
//        System.out.println(bytesToBits(rowKey));
//        s.setStopRow(Bytes.add(nullb, rowKey));
//        s.setFilter(new InclusiveStopFilter(Bytes.add(new byte[]{0}, rowKey)));
//        s.setStopRow(Bytes.add(Bytes.toBytes((byte)0), rowKey));
//        s.setStopRow(Bytes.add(rowKey, Bytes.toBytes((byte)0)));
//        s.setStopRow(rowKey);
        
        
        int count = 0;
        ResultScanner ss = table.getScanner(s);
        boolean first = true;
        for(Result r:ss){
        	System.out.println("------------------------------------------------");
        	first = true;
	        for(KeyValue kv : r.raw()){
	        	if (first){
	        		System.out.println(NetFlowCSVParser.decodeRowKey(kv.getRow(),1));
//	        		System.out.println(bytesToBits(kv.getRow()));
//		            System.out.print(kv.getTimestamp() + " " );
//		            System.out.println(NetFlowV5Record.decodeCFQValues(new CFQValue(kv.getFamily(), kv.getQualifier(), kv.getValue())));
//		            System.out.println("****************");
		            first = false;
	        	}
	        }
	        System.out.println("------------------------------------------------");
	        count++;
        }
        System.out.println("Returned results: " + count);
        System.out.println("------------------------------------------------");
        ss.close();
    }
    /**
     * Scan (or list) a table
     * @throws ParseException 
     */
    public static void getAllRecord (Configuration conf, String tableName) throws ParseException {
        try{
             HTable table = new HTable(conf, tableName);
             Scan scan = new Scan();
             scan.addColumn("d".getBytes(), Utils.getColumnQualifier("In Byte"));
     		scan.addColumn("d".getBytes(), Utils.getColumnQualifier("Out Byte"));
     		//69.198.15.10, 190.119.204.154, 51357,    80,0
     		// 161.220.168.35,     79.23.96.85,    80, 45097
     		String src = "161.220.168.35"; 
     		String srcPort = "80";
     		String dst = "79.23.96.85";
     		String dstPort = "45097";//"45097"
     		
     		// check this one:::: the order of time stamps should be changed due to their reverse order in hbase.
     		String ts2 = "2012-10-28 00:00:00";
     		String ts1 = "2013-05-30 00:00:00";
     		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	    long startTime = dateFormat.parse(ts1).getTime();
    	    long endTime = dateFormat.parse(ts2).getTime();
     		// Rowkey filters
     		byte[] rowkeyStart = NetFlowCSVParser.prepareRowKeyT1Simple(src, srcPort, dst, dstPort, startTime);
     		byte[] rowkeyEnd = NetFlowCSVParser.prepareRowKeyT1Simple(src, srcPort, dst, dstPort, endTime);
     		
     		
     		scan.setStartRow(rowkeyStart);
     		scan.setStopRow(rowkeyEnd);
//     		scan.setStopRow(rowKeyStop);
     		
             ResultScanner ss = table.getScanner(scan);
             int count = 0;
             boolean first = true;
             for(Result r:ss){
            	 first = true;
            	 for(KeyValue kv : r.raw()){
                     if (first){
                    	 System.out.println(NetFlowCSVParser.decodeRowKeyT1(kv.getRow()));
                    	 System.out.print(bytesToBits(kv.getRow()));
                    	 System.out.println();
//	                     System.out.println("KeyValue TS: " + new Date(kv.getTimestamp()));
                    	 first = false;
                     }
//                     System.out.println(NetFlowV5Record.decodeCFQValues(new CFQValue(kv.getFamily(), kv.getQualifier(), kv.getValue())));
                 }
                 count++;
             }
             System.out.println("Returned results: " + count);
        } catch (IOException e){
            e.printStackTrace();
        }
    }
	
    private static void serviceDiscovery(Configuration conf, String servicePort) throws IOException{
        HTable table = new HTable(conf, "NST5");
        Scan scan = new Scan();
		byte[] rowkeyStart = Bytes.toBytes(Integer.parseInt(servicePort));
		byte[] rowkeyEnd = Bytes.toBytes(Integer.parseInt(servicePort) + 1);
 		scan.setStartRow(rowkeyStart);
 		scan.setStopRow(rowkeyEnd);
        ResultScanner ss = table.getScanner(scan);
        int count = 0;
        boolean first = true;
        for(Result r:ss){
       	 first = true;
       	 for(KeyValue kv : r.raw()){
                if (first){
               	 System.out.println(NetFlowCSVParser.decodeRowKey(kv.getRow(), 5));
               	 System.out.print(bytesToBits(kv.getRow()));
               	 System.out.println();
//                    System.out.println("KeyValue TS: " + new Date(kv.getTimestamp()));
               	 first = false;
                }
//                System.out.println(NetFlowV5Record.decodeCFQValues(new CFQValue(kv.getFamily(), kv.getQualifier(), kv.getValue())));
            }
            count++;
        }
        System.out.println("Returned results: " + count);

 		
    }
    
    private static void putPortPlay(Configuration conf){
    	try {
			HTable table = new HTable(conf, "PortTable");
			for (int i = 0; i < 65536; i++) {			
				Put p = new Put(Bytes.toBytes(i));
				p.add(Bytes.toBytes("d"), Bytes.toBytes("q"), Bytes.toBytes("v"));
				table.put(p);
			}
			table.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    private static void putIPPlay(Configuration conf){
    	try {
			HTable table = new HTable(conf, "IPTable");
			InetAddress address;
			for (int i = 0; i < 65536; i++) {			
				Put p = new Put(InetAddress.getByName((i % 255)+".1.1.1").getAddress());
				p.add(Bytes.toBytes("d"), Bytes.toBytes("q"), Bytes.toBytes("v"));
				table.put(p);
			}
			table.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    private static void hbaseClient(){
		Configuration conf = HBaseConfiguration.create();
		try {
			InetAddress address = InetAddress.getByName("191.220.195.154");
//			byte[] rowkey = Bytes.add(address.getAddress(), Bytes.toBytes(Integer.parseInt("119")));
			ByteBuffer buffer = ByteBuffer.allocate(8);
			buffer.put(address.getAddress());
			buffer.put(Bytes.toBytes(Integer.parseInt("80")));
			byte[] rowkey = buffer.array();
//			byte[] rowkey = address.getAddress();
//			byte[] rowkey = Bytes.head(address.getAddress(), 1);
//			byte[] rowkey = Bytes.toBytes(1362308365166l);
//			getRecords(conf, "T1", rowkey);
//			putIPPlay(conf);
//			putPortPlay(conf);
//			getAllRecord(conf, "T1");
			serviceDiscovery(conf, "22");
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	public static void intOp(){
		System.out.println(bytesToBits(new byte[]{-127}));
		System.out.println((byte) -1);
	}
	
	public static String bytesToBits(byte[] b){
		String bits = "";
		for (int j = 0; j < b.length; j++) {
			for (int i = 7; i > -1; i--) {
				if ((b[j] & (1 << i)) != 0) {
					bits += "1";
				}else {
					bits += "0";
				}
//			System.out.println(bits + " " + i);
			}
			bits += " ";
			
		}
		return bits;
	}
	private static void splitter(){
//		String record = "Table1, Source IP: 191.220.195.154, Source Port: 80, Destination IP: 0.3.125.250, Destination Port: 1344, FirstSeen: Mon Mar 11 02:11:22 CET 2013, KeySize: 24";
		Iterator<String> tokens = Splitter.onPattern("\\s+").trimResults().split(new String("107.150.145.204:192.239.62.5  		483919432000")).iterator();
		String ipPair = tokens.next();
		Long traffic = Long.parseLong(tokens.next());
		System.out.println(ipPair + " " + traffic);
//		Splitter.on(",").trimResults().
	}
	public static void main(String[] args) {
//		ArrayList<String> columnStrings = Lists.newArrayList(
//		        Splitter.on(',').trimResults().split("ab ,b,c"));
//		System.out.println(columnStrings);
//		stringInt();
//		stringIP();
//		byteBuffer();
//		lists();
//		byteop();
		hbaseClient();
//		extractFieldValue();
//		hashmaps();
//		splitter();
//		intOp();
//		System.out.println(byteToBits((byte)10));
	}
}
