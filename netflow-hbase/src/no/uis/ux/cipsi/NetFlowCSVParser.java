package no.uis.ux.cipsi;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.http.conn.util.InetAddressUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class NetFlowCSVParser {

//	private final byte[][] families;
//    private final byte[][] qualifiers;
	private final List<byte[]> families;
    private final List<byte[]> qualifiers;

    public static final String COLUMN_FAMILY_NAME = "d";
    
    //TODO: Use enum :)
    private static final String SRC_ADDRESS = "Src IP Addr";
    private static final String SRC_PORT = "Src Pt";
    private static final String DST_ADDRESS = "Dst IP Addr";
    private static final String DST_PORT = "Dst Pt";
    private static final String FIRST_SEEN = "Date flow start";
//    private static String FIRST_SEEN_MS = "Date flow start";
    private static String LAST_SEEN = "Date flow end";
    private static String PROTOCOL = "Proto";
    
    // [sa][sp][da][dp][ts][ts-ms]
    // [0] [1] [2] [3] [4] [5]
    private static int ROWKEY_T1_SIZE = 6;
    private int[] rowkeyT1idxs = new int[ROWKEY_T1_SIZE];
    private static int ROWKEY_T1_IPV4_BYTES = 24;
    
    // [da][dp][sa][sp][ts][ts-ms]
    // [0] [1] [2] [3] [4] [5]
    private static int ROWKEY_T2_SIZE = 6;
    private int[] rowkeyT2idxs = new int[ROWKEY_T2_SIZE];
    private static int ROWKEY_T2_IPV4_BYTES = 24;
    
    int maxColumnCount = 0;

    enum RowKeyFields {
    	SRC_ADDRESS("Src IP Addr", 0, 2),
    	SRC_PORT("Src Pt", 1, 3),
    	DST_ADDRESS("Dst IP Addr", 2, 0),
    	DST_PORT("Dst Pt", 3, 1),
    	FIRST_SEEN("Date flow start", 4, 4);
    	
    	private int t1idx;
    	private int t2idx;
    	private String name;

    	RowKeyFields(String name, int t1idx, int t2idx){
    		this.setName(name);
    		this.t1idx = t1idx;
    		this.t2idx = t2idx;
    	}

    	public int getT1idx() {
			return t1idx;
		}

		public void setT1idx(int t1idx) {
			this.t1idx = t1idx;
		}

		public int getT2idx() {
			return t2idx;
		}

		public void setT2idx(int t2idx) {
			this.t2idx = t2idx;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}



    	
    }
    
    /**
	public NetFlowCSVParser(String header) {
		ArrayList<String> columnStrings = Lists.newArrayList(
		        Splitter.on(",").trimResults().split(header));

		      maxColumnCount = columnStrings.size();
		      families = new byte[maxColumnCount][];
		      qualifiers = new byte[maxColumnCount][];

		      for (int i = 0; i < maxColumnCount; i++) {
		        String str = columnStrings.get(i);
		        switch (str) {
				case SRC_ADDRESS:
					rowkeyT1idxs[0] = i;
					rowkeyT2idxs[2] = i;
					break;
				case SRC_PORT:
					rowkeyT1idxs[1] = i;
					rowkeyT2idxs[3] = i;
					break;
				case DST_ADDRESS:
					rowkeyT1idxs[2] = i;
					rowkeyT2idxs[0] = i;
					break;
				case DST_PORT:
					rowkeyT1idxs[3] = i;
					rowkeyT2idxs[1] = i;
					break;
				case FIRST_SEEN:
					rowkeyT1idxs[4] = i;
					rowkeyT2idxs[4] = i;
					break;
				default:
					break;
				}

		        families[i] = COLUMN_FAMILY_NAME.getBytes();
		        qualifiers[i] = Utils.getColumnQualifier(str);
		      }
	}**/
	
	public NetFlowCSVParser(String header, boolean fuck) {
		ArrayList<String> columnStrings = Lists.newArrayList(
		        Splitter.on(",").trimResults().split(header));

	      maxColumnCount = columnStrings.size();
//		      families = new byte[maxColumnCount][];
//		      qualifiers = new byte[maxColumnCount][];
		      
			families = new ArrayList<byte[]>();
			qualifiers = new ArrayList<byte[]>();

		      for (int i = 0; i < maxColumnCount; i++) {
		        String column = columnStrings.get(i);
		        if (column.equals(RowKeyFields.SRC_ADDRESS.getName())) {
					rowkeyT1idxs[RowKeyFields.SRC_ADDRESS.getT1idx()] = i;
					rowkeyT2idxs[RowKeyFields.SRC_ADDRESS.getT2idx()] = i;
					
				} else if (column.equals(RowKeyFields.SRC_PORT.getName())) {
					rowkeyT1idxs[RowKeyFields.SRC_PORT.getT1idx()] = i;
					rowkeyT2idxs[RowKeyFields.SRC_PORT.getT2idx()] = i;
					
				} else if (column.equals(RowKeyFields.DST_ADDRESS.getName())){
					rowkeyT1idxs[RowKeyFields.DST_ADDRESS.getT1idx()] = i;
					rowkeyT2idxs[RowKeyFields.DST_ADDRESS.getT2idx()] = i;
					
				} else if (column.equals(RowKeyFields.DST_PORT.getName())){
					rowkeyT1idxs[RowKeyFields.DST_PORT.getT1idx()] = i;
					rowkeyT2idxs[RowKeyFields.DST_PORT.getT2idx()] = i;

				} else if (column.equals(RowKeyFields.FIRST_SEEN.getName())){
					rowkeyT1idxs[RowKeyFields.FIRST_SEEN.getT1idx()] = i;
					rowkeyT2idxs[RowKeyFields.FIRST_SEEN.getT2idx()] = i;
				} else {
			        families.add(COLUMN_FAMILY_NAME.getBytes());
			        qualifiers.add(Utils.getColumnQualifier(column));
				}
		      }
	}

	public byte[] prepareRowKeyT1(List<String> fields){
		// [sa][sp][da][dp][1 - ts]
	    // [0] [1] [2] [3] [4] 
		String rowkeyT1String = "";
		ByteBuffer rowkeyT1 = ByteBuffer.allocate(ROWKEY_T1_IPV4_BYTES);
		byte[] output = new byte[ROWKEY_T1_IPV4_BYTES];
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		long firstSeen;
		int port;
		try {
			for (int i = 0; i < rowkeyT1idxs.length; i++) {
				
				// SRC/DST IP transformation to byte[]
				if (i == RowKeyFields.SRC_ADDRESS.getT1idx() || i == RowKeyFields.DST_ADDRESS.getT1idx()){
					InetAddress address = InetAddress.getByName(fields.get(rowkeyT1idxs[i]));
					rowkeyT1.put(address.getAddress());
				} 
				// FIRST_SEEN in milliseconds to byte[] long-8bytes
				else if (i == RowKeyFields.FIRST_SEEN.getT1idx()){
					// 2013-03-03 12:29:33.730
					firstSeen = dateFormat.parse(fields.get(rowkeyT1idxs[i])).getTime();
					rowkeyT1.put(Bytes.toBytes(Long.MAX_VALUE - firstSeen));
				}
				// SPORT/DPORT to byte[] int-4bytes FIXME: two bytes are only required
				else if (i == RowKeyFields.SRC_PORT.getT1idx() || i == RowKeyFields.DST_PORT.getT1idx()){
					port = Integer.parseInt(fields.get(rowkeyT1idxs[i]));
					rowkeyT1.put(Bytes.toBytes(port));
				}
				
				rowkeyT1String = rowkeyT1String.concat("[" + fields.get(rowkeyT1idxs[i]) + "]");
			}
//			System.out.println("RowKeyT1 string representation" + rowkeyT1String);
			output = rowkeyT1.array();
//			System.out.println(output.length);
//			System.out.println("Source IP: " + InetAddress.getByAddress(Arrays.copyOfRange(output, 0, 4)).getHostAddress());
//			System.out.println("Source Port: " + Bytes.toShort(Arrays.copyOfRange(output, 4, 6)));
//			System.out.println("Destination IP: " + InetAddress.getByAddress(Arrays.copyOfRange(output, 6, 10)));
//			System.out.println("Destination Port: " + Bytes.toShort(Arrays.copyOfRange(output, 10, 12)));
//			System.out.println("FirstSeen: " + new Date(Bytes.toLong(Arrays.copyOfRange(output, 12, 20))));
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		return output;
	}
	
	public List<CFQValue> prepareValuesT1(List<String> allFields){
		List<String> fields = new ArrayList<>(allFields);
		
		List<CFQValue> cfqValues = new ArrayList<CFQValue>();
		// Removing RowKey elements
		for (int i = 0; i < rowkeyT1idxs.length; i++) {
			fields.set(rowkeyT1idxs[i], null);
		}
		for (int i = 0; i < rowkeyT1idxs.length; i++) {
			fields.remove(null);
		}
		// Preparing the remaining values
		try {
			cfqValues = NetFlowV5Record.encodeCFQValues(fields, families, qualifiers);
			
//			for (CFQValue cfqValue : cfqValues) {
//				System.out.println(NetFlowV5Record.decodeCFQValues(cfqValue));
//			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cfqValues;
	}
	
	public byte[] prepareRowKeyT2(List<String> fields){
	    // [da][dp][sa][sp][1 - ts]
	    // [0] [1] [2] [3] [4] 
		String rowkeyT2String = "";
		ByteBuffer rowkeyT2 = ByteBuffer.allocate(ROWKEY_T2_IPV4_BYTES);
		byte[] output = new byte[ROWKEY_T2_IPV4_BYTES];
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		long firstSeen;
		int port;
		try {
			for (int i = 0; i < rowkeyT2idxs.length; i++) {
				
				// SRC/DST IP transformation to byte[]
				if (i == RowKeyFields.SRC_ADDRESS.getT2idx() || i == RowKeyFields.DST_ADDRESS.getT2idx()){
					InetAddress address = InetAddress.getByName(fields.get(rowkeyT2idxs[i]));
					rowkeyT2.put(address.getAddress());
				} 
				// FIRST_SEEN in milliseconds to byte[] long-8bytes
				else if (i == RowKeyFields.FIRST_SEEN.getT2idx()){
					// 2013-03-03 12:29:33.730
					firstSeen = dateFormat.parse(fields.get(rowkeyT2idxs[i])).getTime();
					rowkeyT2.put(Bytes.toBytes(Long.MAX_VALUE - firstSeen));
				}
				// SPORT/DPORT to byte[] short-2bytes
				else if (i == RowKeyFields.SRC_PORT.getT2idx() || i == RowKeyFields.DST_PORT.getT2idx()){
					port = Integer.parseInt(fields.get(rowkeyT2idxs[i]));
					rowkeyT2.put(Bytes.toBytes(port));
				}
				
				rowkeyT2String = rowkeyT2String.concat("[" + fields.get(rowkeyT2idxs[i]) + "]");
			}
			output = rowkeyT2.array();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		return output;
	}
	
	public List<CFQValue> prepareValuesT2(List<String> fields){
//		prepareRowKeyT1(fields);
		CFQValue cfqValue = new CFQValue(COLUMN_FAMILY_NAME.getBytes(), null, new byte[0]);
		List<CFQValue> list = new ArrayList<CFQValue>();
		list.add(cfqValue);
		return list;
	}
	
	public int[] getRowkeyT1idxs() {
		return rowkeyT1idxs;
	}

	public void setRowkeyT1idxs(int[] rowkeyT1idxs) {
		this.rowkeyT1idxs = rowkeyT1idxs;
	}

	public int[] getRowkeyT2idxs() {
		return rowkeyT2idxs;
	}

	public void setRowkeyT2idxs(int[] rowkeyT2idxs) {
		this.rowkeyT2idxs = rowkeyT2idxs;
	}

	public List<byte[]> getFamilies() {
		return families;
	}

	public List<byte[]> getQualifiers() {
		return qualifiers;
	}
	
	
	public static void printRowKeyT1(byte[] input){
		try {
			System.out.println("KeySize: " + input.length);
			System.out.println("Source IP: " + InetAddress.getByAddress(Arrays.copyOfRange(input, 0, 4)).getHostAddress());
			System.out.println("Source Port: " + Bytes.toInt(Arrays.copyOfRange(input, 4, 8)));
			System.out.println("Destination IP: " + InetAddress.getByAddress(Arrays.copyOfRange(input, 8, 12)).getHostAddress());
			System.out.println("Destination Port: " + Bytes.toInt(Arrays.copyOfRange(input, 12, 16)));
			System.out.println("FirstSeen: " + new Date(Long.MAX_VALUE - Bytes.toLong(Arrays.copyOfRange(input, 16, 24))) + " :" + (Long.MAX_VALUE - Bytes.toLong(Arrays.copyOfRange(input, 16, 24))));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public static String decodeRowKeyT1(byte[] input){
		try {
			return "Source IP: " + InetAddress.getByAddress(Arrays.copyOfRange(input, 0, 4)).getHostAddress() +
					",Source Port: " + Bytes.toInt(Arrays.copyOfRange(input, 4, 8)) +
					",Destination IP: " + InetAddress.getByAddress(Arrays.copyOfRange(input, 8, 12)).getHostAddress() +
					",Destination Port: " + Bytes.toInt(Arrays.copyOfRange(input, 12, 16)) +
					",FirstSeen: " + new Date(Long.MAX_VALUE - Bytes.toLong(Arrays.copyOfRange(input, 16, 24))) +
					",KeySize: " + input.length;
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static void printRowKeyT2(byte[] input){
		try {
			System.out.println("KeySize: " + input.length);
			System.out.println("Destination IP: " + InetAddress.getByAddress(Arrays.copyOfRange(input, 0, 4)));
			System.out.println("Destination Port: " + Bytes.toInt(Arrays.copyOfRange(input, 4, 8)));
			System.out.println("Source IP: " + InetAddress.getByAddress(Arrays.copyOfRange(input, 8, 12)).getHostAddress());
			System.out.println("Source Port: " + Bytes.toInt(Arrays.copyOfRange(input, 12, 16)));
			System.out.println("FirstSeen: " + new Date(Long.MAX_VALUE - Bytes.toLong(Arrays.copyOfRange(input, 16, 24))));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public static String decodeRowKeyT2(byte[] input){
		try {
			return "KeySize: " + input.length +
					"Destination IP: " + InetAddress.getByAddress(Arrays.copyOfRange(input, 0, 4)) +
					"Destination Port: " + Bytes.toInt(Arrays.copyOfRange(input, 4, 8)) +
					"Source IP: " + InetAddress.getByAddress(Arrays.copyOfRange(input, 8, 12)).getHostAddress() +
					"Source Port: " + Bytes.toInt(Arrays.copyOfRange(input, 12, 16)) +
					"FirstSeen: " + new Date(Long.MAX_VALUE - Bytes.toLong(Arrays.copyOfRange(input, 16, 24)));
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private static void printRowValues(List<CFQValue> values){
		for (CFQValue cfqValue : values) {
			System.out.println(new String(cfqValue.getColumnFamily())+ ":" +
								new String(cfqValue.getQualifier())+ ":" +
								new String(cfqValue.getValue()));
		}
	}
	public static void main(String[] args) {
		NetFlowCSVParser parser = new NetFlowCSVParser("Date flow start,Date flow end,Duration,Src IP Addr," +
				"Dst IP Addr, Src Pt, Dst Pt ,Proto  ,Flags ,Fwd ,STos   ,In Pkt  ,In Byte  ,Out Pkt ,Out Byte  ,Input ," +
				"Output ,Src AS ,Dst AS ,SMask ,DMask ,DTos ,Dir      ,Next-hop IP  ,BGP next-hop IP ,SVlan ,DVlan   ," +
				"In src MAC Addr  ,Out dst MAC Addr   ,In dst MAC Addr  ,Out src MAC Addr  ,Router IP,MPLS lbl 1   ,MPLS lbl 2   ," +
				"MPLS lbl 3   ,MPLS lbl 4", false);
		
//		for (int i = 0; i < parser.getQualifiers().size(); i++) {
//			System.out.print(new String(parser.getFamilies().get(i)) + ":");
//			System.out.print(Bytes.toInt(parser.getQualifiers().get(i))+",");
//		}
//		System.out.println();
//		for (int i = 0; i < parser.getRowkeyT1idxs().length; i++) {
//			System.out.println(parser.getRowkeyT1idxs()[i]);
//			System.out.println(parser.getRowkeyT2idxs()[i]);
//		}
//		System.out.println('\0');
		
		ArrayList<String> fields = Lists.newArrayList(
		        Splitter.on(",").trimResults().split("2013-03-03 11:59:32.091,2013-03-03 11:59:32.091,    0.000,    64.170.31.82,   161.223.1.142, 64413,   443,6    ,.A....,  0,   0,    1000,   52000,       0,       0,   535,   525,     0,     0,    0,    0,   0,  I		,         0.0.0.0,         0.0.0.0,    0,    0,00:00:00:00:00:00,00:00:00:00:00:00,00:00:00:00:00:00,00:00:00:00:00:00,         0.0.0.0,       0-0-0,       0-0-0,       0-0-0,       0-0-0"));
		
		
		printRowKeyT1(parser.prepareRowKeyT1(fields));
		parser.prepareValuesT1(fields);
//		for (int i = 0; i < parser.getQualifiers().size(); i++) {
//			System.out.print(new String(parser.getFamilies().get(i)) + ":");
//			System.out.print(Bytes.toInt(parser.getQualifiers().get(i))+",");
//		}
		
//		printRowKeyT2(parser.prepareRowKeyT2(fields));
//		System.out.println(parser.prepareRowKeyT1(fields));
//		System.out.println(parser.prepareRowKeyT2(fields));
//		
//		try {
//			InetAddress address = Inet6Address.getByName("::ffff:192.168.1.1");
//			System.out.println(InetAddressUtils.isIPv4Address(address.getHostAddress()));
//			System.out.println(InetAddressUtils.isIPv6Address(address.getHostAddress()));
//			InetAddress address2 = Inet6Address.getByName("3ffe:1900:4545:3:200:f8ff:fe21:67cf");
//			for (int i = 0; i < address.getAddress().length; i++) {
//				System.out.println((address.getAddress()[i]));
//			} 
//			System.out.println(address.getAddress().length + " ------" + address2.getAddress().length );
//			for (int i = 0; i < address2.getAddress().length; i++) {
//				System.out.println((address2.getAddress()[i]));
//			} 
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
				
		
		
	}
}
