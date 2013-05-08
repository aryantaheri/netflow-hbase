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
import org.apache.hadoop.hbase.client.Put;
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

    // [sa][da][sp][dp][1 - ts]
    // [0] [1] [2] [3] [4] 
    private static int ROWKEY_T3_SIZE = 6;
    private int[] rowkeyT3idxs = new int[ROWKEY_T3_SIZE];
    private static int ROWKEY_T3_IPV4_BYTES = 24;
    
    // [da][sa][dp][sp][1 - ts]
    // [0] [1] [2] [3] [4] [5]
    private static int ROWKEY_T4_SIZE = 6;
    private int[] rowkeyT4idxs = new int[ROWKEY_T4_SIZE];
    private static int ROWKEY_T4_IPV4_BYTES = 24;
    
    // [sp][sa][da][dp][1 - ts]
    // [0] [1] [2] [3] [4] 
    private static int ROWKEY_T5_SIZE = 6;
    private int[] rowkeyT5idxs = new int[ROWKEY_T5_SIZE];
    private static int ROWKEY_T5_IPV4_BYTES = 24;
    
    // [dp][da][sa][sp][1 - ts]
    // [0] [1] [2] [3] [4] 
    private static int ROWKEY_T6_SIZE = 6;
    private int[] rowkeyT6idxs = new int[ROWKEY_T6_SIZE];
    private static int ROWKEY_T6_IPV4_BYTES = 24;
    
    // [sp][da][sa][dp][1 - ts]
    // [0] [1] [2] [3] [4] 
    private static int ROWKEY_T7_SIZE = 6;
    private int[] rowkeyT7idxs = new int[ROWKEY_T7_SIZE];
    private static int ROWKEY_T7_IPV4_BYTES = 24;
    
    // [dp][sa][da][sp][1 - ts]
    // [0] [1] [2] [3] [4] 
    private static int ROWKEY_T8_SIZE = 6;
    private int[] rowkeyT8idxs = new int[ROWKEY_T8_SIZE];
    private static int ROWKEY_T8_IPV4_BYTES = 24;
    
    int maxColumnCount = 0;

    enum RowKeyFields {
    	SRC_ADDRESS("Src IP Addr"	, 0, 2, 0, 1, 1, 2, 2, 1),
    	SRC_PORT("Src Pt"			, 1, 3, 2, 3, 0, 3, 0, 3),
    	DST_ADDRESS("Dst IP Addr"	, 2, 0, 1, 0, 2, 1, 1, 2),
    	DST_PORT("Dst Pt"			, 3, 1, 3, 2, 3, 0, 3, 0),
    	FIRST_SEEN("Date flow start", 4, 4, 4, 4, 4, 4, 4, 4);
    	
    	private int t1idx;
    	private int t2idx;
    	
    	private int t3idx;
    	private int t4idx;

    	private int t5idx;
    	private int t6idx;

    	private int t7idx;
    	private int t8idx;



    	private String name;

//    	@Deprecated
//    	RowKeyFields(String name, int t1idx, int t2idx){
//    		this.setName(name);
//    		this.t1idx = t1idx;
//    		this.t2idx = t2idx;
//    	}
    	
    	RowKeyFields(String name, int t1idx, int t2idx, int t3idx, int t4idx, int t5idx, int t6idx, int t7idx, int t8idx){
    		this.setName(name);
    		this.t1idx = t1idx;
    		this.t2idx = t2idx;
    		
    		this.t3idx = t3idx;
    		this.t4idx = t4idx;
    		
    		this.t5idx = t5idx;
    		this.t6idx = t6idx;
    		
    		this.t7idx = t7idx;
    		this.t8idx = t8idx;
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

		public int getT3idx() {
			return t3idx;
		}

		public void setT3idx(int t3idx) {
			this.t3idx = t3idx;
		}

		public int getT4idx() {
			return t4idx;
		}

		public void setT4idx(int t4idx) {
			this.t4idx = t4idx;
		}

		public int getT5idx() {
			return t5idx;
		}

		public void setT5idx(int t5idx) {
			this.t5idx = t5idx;
		}

		public int getT6idx() {
			return t6idx;
		}

		public void setT6idx(int t6idx) {
			this.t6idx = t6idx;
		}

		public int getT7idx() {
			return t7idx;
		}

		public void setT7idx(int t7idx) {
			this.t7idx = t7idx;
		}

		public int getT8idx() {
			return t8idx;
		}

		public void setT8idx(int t8idx) {
			this.t8idx = t8idx;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getIdx(int table){
			switch (table) {
			case 1:
				return getT1idx();
			case 2:
				return getT2idx();
			case 3:
				return getT3idx();
			case 4:
				return getT4idx();
			case 5:
				return getT5idx();
			case 6:
				return getT6idx();
			case 7:
				return getT7idx();
			case 8:
				return getT8idx();

			default:
				break;
			}
			return -1;
		}

    	
    }
    
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
					rowkeyT3idxs[RowKeyFields.SRC_ADDRESS.getT3idx()] = i;
					rowkeyT4idxs[RowKeyFields.SRC_ADDRESS.getT4idx()] = i;
					rowkeyT5idxs[RowKeyFields.SRC_ADDRESS.getT5idx()] = i;
					rowkeyT6idxs[RowKeyFields.SRC_ADDRESS.getT6idx()] = i;
					rowkeyT7idxs[RowKeyFields.SRC_ADDRESS.getT7idx()] = i;
					rowkeyT8idxs[RowKeyFields.SRC_ADDRESS.getT8idx()] = i;
					
				} else if (column.equals(RowKeyFields.SRC_PORT.getName())) {
					rowkeyT1idxs[RowKeyFields.SRC_PORT.getT1idx()] = i;
					rowkeyT2idxs[RowKeyFields.SRC_PORT.getT2idx()] = i;
					rowkeyT3idxs[RowKeyFields.SRC_PORT.getT3idx()] = i;
					rowkeyT4idxs[RowKeyFields.SRC_PORT.getT4idx()] = i;
					rowkeyT5idxs[RowKeyFields.SRC_PORT.getT5idx()] = i;
					rowkeyT6idxs[RowKeyFields.SRC_PORT.getT6idx()] = i;
					rowkeyT7idxs[RowKeyFields.SRC_PORT.getT7idx()] = i;
					rowkeyT8idxs[RowKeyFields.SRC_PORT.getT8idx()] = i;
					
				} else if (column.equals(RowKeyFields.DST_ADDRESS.getName())){
					rowkeyT1idxs[RowKeyFields.DST_ADDRESS.getT1idx()] = i;
					rowkeyT2idxs[RowKeyFields.DST_ADDRESS.getT2idx()] = i;
					rowkeyT3idxs[RowKeyFields.DST_ADDRESS.getT3idx()] = i;
					rowkeyT4idxs[RowKeyFields.DST_ADDRESS.getT4idx()] = i;
					rowkeyT5idxs[RowKeyFields.DST_ADDRESS.getT5idx()] = i;
					rowkeyT6idxs[RowKeyFields.DST_ADDRESS.getT6idx()] = i;
					rowkeyT7idxs[RowKeyFields.DST_ADDRESS.getT7idx()] = i;
					rowkeyT8idxs[RowKeyFields.DST_ADDRESS.getT8idx()] = i;
					
				} else if (column.equals(RowKeyFields.DST_PORT.getName())){
					rowkeyT1idxs[RowKeyFields.DST_PORT.getT1idx()] = i;
					rowkeyT2idxs[RowKeyFields.DST_PORT.getT2idx()] = i;
					rowkeyT3idxs[RowKeyFields.DST_PORT.getT3idx()] = i;
					rowkeyT4idxs[RowKeyFields.DST_PORT.getT4idx()] = i;
					rowkeyT5idxs[RowKeyFields.DST_PORT.getT5idx()] = i;
					rowkeyT6idxs[RowKeyFields.DST_PORT.getT6idx()] = i;
					rowkeyT7idxs[RowKeyFields.DST_PORT.getT7idx()] = i;
					rowkeyT8idxs[RowKeyFields.DST_PORT.getT8idx()] = i;

				} else if (column.equals(RowKeyFields.FIRST_SEEN.getName())){
					rowkeyT1idxs[RowKeyFields.FIRST_SEEN.getT1idx()] = i;
					rowkeyT2idxs[RowKeyFields.FIRST_SEEN.getT2idx()] = i;
					rowkeyT3idxs[RowKeyFields.FIRST_SEEN.getT3idx()] = i;
					rowkeyT4idxs[RowKeyFields.FIRST_SEEN.getT4idx()] = i;
					rowkeyT5idxs[RowKeyFields.FIRST_SEEN.getT5idx()] = i;
					rowkeyT6idxs[RowKeyFields.FIRST_SEEN.getT6idx()] = i;
					rowkeyT7idxs[RowKeyFields.FIRST_SEEN.getT7idx()] = i;
					rowkeyT8idxs[RowKeyFields.FIRST_SEEN.getT8idx()] = i;
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
		List<String> fields = new ArrayList<String>(allFields);
		
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
	
	public List<CFQValue> prepareDummyValue(){
		CFQValue cfqValue = new CFQValue(COLUMN_FAMILY_NAME.getBytes(), null, new byte[0]);
		List<CFQValue> list = new ArrayList<CFQValue>();
		list.add(cfqValue);
		return list;
	}
	
	public Put getDummyPut(byte[] rowkey){
		Put put = new Put(rowkey);
		List<CFQValue> values = this.prepareDummyValue();
		for (CFQValue cfqValue : values) {
			put.add(cfqValue.getColumnFamily(), cfqValue.getQualifier(), cfqValue.getValue());
		}
		return put;
	}
	
	
	public byte[] prepareRowKeyT3(List<String> fields){
		ByteBuffer rowkeyT3 = ByteBuffer.allocate(ROWKEY_T3_IPV4_BYTES);
		byte[] output = new byte[ROWKEY_T3_IPV4_BYTES];
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		long firstSeen;
		int port;
		try {
			for (int i = 0; i < rowkeyT3idxs.length; i++) {
				
				// SRC/DST IP transformation to byte[]
				if (i == RowKeyFields.SRC_ADDRESS.getT3idx() || i == RowKeyFields.DST_ADDRESS.getT3idx()){
					InetAddress address = InetAddress.getByName(fields.get(rowkeyT3idxs[i]));
					rowkeyT3.put(address.getAddress());
				} 
				// FIRST_SEEN in milliseconds to byte[] long-8bytes
				else if (i == RowKeyFields.FIRST_SEEN.getT3idx()){
					// 2013-03-03 12:29:33.730
					firstSeen = dateFormat.parse(fields.get(rowkeyT3idxs[i])).getTime();
					rowkeyT3.put(Bytes.toBytes(Long.MAX_VALUE - firstSeen));
				}
				// SPORT/DPORT to byte[] short-2bytes
				else if (i == RowKeyFields.SRC_PORT.getT3idx() || i == RowKeyFields.DST_PORT.getT3idx()){
					port = Integer.parseInt(fields.get(rowkeyT3idxs[i]));
					rowkeyT3.put(Bytes.toBytes(port));
				}
			}
			output = rowkeyT3.array();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		return output;
	}

	
	public byte[] prepareRowKeyT4(List<String> fields){
		ByteBuffer rowkeyT4 = ByteBuffer.allocate(ROWKEY_T4_IPV4_BYTES);
		byte[] output = new byte[ROWKEY_T4_IPV4_BYTES];
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		long firstSeen;
		int port;
		try {
			for (int i = 0; i < rowkeyT4idxs.length; i++) {
				
				// SRC/DST IP transformation to byte[]
				if (i == RowKeyFields.SRC_ADDRESS.getT4idx() || i == RowKeyFields.DST_ADDRESS.getT4idx()){
					InetAddress address = InetAddress.getByName(fields.get(rowkeyT4idxs[i]));
					rowkeyT4.put(address.getAddress());
				} 
				// FIRST_SEEN in milliseconds to byte[] long-8bytes
				else if (i == RowKeyFields.FIRST_SEEN.getT4idx()){
					// 2013-03-03 12:29:33.730
					firstSeen = dateFormat.parse(fields.get(rowkeyT4idxs[i])).getTime();
					rowkeyT4.put(Bytes.toBytes(Long.MAX_VALUE - firstSeen));
				}
				// SPORT/DPORT to byte[] short-2bytes
				else if (i == RowKeyFields.SRC_PORT.getT4idx() || i == RowKeyFields.DST_PORT.getT4idx()){
					port = Integer.parseInt(fields.get(rowkeyT4idxs[i]));
					rowkeyT4.put(Bytes.toBytes(port));
				}
			}
			output = rowkeyT4.array();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		return output;
	}
	
	public byte[] prepareRowKeyT5(List<String> fields){
		ByteBuffer rowkeyT5 = ByteBuffer.allocate(ROWKEY_T5_IPV4_BYTES);
		byte[] output = new byte[ROWKEY_T5_IPV4_BYTES];
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		long firstSeen;
		int port;
		try {
			for (int i = 0; i < rowkeyT5idxs.length; i++) {
				
				// SRC/DST IP transformation to byte[]
				if (i == RowKeyFields.SRC_ADDRESS.getT5idx() || i == RowKeyFields.DST_ADDRESS.getT5idx()){
					InetAddress address = InetAddress.getByName(fields.get(rowkeyT5idxs[i]));
					rowkeyT5.put(address.getAddress());
				} 
				// FIRST_SEEN in milliseconds to byte[] long-8bytes
				else if (i == RowKeyFields.FIRST_SEEN.getT5idx()){
					// 2013-03-03 12:29:33.730
					firstSeen = dateFormat.parse(fields.get(rowkeyT5idxs[i])).getTime();
					rowkeyT5.put(Bytes.toBytes(Long.MAX_VALUE - firstSeen));
				}
				// SPORT/DPORT to byte[] short-2bytes
				else if (i == RowKeyFields.SRC_PORT.getT5idx() || i == RowKeyFields.DST_PORT.getT5idx()){
					port = Integer.parseInt(fields.get(rowkeyT5idxs[i]));
					rowkeyT5.put(Bytes.toBytes(port));
				}
			}
			output = rowkeyT5.array();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		return output;
	}

	public byte[] prepareRowKeyT6(List<String> fields){
		ByteBuffer rowkeyT6 = ByteBuffer.allocate(ROWKEY_T6_IPV4_BYTES);
		byte[] output = new byte[ROWKEY_T6_IPV4_BYTES];
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		long firstSeen;
		int port;
		try {
			for (int i = 0; i < rowkeyT6idxs.length; i++) {
				
				// SRC/DST IP transformation to byte[]
				if (i == RowKeyFields.SRC_ADDRESS.getT6idx() || i == RowKeyFields.DST_ADDRESS.getT6idx()){
					InetAddress address = InetAddress.getByName(fields.get(rowkeyT6idxs[i]));
					rowkeyT6.put(address.getAddress());
				} 
				// FIRST_SEEN in milliseconds to byte[] long-8bytes
				else if (i == RowKeyFields.FIRST_SEEN.getT6idx()){
					// 2013-03-03 12:29:33.730
					firstSeen = dateFormat.parse(fields.get(rowkeyT6idxs[i])).getTime();
					rowkeyT6.put(Bytes.toBytes(Long.MAX_VALUE - firstSeen));
				}
				// SPORT/DPORT to byte[] short-2bytes
				else if (i == RowKeyFields.SRC_PORT.getT6idx() || i == RowKeyFields.DST_PORT.getT6idx()){
					port = Integer.parseInt(fields.get(rowkeyT6idxs[i]));
					rowkeyT6.put(Bytes.toBytes(port));
				}
			}
			output = rowkeyT6.array();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		return output;
	}

	public byte[] prepareRowKeyT7(List<String> fields){
		ByteBuffer rowkeyT7 = ByteBuffer.allocate(ROWKEY_T7_IPV4_BYTES);
		byte[] output = new byte[ROWKEY_T7_IPV4_BYTES];
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		long firstSeen;
		int port;
		try {
			for (int i = 0; i < rowkeyT7idxs.length; i++) {
				
				// SRC/DST IP transformation to byte[]
				if (i == RowKeyFields.SRC_ADDRESS.getT7idx() || i == RowKeyFields.DST_ADDRESS.getT7idx()){
					InetAddress address = InetAddress.getByName(fields.get(rowkeyT7idxs[i]));
					rowkeyT7.put(address.getAddress());
				} 
				// FIRST_SEEN in milliseconds to byte[] long-8bytes
				else if (i == RowKeyFields.FIRST_SEEN.getT7idx()){
					// 2013-03-03 12:29:33.730
					firstSeen = dateFormat.parse(fields.get(rowkeyT7idxs[i])).getTime();
					rowkeyT7.put(Bytes.toBytes(Long.MAX_VALUE - firstSeen));
				}
				// SPORT/DPORT to byte[] short-2bytes
				else if (i == RowKeyFields.SRC_PORT.getT7idx() || i == RowKeyFields.DST_PORT.getT7idx()){
					port = Integer.parseInt(fields.get(rowkeyT7idxs[i]));
					rowkeyT7.put(Bytes.toBytes(port));
				}
			}
			output = rowkeyT7.array();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		return output;
	}

	public byte[] prepareRowKeyT8(List<String> fields){
		ByteBuffer rowkeyT8 = ByteBuffer.allocate(ROWKEY_T8_IPV4_BYTES);
		byte[] output = new byte[ROWKEY_T8_IPV4_BYTES];
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		long firstSeen;
		int port;
		try {
			for (int i = 0; i < rowkeyT8idxs.length; i++) {
				
				// SRC/DST IP transformation to byte[]
				if (i == RowKeyFields.SRC_ADDRESS.getT8idx() || i == RowKeyFields.DST_ADDRESS.getT8idx()){
					InetAddress address = InetAddress.getByName(fields.get(rowkeyT8idxs[i]));
					rowkeyT8.put(address.getAddress());
				} 
				// FIRST_SEEN in milliseconds to byte[] long-8bytes
				else if (i == RowKeyFields.FIRST_SEEN.getT8idx()){
					// 2013-03-03 12:29:33.730
					firstSeen = dateFormat.parse(fields.get(rowkeyT8idxs[i])).getTime();
					rowkeyT8.put(Bytes.toBytes(Long.MAX_VALUE - firstSeen));
				}
				// SPORT/DPORT to byte[] short-2bytes
				else if (i == RowKeyFields.SRC_PORT.getT8idx() || i == RowKeyFields.DST_PORT.getT8idx()){
					port = Integer.parseInt(fields.get(rowkeyT8idxs[i]));
					rowkeyT8.put(Bytes.toBytes(port));
				}
			}
			output = rowkeyT8.array();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		return output;
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
	
	
	public static String decodeRowKey(byte[] input, int table){
		int saIdx = RowKeyFields.SRC_ADDRESS.getIdx(table);
		int spIdx = RowKeyFields.SRC_PORT.getIdx(table);
		int daIdx = RowKeyFields.DST_ADDRESS.getIdx(table);
		int dpIdx = RowKeyFields.DST_PORT.getIdx(table);
		int tsIdx = RowKeyFields.FIRST_SEEN.getIdx(table);
		String[] orderedkey = new String[5];
		String output = "Table" + table + ", ";
		
		try {
			orderedkey[saIdx] = "Source IP: " + InetAddress.getByAddress(Arrays.copyOfRange(input, saIdx*4, (saIdx+1)*4)).getHostAddress();
			orderedkey[spIdx] = "Source Port: " + Bytes.toInt(Arrays.copyOfRange(input, spIdx*4, (spIdx+1)*4));
			orderedkey[daIdx] = "Destination IP: " + InetAddress.getByAddress(Arrays.copyOfRange(input, daIdx*4, (daIdx+1)*4)).getHostAddress();
			orderedkey[dpIdx] = "Destination Port: " + Bytes.toInt(Arrays.copyOfRange(input, dpIdx*4, (dpIdx+1)*4));
			orderedkey[tsIdx] = "FirstSeen: " + new Date(Long.MAX_VALUE - Bytes.toLong(Arrays.copyOfRange(input, tsIdx*4, (tsIdx+2)*4)));
			
			for (int i = 0; i < orderedkey.length; i++) {
				output = output.concat(orderedkey[i]).concat(", ");
			}
			
			output = output.concat("KeySize: " + input.length);
			
			return output;
			
//			return "Table" + table + ", Source IP: " + InetAddress.getByAddress(Arrays.copyOfRange(input, saIdx*4, (saIdx+1)*4)).getHostAddress() +
//					",Source Port: " + Bytes.toInt(Arrays.copyOfRange(input, spIdx*4, (spIdx+1)*4)) +
//					",Destination IP: " + InetAddress.getByAddress(Arrays.copyOfRange(input, daIdx*4, (daIdx+1)*4)).getHostAddress() +
//					",Destination Port: " + Bytes.toInt(Arrays.copyOfRange(input, dpIdx*4, (dpIdx+1)*4)) +
//					",FirstSeen: " + new Date(Long.MAX_VALUE - Bytes.toLong(Arrays.copyOfRange(input, tsIdx*4, (tsIdx+2)*4))) +
//					",KeySize: " + input.length;
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
				"Dst IP Addr, Src Pt, Dst Pt ,Proto  ,Flags ,Fwd ,STos   ,In Pkt  ,In Byte  ,Out Pkt ,Out Byte  , Flows, Input ," +
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
		        Splitter.on(",").trimResults().split("2013-03-03 11:59:32.091,2013-03-03 11:59:32.091,    0.000,    64.170.31.82,   161.223.1.142, 64413,   443,6    ,.A....,  0,   0,    1000,   52000,       0,       0,   1,535,   525,     0,     0,    0,    0,   0,  I		,         0.0.0.0,         0.0.0.0,    0,    0,00:00:00:00:00:00,00:00:00:00:00:00,00:00:00:00:00:00,00:00:00:00:00:00,         0.0.0.0,       0-0-0,       0-0-0,       0-0-0,       0-0-0"));
		
		
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
