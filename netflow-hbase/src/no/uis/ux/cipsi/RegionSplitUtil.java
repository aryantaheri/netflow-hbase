package no.uis.ux.cipsi;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;
import org.apache.http.impl.client.RedirectLocations;

import com.google.common.base.Preconditions;

// split
public class RegionSplitUtil {
	
	
	/**
	 * Info:
	 * 
	 * T1 ROWS=32613828
	 * T2 ROWS=32613828
	 * T3 ROWS=32613828
	 * T4 ROWS=32613828
	 * T5 ROWS=32613828
	 * T6 ROWS=32613828
	 * T7 ROWS=32613828
	 * T8 ROWS=32613828
	 */
	
	private static HBaseAdmin hbaseAdmin;
	/**
     * Scan (or list) a table
     */
    public static void scan (Configuration conf, String tableName) {
        try{
             HTable table = new HTable(conf, tableName);
             Scan scan = new Scan();
             FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter();
             KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
             FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
             filterList.addFilter(keyOnlyFilter);
             filterList.addFilter(firstKeyOnlyFilter);
             scan.setFilter(filterList);
             ResultScanner ss = table.getScanner(scan);
             int count = 0;
             boolean first = true;
             for(Result r:ss){
//            	 for(KeyValue kv : r.raw()){
//                	 System.out.println(NetFlowCSVParser.decodeRowKey(kv.getRow(), Integer.parseInt(tableName)));
////                	 System.out.print(bytesToBits(kv.getRow()));
//                	 System.out.println();
////	                     System.out.println("KeyValue TS: " + new Date(kv.getTimestamp()));
////                     System.out.println(NetFlowV5Record.decodeCFQValues(new CFQValue(kv.getFamily(), kv.getQualifier(), kv.getValue())));
//                 }
                 count++;
             }
             System.out.println("Returned results: " + count);
        } catch (IOException e){
            e.printStackTrace();
        }
    }
    

    /**
     * A SplitAlgorithm that divides the space of possible keys evenly. Useful
     * when the keys are approximately uniform random bytes (e.g. hashes). Rows
     * are raw byte values in the range <b>00 => FF</b> and are right-padded with
     * zeros to keep the same memcmp() order. This is the natural algorithm to use
     * for a byte[] environment and saves space, but is not necessarily the
     * easiest for readability.
     */
    public static class UniformSplit implements SplitAlgorithm {
      static final byte xFF = (byte) 0xFF;
      byte[] firstRowBytes;
      byte[] lastRowBytes;
      
      public byte[] split(byte[] start, byte[] end) {
        return Bytes.split(start, end, 1)[1];
      }

      @Override
      public byte[][] split(int numRegions) {
    	  InetAddress startAddress = null;
    	  InetAddress endAddress = null;
    	  try {
			startAddress = InetAddress.getByName("0.0.0.0");
			endAddress = InetAddress.getByName("255.255.255.255");
    	  } catch (UnknownHostException e) {
    		  e.printStackTrace();
    	  }
    	firstRowBytes = startAddress.getAddress();
    	lastRowBytes = endAddress.getAddress();
    	  
        Preconditions.checkArgument(
            Bytes.compareTo(lastRowBytes, firstRowBytes) > 0,
            "last row (%s) is configured less than first row (%s)",
            Bytes.toStringBinary(lastRowBytes),
            Bytes.toStringBinary(firstRowBytes));

        byte[][] splits = Bytes.split(firstRowBytes, lastRowBytes, numRegions - 1);
        Preconditions.checkState(splits != null,
            "Could not split region with given user input: " + this);

        // remove endpoints, which are included in the splits list
        return Arrays.copyOfRange(splits, 1, splits.length - 1);
      }

      @Override
      public byte[] firstRow() {
        return firstRowBytes;
      }

      @Override
      public byte[] lastRow() {
        return lastRowBytes;
      }

      public void setFirstRow(String userInput) {
        firstRowBytes = Bytes.toBytesBinary(userInput);
      }

      public void setLastRow(String userInput) {
        lastRowBytes = Bytes.toBytesBinary(userInput);
      }

      @Override
      public byte[] strToRow(String input) {
        return Bytes.toBytesBinary(input);
      }

      @Override
      public String rowToStr(byte[] row) {
        return Bytes.toStringBinary(row);
      }

      @Override
      public String separator() {
        return ",";
      }

      @Override
      public String toString() {
        return this.getClass().getSimpleName() + " [" + rowToStr(firstRow())
            + "," + rowToStr(lastRow()) + "]";
      }
    }
    
    
	public static byte[][] splitIPTables(int numRegions) {
		byte[] firstRowBytes;
		byte[] lastRowBytes;
		InetAddress startAddress = null;
		InetAddress endAddress = null;
		try {
			startAddress = InetAddress.getByName("0.0.0.0");
			endAddress = InetAddress.getByName("255.255.255.255");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		firstRowBytes = startAddress.getAddress();
		lastRowBytes = endAddress.getAddress();

		Preconditions.checkArgument(
				Bytes.compareTo(lastRowBytes, firstRowBytes) > 0,
				"last row (%s) is configured less than first row (%s)",
				Bytes.toStringBinary(lastRowBytes),
				Bytes.toStringBinary(firstRowBytes));

		byte[][] splits = Bytes.split(firstRowBytes, lastRowBytes,
				numRegions - 1);
		Preconditions.checkState(splits != null, "Could not split region with given user input: " );

		// remove endpoints, which are included in the splits list
		return Arrays.copyOfRange(splits, 1, splits.length - 1);
	}
	
	/**
	 * 
	 * 
64
76
86
120
161
162
165
190.118.0
190.118.125
190.119.0
190.119.125
190.200.
192.0.0.0
199





	 * @return
	 */
	
	public static byte[][] splitIPTablesEmpirical() {
//		byte[] firstRowBytes;
		byte[][] rowBytes = new byte[14][];
		try {
			
			rowBytes[0] =  InetAddress.getByName("64.125.125.125").getAddress();
			rowBytes[1] =  InetAddress.getByName("76.125.125.125").getAddress();
			rowBytes[2] =  InetAddress.getByName("86.125.125.125").getAddress();
			rowBytes[3] =  InetAddress.getByName("120.125.125.125").getAddress();
			rowBytes[4] =  InetAddress.getByName("161.125.125.125").getAddress();
			rowBytes[5] =  InetAddress.getByName("162.125.125.125").getAddress();
			rowBytes[6] =  InetAddress.getByName("165.125.125.125").getAddress();
			rowBytes[7] =  InetAddress.getByName("190.118.0.0").getAddress();
			rowBytes[8] =  InetAddress.getByName("190.118.125.125").getAddress();
			rowBytes[9] =  InetAddress.getByName("190.119.0.0").getAddress();
			rowBytes[10] =  InetAddress.getByName("190.119.125.125").getAddress();
			rowBytes[11] =  InetAddress.getByName("190.200.0.0").getAddress();
			rowBytes[12] =  InetAddress.getByName("192.125.125.125").getAddress();
			rowBytes[13] =  InetAddress.getByName("199.125.125.125").getAddress();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		// remove endpoints, which are included in the splits list
		return rowBytes;
	}	
	
	public static byte[][] splitPortTables(int numRegions) {
		// positives are used only, so we put the lower bound to 0
		byte[] firstRowBytes = Bytes.toBytes((int) 0);
		byte[] lastRowBytes = Bytes.toBytes((int) 65536);

		Preconditions.checkArgument(
				Bytes.compareTo(lastRowBytes, firstRowBytes) > 0,
				"last row (%s) is configured less than first row (%s)",
				Bytes.toStringBinary(lastRowBytes),
				Bytes.toStringBinary(firstRowBytes));

		byte[][] splits = Bytes.split(firstRowBytes, lastRowBytes,
				numRegions - 1);
		Preconditions.checkState(splits != null,
				"Could not split region with given user input: " );

		// remove endpoints, which are included in the splits list
		return Arrays.copyOfRange(splits, 1, splits.length - 1);
	}
    
	
	/**
	 * 
23
53
80
80
443
443
993
1935
3389
9001
9001
51413
57000
63000


	 * @return
	 */
	
	public static byte[][] splitPortTablesEmpirical() {
		// positives are used only, so we put the lower bound to 0
		// WL: well-known: System+User ports 
		// System Ports (0-1023), User Ports (1024-49151)

		byte[] ipSplitter = null;
		try {
			ipSplitter = InetAddress.getByName("160.125.125.125").getAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		byte[][] rowBytes = new byte[14][];
		rowBytes[0] = Bytes.toBytes((int) 23);
		rowBytes[1] = Bytes.toBytes((int) 53);
		rowBytes[2] = Bytes.toBytes((int) 80);
		rowBytes[3] = Bytes.add(Bytes.toBytes((int) 80), ipSplitter);
		rowBytes[4] = Bytes.toBytes((int) 443);
		rowBytes[5] = Bytes.add(Bytes.toBytes((int) 443), ipSplitter);
		rowBytes[6] = Bytes.toBytes((int) 993);
		rowBytes[7] = Bytes.toBytes((int) 1935);
		rowBytes[8] = Bytes.toBytes((int) 3389);
		rowBytes[9] = Bytes.toBytes((int) 9001);
		rowBytes[10] = Bytes.add(Bytes.toBytes((int) 9001), ipSplitter);
		rowBytes[11] = Bytes.toBytes((int) 51413);
		rowBytes[12] = Bytes.toBytes((int) 57000);
		rowBytes[13] = Bytes.toBytes((int) 63000);

		// remove endpoints, which are included in the splits list
		return rowBytes;
	}
	
	public static byte[][] splitPortTablesEmpirical1() {
		// positives are used only, so we put the lower bound to 0
		// WL: well-known: System+User ports 
		// System Ports (0-1023), User Ports (1024-49151)
		byte[] firstRowBytesWL = Bytes.toBytes((int) 0);
		byte[] lastRowBytesWL = Bytes.toBytes((int) 4500);


		byte[][] splitsWL = Bytes.split(firstRowBytesWL, lastRowBytesWL,	8 - 1);
		Preconditions.checkState(splitsWL != null, "Could not split region with given user input: " );

		// DP: Dynamic/Private ports
		byte[] firstRowBytesDP = Bytes.toBytes((int) 48000);
		byte[] lastRowBytesDP = Bytes.toBytes((int) 65536);


		byte[][] splitsDP = Bytes.split(firstRowBytesDP, lastRowBytesDP,	6 - 1);
		
		byte[][] splits = new byte[splitsWL.length + splitsDP.length][];
		for (int i = 0; i < splitsWL.length; i++) {
			splits[i] = splitsWL[i];
		}
		for (int i = 0; i < splitsDP.length; i++) {
			splits[splitsWL.length + i] = splitsDP[i];
		}
		// remove endpoints, which are included in the splits list
		return Arrays.copyOfRange(splits, 1, splits.length - 1);
	}
    
	private static boolean doesTableExist(String tableName) throws IOException {
		return hbaseAdmin.tableExists(tableName.getBytes());
	}
	
	private static void createTestTable(Configuration conf, String tableName, boolean dropIfExist)
			throws IOException {

		if (doesTableExist(tableName)){
			if (!dropIfExist){
				return;
			}else{
				hbaseAdmin.disableTable(tableName);
				hbaseAdmin.deleteTable(tableName);
			}
		}

		HTableDescriptor htd = new HTableDescriptor(tableName.getBytes());
		HColumnDescriptor hcd = new HColumnDescriptor(NetFlowCSVParser.COLUMN_FAMILY_NAME.getBytes());
		hcd.setMaxVersions(1);
		hcd.setBlockCacheEnabled(false);
		
		hcd.setBloomFilterType(BloomType.ROW);

		// compression
		hcd.setCompressionType(Algorithm.LZO);
		
		// deferred log flush
		htd.setDeferredLogFlush(true);
		
		// pre-splitting regions  
		// region size 

		htd.addFamily(hcd);
		if (tableName.equalsIgnoreCase("IPTable")){
			hbaseAdmin.createTable(htd, splitIPTables(15));
		} else if (tableName.equalsIgnoreCase("PortTable")){
			hbaseAdmin.createTable(htd, splitPortTables(15));
		}
	}
	
	private static void regionStatus(){
		HTable table;
		try {
			table = new HTable("T1");
			byte[][] startKeys = table.getStartKeys();
			Map<HRegionInfo, HServerAddress> regions = table.getRegionsInfo();
			HRegionInfo regionInfo = null;

			for (int i = 0; i < startKeys.length; i++) {
			}

			ArrayList<RegionLoad> totalRegionLoads = new ArrayList<RegionLoad>();
			ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
			ArrayList<HServerInfo> servers =  new ArrayList<HServerInfo>(clusterStatus.getServerInfo());
			for (HServerInfo hServerInfo : servers) {
				HServerLoad serverLoad = hServerInfo.getLoad();
				ArrayList<RegionLoad> regionLoads = new ArrayList<RegionLoad>(serverLoad.getRegionsLoad());
				totalRegionLoads.addAll(regionLoads);
//				for (RegionLoad regionLoad : regionLoads) {
//					System.out.println(regionLoad.getNameAsString());
//					System.out.println(regionLoad.getStorefileSizeMB());
//					System.out.println("----");
//					regionLoad.
//				}
			}
			
			String[] tables = {"T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8"};
			byte[][] ipStartKeys = new byte[15][]; 
			ipStartKeys[0] = new byte[0];
//			System.arraycopy(splitIPTables(15), 0, ipStartKeys, 1, 14);
			System.arraycopy(splitIPTablesEmpirical(), 0, ipStartKeys, 1, 14);
			
			byte[][] portStartKeys = new byte[15][]; 
			portStartKeys[0] = new byte[0];
//			System.arraycopy(splitPortTables(15), 0, portStartKeys, 1, 14);
			System.arraycopy(splitPortTablesEmpirical(), 0, portStartKeys, 1, 14);
			
			int count = 0;
			String latex = "";
			for (int i = 0; i < 4; i++) {
				count = 0;
				System.out.println();
				for (int j = 0; j < ipStartKeys.length; j++) {
					HRegionLocation hRegionLocation =  hbaseAdmin.getConnection().getRegionLocation(Bytes.toBytes(tables[i]), ipStartKeys[j], false);
					HRegionInfo hRegionInfo = hRegionLocation.getRegionInfo();
					hRegionInfo.getStartKey();

					String startKey = "";
					if(j != 0) startKey = InetAddress.getByAddress(ipStartKeys[j]).getHostAddress();
//					System.out.println("Analyzing Table: " + tables[i] + ", Start key:" + startKey);

					for (RegionLoad regionLoad: totalRegionLoads) {
						if (!regionLoad.getNameAsString().startsWith("T")) continue;
//						System.out.println(regionLoad.getNameAsString());
//						System.out.println(hRegionInfo.getRegionNameAsString());
						if (Bytes.equals(regionLoad.getName(), hRegionInfo.getRegionName())) {
							count++;
							System.out.println("Region" + count + ": " + ", Start key:" + startKey + " " + hRegionInfo.getRegionNameAsString() + ", Table: " + hRegionInfo.getTableNameAsString() + ", Size: " + regionLoad.getStorefileSizeMB() + ", StoreFiles: " + regionLoad.getStorefiles());
//							System.out.println(startKey + " " + hRegionInfo.getTableNameAsString() + " " + regionLoad.getStorefileSizeMB() + "-" + regionLoad.getStorefiles());
							if (count == 1 ) latex = startKey  + " & " + regionLoad.getStorefileSizeMB() + "-" + regionLoad.getStorefiles();
							else latex = latex + " & " + regionLoad.getStorefileSizeMB() + "-" + regionLoad.getStorefiles();
							
						}
					}
				}
				System.out.println(latex);
			}
			
			for (int i = 4; i < 8; i++) {
				count = 0;
				System.out.println();
				for (int j = 0; j < portStartKeys.length; j++) {
					HRegionLocation hRegionLocation =  hbaseAdmin.getConnection().getRegionLocation(Bytes.toBytes(tables[i]), portStartKeys[j], false);
					HRegionInfo hRegionInfo = hRegionLocation.getRegionInfo();
					hRegionInfo.getStartKey();

					String startKey = "";
					if(j != 0) startKey = Bytes.toInt(portStartKeys[j])+"";
//					System.out.println("Analyzing Table: " + tables[i] + ", Start key:" + startKey);

					for (RegionLoad regionLoad: totalRegionLoads) {
						if (!regionLoad.getNameAsString().startsWith("T")) continue;
//						System.out.println(regionLoad.getNameAsString());
//						System.out.println(hRegionInfo.getRegionNameAsString());
						if (Bytes.equals(regionLoad.getName(), hRegionInfo.getRegionName())) {
							count++;
							System.out.println("Region" + count + ": " + ", Start key:" + startKey + " " + hRegionInfo.getRegionNameAsString() + ", Table: " + hRegionInfo.getTableNameAsString() + ", Size: " + regionLoad.getStorefileSizeMB() + ", StoreFiles: " + regionLoad.getStorefiles());
//							System.out.println(startKey + " " + hRegionInfo.getTableNameAsString() + " " + regionLoad.getStorefileSizeMB() + "-" + regionLoad.getStorefiles());
							if (count == 1 ) latex = startKey  + " & " + regionLoad.getStorefileSizeMB() + "-" + regionLoad.getStorefiles();
							else latex = latex + " & " + regionLoad.getStorefileSizeMB() + "-" + regionLoad.getStorefiles();
							
						}
					}
				}
				System.out.println(latex);
			}
			
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void splitPrinter(){
		byte[][] splits = splitIPTables(15);
		byte[][] port_splits = splitPortTables(15);
		for (int i = 0; i < splits.length; i++) {
			System.out.print(i+2);
			System.out.print(" & ");
			try {
				System.out.print(InetAddress.getByAddress(splits[i]).getHostAddress());
				System.out.print(" & ");
				System.out.print(Bytes.toInt(port_splits[i]));
				System.out.print(" \\\\ \\hline ");
//			System.out.println(splits[i]);
				System.out.println("");
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		try {
			hbaseAdmin = new HBaseAdmin(conf);
//			createTestTable(conf, "IPTable", true);
//			createTestTable(conf, "PortTable", true);
			regionStatus();
//			byte[][] port_splits = splitPortTablesEmpirical();
//			for (int i = 0; i < port_splits.length; i++) {
//				System.out.println(i);
//				System.out.println(Bytes.toInt(port_splits[i]));
//			}
		
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
