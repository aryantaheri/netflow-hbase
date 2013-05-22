package no.uis.ux.cipsi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class SimpleCSVImport {

	private static final Logger ImportLOG = Logger
			.getLogger(SimpleCSVImport.class);
	private static HBaseAdmin hbaseAdmin;
	private static String T1_TABLE_NAME = "T1";
	private static String T2_TABLE_NAME = "T2";
	
	private static String T3_TABLE_NAME = "T3";
	private static String T4_TABLE_NAME = "T4";
	
	private static String T5_TABLE_NAME = "T5";
	private static String T6_TABLE_NAME = "T6";
	
	private static String T7_TABLE_NAME = "T7";
	private static String T8_TABLE_NAME = "T8";
	
//	private static String T1_TABLE_NAME = "NST1";
//	private static String T2_TABLE_NAME = "NST2";
//	
//	private static String T3_TABLE_NAME = "NST3";
//	private static String T4_TABLE_NAME = "NST4";
//	
//	private static String T5_TABLE_NAME = "NST5";
//	private static String T6_TABLE_NAME = "NST6";
//	
//	private static String T7_TABLE_NAME = "NST7";
//	private static String T8_TABLE_NAME = "NST8";

	private static final int NUMBER_OF_REGIONS = 15;
	
	static class NetFlowT1ImportMapper extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {
		private NetFlowCSVParser netFlowCSVParser = null;


		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			netFlowCSVParser = new NetFlowCSVParser(
					"Date flow start,Date flow end,Duration,Src IP Addr,"
							+ "Dst IP Addr, Src Pt, Dst Pt ,Proto  ,Flags ,Fwd ,STos   ,In Pkt  ,In Byte  ,Out Pkt ,Out Byte  ,Flows ,Input ,"
							+ "Output ,Src AS ,Dst AS ,SMask ,DMask ,DTos ,Dir      ,Next-hop IP  ,BGP next-hop IP ,SVlan ,DVlan   ,"
							+ "In src MAC Addr  ,Out dst MAC Addr   ,In dst MAC Addr  ,Out src MAC Addr  , Router IP, MPLS lbl 1   ,MPLS lbl 2   ,"
							+ "MPLS lbl 3   ,MPLS lbl 4", false);
			
		}

		@Override
		protected void map(LongWritable key, Text line, Context context) {
			try {
				ArrayList<String> fields = Lists.newArrayList(Splitter.on(",")
						.trimResults().split(line.toString()));

				// Prepare T1 key
				byte[] rowkeyT1 = netFlowCSVParser.prepareRowKeyT1(fields);
//				NetFlowCSVParser.printRowKeyT1(rowkeyT1);
//				System.out.println(NetFlowCSVParser.decodeRowKey(rowkeyT1, 1));

				Put putT1 = new Put(rowkeyT1);
				// Prepare T1 values
				List<CFQValue> values = netFlowCSVParser
						.prepareValuesT1(fields);
				for (CFQValue cfqValue : values) {
//					System.out.println(NetFlowV5Record.decodeCFQValues(cfqValue));
					putT1.add(cfqValue.getColumnFamily(),
							cfqValue.getQualifier(), cfqValue.getValue());
				}
				putT1.setWriteToWAL(false);
				

				// Prepare T2 key
				byte[] rowkeyT2 = netFlowCSVParser.prepareRowKeyT2(fields);
//				System.out.println(NetFlowCSVParser.decodeRowKey(rowkeyT2, 2));

				// Prepare T2 values
				Put putT2 = new Put(rowkeyT2);
				List<CFQValue> valuesT2 = netFlowCSVParser.prepareDummyValue();
				for (CFQValue cfqValue : valuesT2) {
					putT2.add(cfqValue.getColumnFamily(), cfqValue.getQualifier(), cfqValue.getValue());
				}
				putT2.setWriteToWAL(false);

				// Prepare T3 key
				byte[] rowkeyT3 = netFlowCSVParser.prepareRowKeyT3(fields);
//				System.out.println(NetFlowCSVParser.decodeRowKey(rowkeyT3, 3));
				Put putT3 = netFlowCSVParser.getDummyPut(rowkeyT3);
				putT3.setWriteToWAL(false);				

				// Prepare T4 key
				byte[] rowkeyT4 = netFlowCSVParser.prepareRowKeyT4(fields);
//				System.out.println(NetFlowCSVParser.decodeRowKey(rowkeyT4, 4));
				Put putT4 = netFlowCSVParser.getDummyPut(rowkeyT4);
				putT4.setWriteToWAL(false);		
				
				// Prepare T5 key
				byte[] rowkeyT5 = netFlowCSVParser.prepareRowKeyT5(fields);
//				System.out.println(NetFlowCSVParser.decodeRowKey(rowkeyT5, 5));
				Put putT5 = netFlowCSVParser.getDummyPut(rowkeyT5);
				putT5.setWriteToWAL(false);		
				
				// Prepare T6 key
				byte[] rowkeyT6 = netFlowCSVParser.prepareRowKeyT6(fields);
//				System.out.println(NetFlowCSVParser.decodeRowKey(rowkeyT6, 6));
				Put putT6 = netFlowCSVParser.getDummyPut(rowkeyT6);
				putT6.setWriteToWAL(false);		
				
				// Prepare T7 key
				byte[] rowkeyT7 = netFlowCSVParser.prepareRowKeyT7(fields);
//				System.out.println(NetFlowCSVParser.decodeRowKey(rowkeyT7, 7));
				Put putT7 = netFlowCSVParser.getDummyPut(rowkeyT7);
				putT7.setWriteToWAL(false);		

				// Prepare T8 key
				byte[] rowkeyT8 = netFlowCSVParser.prepareRowKeyT8(fields);
//				System.out.println(NetFlowCSVParser.decodeRowKey(rowkeyT8, 8));
				Put putT8 = netFlowCSVParser.getDummyPut(rowkeyT8);
				putT8.setWriteToWAL(false);

				// Single table
				// context.write(new ImmutableBytesWritable(rowkeyT1), put);

				// Multiple tables
				context.write(
						new ImmutableBytesWritable(T1_TABLE_NAME.getBytes()),
						putT1);
				context.write(
						new ImmutableBytesWritable(T2_TABLE_NAME.getBytes()),
						putT2);
				context.write(
						new ImmutableBytesWritable(T3_TABLE_NAME.getBytes()),
						putT3);
				context.write(
						new ImmutableBytesWritable(T4_TABLE_NAME.getBytes()),
						putT4);
				context.write(
						new ImmutableBytesWritable(T5_TABLE_NAME.getBytes()),
						putT5);
				context.write(
						new ImmutableBytesWritable(T6_TABLE_NAME.getBytes()),
						putT6);
				context.write(
						new ImmutableBytesWritable(T7_TABLE_NAME.getBytes()),
						putT7);
				context.write(
						new ImmutableBytesWritable(T8_TABLE_NAME.getBytes()),
						putT8);



			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		
		
	}
	
	

	private static boolean doesTableExist(String tableName) throws IOException {
		return hbaseAdmin.tableExists(tableName.getBytes());
	}

	private static void createTable(Configuration conf, String tableName, boolean dropIfExist)
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
		
		// bloomfilter
		if (tableName.equals(T1_TABLE_NAME)){
			hcd.setBloomFilterType(BloomType.ROWCOL);
		} else {
			hcd.setBloomFilterType(BloomType.ROW);
		}

		// compression
		hcd.setCompressionType(Algorithm.LZO);
		
		// deferred log flush
		htd.setDeferredLogFlush(true);
		
		// pre-splitting regions 
		byte[][] splits = null;
		if (tableName.equals(T1_TABLE_NAME) || tableName.equals(T2_TABLE_NAME) || 
				tableName.equals(T3_TABLE_NAME) || tableName.equals(T4_TABLE_NAME) ){
//			splits = RegionSplitUtil.splitIPTables(NUMBER_OF_REGIONS);
			// Empirical splitting
			splits = RegionSplitUtil.splitIPTablesEmpirical();
		} else if (tableName.equals(T5_TABLE_NAME) || tableName.equals(T6_TABLE_NAME) || 
				tableName.equals(T7_TABLE_NAME) || tableName.equals(T8_TABLE_NAME) ){
//			splits = RegionSplitUtil.splitPortTables(NUMBER_OF_REGIONS);
			// Empirical splitting
			splits = RegionSplitUtil.splitPortTablesEmpirical();
		}
		
		// region size 

		htd.addFamily(hcd);
		// For No-Splitting Config
//		hbaseAdmin.createTable(htd);
		hbaseAdmin.createTable(htd, splits);
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
//		String input = "data/csv/32.csv";
//		String input = "data/csv/32.csv.gz";
//		String input = "data/csv/oslo_gw.2013.04.10.csv";
//		String input = "data/csv/trd_gw1.2013.04.10.csv.gz";
		String input = "/netflow";
//		conf.set("hbase.zookeeper.quorum", "haisen24.ux.uis.no");
//		String column = "";

		// conf.set("conf.column", column);
		Job job = new Job(conf, "Import All NetFlow records from directory " + input + " into 8 tables" );
		job.setJarByClass(SimpleCSVImport.class);
		job.setMapperClass(NetFlowT1ImportMapper.class);
		// job.setOutputFormatClass(TableOutputFormat.class);
		// job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Writable.class);
		job.setNumReduceTasks(0);

		// MultiTableOutput format
		// this is the key to writing to multiple tables in hbase
		job.setOutputFormatClass(MultiTableOutputFormat.class);

		// HFile output, need a real cluster
		// String hfileOutPath =
		// "/home/aryan/workspace/NetFlowBulkImport/data/csv/hfile.out";
		// job.setReducerClass(PutSortReducer.class);
		// Path outputDir = new Path(hfileOutPath);
		// FileOutputFormat.setOutputPath(job, outputDir);
		// job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		// job.setMapOutputValueClass(Put.class);
		// HFileOutputFormat.configureIncrementalLoad(job, new HTable(conf,
		// table));

		hbaseAdmin = new HBaseAdmin(conf);
		boolean deleteTables = true;
		createTable(conf, T1_TABLE_NAME, deleteTables);
		createTable(conf, T2_TABLE_NAME, deleteTables);
		createTable(conf, T3_TABLE_NAME, deleteTables);
		createTable(conf, T4_TABLE_NAME, deleteTables);
		createTable(conf, T5_TABLE_NAME, deleteTables);
		createTable(conf, T6_TABLE_NAME, deleteTables);
		createTable(conf, T7_TABLE_NAME, deleteTables);
		createTable(conf, T8_TABLE_NAME, deleteTables);
		FileInputFormat.addInputPath(job, new Path(input));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * HTable table = new HTable(conf, tableName);
	 * job.setReducerClass(PutSortReducer.class); Path outputDir = new
	 * Path(hfileOutPath); FileOutputFormat.setOutputPath(job, outputDir);
	 * job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	 * job.setMapOutputValueClass(Put.class);
	 * HFileOutputFormat.configureIncrementalLoad(job, table);
	 */
}
