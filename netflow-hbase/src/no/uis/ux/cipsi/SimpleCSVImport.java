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
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
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

	static class NetFlowT1ImportMapper extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {
		private NetFlowCSVParser netFlowCSVParser = null;


		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			netFlowCSVParser = new NetFlowCSVParser(
					"Date flow start,Date flow end,Duration,Src IP Addr,"
							+ "Dst IP Addr, Src Pt, Dst Pt ,Proto  ,Flags ,Fwd ,STos   ,In Pkt  ,In Byte  ,Out Pkt ,Out Byte  ,Input ,"
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
				NetFlowCSVParser.printRowKeyT1(rowkeyT1);

				Put putT1 = new Put(rowkeyT1);
				// Prepare T1 values
				List<CFQValue> values = netFlowCSVParser
						.prepareValuesT1(fields);
				for (CFQValue cfqValue : values) {
					System.out.println(NetFlowV5Record
							.decodeCFQValues(cfqValue));
					putT1.add(cfqValue.getColumnFamily(),
							cfqValue.getQualifier(), cfqValue.getValue());
				}
				putT1.setWriteToWAL(false);

				// Prepare T2 key
				byte[] rowkeyT2 = netFlowCSVParser.prepareRowKeyT2(fields);
				NetFlowCSVParser.printRowKeyT2(rowkeyT2);

				Put putT2 = new Put(rowkeyT2);
				// Prepare T2 values
				List<CFQValue> valuesT2 = netFlowCSVParser
						.prepareValuesT2(fields);
				for (CFQValue cfqValue : valuesT2) {
					// System.out.println(NetFlowV5Record.decodeCFQValues(cfqValue));
					putT2.add(cfqValue.getColumnFamily(),
							cfqValue.getQualifier(), cfqValue.getValue());
				}
				putT2.setWriteToWAL(false);

				// Single table
				// context.write(new ImmutableBytesWritable(rowkeyT1), put);

				// Multiple tables
				context.write(
						new ImmutableBytesWritable(T1_TABLE_NAME.getBytes()),
						putT1);
				context.write(
						new ImmutableBytesWritable(T2_TABLE_NAME.getBytes()),
						putT2);

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	private static boolean doesTableExist(String tableName) throws IOException {
		return hbaseAdmin.tableExists(tableName.getBytes());
	}

	private static void createTable(Configuration conf, String tableName)
			throws IOException {

		if (doesTableExist(tableName))
			return;

		HTableDescriptor htd = new HTableDescriptor(tableName.getBytes());
		HColumnDescriptor hcd = new HColumnDescriptor(NetFlowCSVParser.COLUMN_FAMILY_NAME.getBytes());
		hcd.setMaxVersions(1);
		hcd.setBlockCacheEnabled(false);
		htd.addFamily(hcd);
		hbaseAdmin.createTable(htd);
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		String input = "data/csv/simple.csv";
//		String column = "";

		// conf.set("conf.column", column);
		Job job = new Job(conf, "Import NetFlow records from file " + input + " into tables: " + T1_TABLE_NAME + " and " + T2_TABLE_NAME);
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
		createTable(conf, T1_TABLE_NAME);
		createTable(conf, T2_TABLE_NAME);
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
