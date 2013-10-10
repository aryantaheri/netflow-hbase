package no.uis.ux.cipsi.net.query;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import no.uis.ux.cipsi.NetFlowCSVParser;
import no.uis.ux.cipsi.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SrcDstTrafficQuery {

	static class SrcDstTrafficMapper  extends TableMapper<ImmutableBytesWritable, Put> {
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {

			String src = NetFlowCSVParser.getDecodedSrcIPRowKey(key.get(), 1);
			String srcPort = NetFlowCSVParser.getDecodedSrcPortRowKey(key.get(), 1);
			String dst = NetFlowCSVParser.getDecodedDstIPRowKey(key.get(), 1);
			String dstPort = NetFlowCSVParser.getDecodedDstPortRowKey(key.get(), 1);
			
			String ipPortPair = src + ":" + srcPort + ":" + dst + ":" + dstPort;
//			String ipPortReversePair = dst + ":" + dstPort + ":" + src + ":" + srcPort;
			
			byte[] ib = value.getValue("d".getBytes(), Utils.getColumnQualifier("In Byte"));
			long ibyte = 0l;
			if (ib != null && ib.length != 0){
				ibyte = Bytes.toLong(ib);
			}
			
			byte[] ob = value.getValue("d".getBytes(), Utils.getColumnQualifier("Out Byte"));
			long obyte = 0l;
			if ( ob != null && ob.length != 0){
				obyte = Bytes.toLong(ob);
			}

			long total = ibyte + obyte;
//			System.out.println("Map: " + ipPortPair + ": " + total);
			context.write(new Text(ipPortPair), new LongWritable(total)); 
			
		}
	}
	
	static class SrcDstTrafficReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		protected void reduce(Text key, Iterable<LongWritable> values,
				org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			long totalTraffic = 0l;
			for (LongWritable value : values) {
				totalTraffic += value.get();
			}
			context.write(key, new LongWritable(totalTraffic));
		}
	}
	
	private static Job createSubmittableJob(Configuration conf, String output, String src, String srcPort,
			String dst, String dstPort, long startTime, long endTime) throws IOException {
		Job job = new Job(conf, "Source Destination Traffice Query" );
		job.setJarByClass(SrcDstTrafficQuery.class);
		String tableName = "T1";
		
		Scan scan = new Scan();		// Specify filters e.g. for TopN: obyt+ibyt
		scan.setCaching(500);       // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		
		// Columns
		scan.addColumn("d".getBytes(), Utils.getColumnQualifier("In Byte"));
		scan.addColumn("d".getBytes(), Utils.getColumnQualifier("Out Byte"));
		
		// Rowkey filters
		// NOTE: reverse order of timestamps, due to the reverse order of records in hbase
		byte[] rowkeyStart = NetFlowCSVParser.prepareRowKeyT1Simple(src, srcPort, dst, dstPort, endTime);
		byte[] rowkeyEnd = NetFlowCSVParser.prepareRowKeyT1Simple(src, srcPort, dst, dstPort, startTime);
		
		scan.setStartRow(rowkeyStart);
		scan.setStopRow(rowkeyEnd);
		
		TableMapReduceUtil.initTableMapperJob(
				  tableName,					// input HBase table name
				  scan,           				// Scan instance to control CF and attribute selection
				  SrcDstTrafficMapper.class,   	// mapper
				  Text.class,             // mapper output key
				  LongWritable.class,             // mapper output value
				  job);
		
		job.setReducerClass(SrcDstTrafficReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(output));  // adjust directories as required
		
//		job.setNumReduceTasks(1);   // at least one, adjust as required
		
		return job;
	}
	
	
	
	public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 6) {
	      System.err.println("Wrong number of arguments: " + otherArgs.length);
	      System.exit(-1);
	    }
	    String src = otherArgs[0];
//	    System.out.println(src);
	    String srcPort = otherArgs[1];
//	    System.out.println(srcPort);
	    String dst = otherArgs[2];
//	    System.out.println(dst);
	    String dstPort = otherArgs[3];
//	    System.out.println(dstPort);
	    String ts1 = otherArgs[4];
//	    System.out.println(ts1);
	    String ts2 = otherArgs[5];
//	    System.out.println(ts2);
	    
	    String output = "/netflow-query/src-dst-traffic";
	    
	    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    long startTime = dateFormat.parse(ts1).getTime();
	    long endTime = dateFormat.parse(ts2).getTime();
	    
	    Job job = createSubmittableJob(conf, output, src, srcPort, dst, dstPort, startTime, endTime);
	    
	    boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}


}
