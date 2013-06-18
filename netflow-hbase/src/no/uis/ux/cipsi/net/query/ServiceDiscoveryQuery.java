package no.uis.ux.cipsi.net.query;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import no.uis.ux.cipsi.NetFlowCSVParser;
import no.uis.ux.cipsi.Utils;
import no.uis.ux.cipsi.net.query.SrcDstTrafficQuery.SrcDstTrafficMapper;
import no.uis.ux.cipsi.net.query.SrcDstTrafficQuery.SrcDstTrafficReducer;

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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ServiceDiscoveryQuery {

	static class ServiceDiscoveryMapper  extends TableMapper<ImmutableBytesWritable, Put> {
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			Date ts = NetFlowCSVParser.getDecodedFirstSeenRowKey(key.get(), 5);
			
			String src = NetFlowCSVParser.getDecodedSrcIPRowKey(key.get(), 5);
			String srcPort = NetFlowCSVParser.getDecodedSrcPortRowKey(key.get(), 5);
			String dst = NetFlowCSVParser.getDecodedDstIPRowKey(key.get(), 5);
			String dstPort = NetFlowCSVParser.getDecodedDstPortRowKey(key.get(), 5);
			

		}
	}
	
	static class ServiceDiscoveryReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		
	}
	
	private static Job createSubmittableJob(Configuration conf, String output,
			String servicePort, long startTime, long endTime) throws IOException {
		Job job = new Job(conf, "Service Discovery Query: " + servicePort );
		job.setJarByClass(ServiceDiscoveryQuery.class);
		String tableName = "T5";
		
		Scan scan = new Scan();		// Specify filters 
		scan.setCaching(500);       // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		
		// Columns
//		scan.addColumn("d".getBytes(), Utils.getColumnQualifier("In Byte"));
//		scan.addColumn("d".getBytes(), Utils.getColumnQualifier("Out Byte"));
		
		// Rowkey filters
		// NOTE: reverse order of timestamps, due to the reverse order of records in hbase
		byte[] rowkeyStart = Bytes.toBytes(Integer.parseInt(servicePort));
		byte[] rowkeyEnd = Bytes.toBytes(Integer.parseInt(servicePort) + 1);
		
		scan.setStartRow(rowkeyStart);
		scan.setStopRow(rowkeyEnd);
		
		TableMapReduceUtil.initTableMapperJob(
				  tableName,					// input HBase table name
				  scan,           				// Scan instance to control CF and attribute selection
				  ServiceDiscoveryMapper.class,   	// mapper
				  Text.class,             // mapper output key
				  LongWritable.class,             // mapper output value
				  job);
		
//		job.setReducerClass(ServiceDiscoveryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));  // adjust directories as required
		
		job.setNumReduceTasks(0);   // is not required
		
		return job;
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ParseException {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 3) {
	      System.err.println("Wrong number of arguments: " + otherArgs.length);
	      System.exit(-1);
	    }
	    
	    String servicePort = args[0];
	    System.out.println(servicePort);
	    String ts1 = args[1];
	    System.out.println(ts1);
	    String ts2 = args[2];
	    System.out.println(ts2);
	    
	    String output = "/netflow-query/service-discovery-"+servicePort;
	    
	    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	    long startTime = dateFormat.parse(ts1).getTime();
	    long endTime = dateFormat.parse(ts2).getTime();
	    
	    Job job = createSubmittableJob(conf, output, servicePort, startTime, endTime);
	    
	    boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	    
	}

	
}
