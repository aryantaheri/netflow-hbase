package no.uis.ux.cipsi.net.query;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;

import no.uis.ux.cipsi.NetFlowCSVParser;
import no.uis.ux.cipsi.Utils;
import no.uis.ux.cipsi.net.query.TopNMapR.TopNMapper;
import no.uis.ux.cipsi.net.query.TopNMapR.TopNReducer;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NetFlowIPQuery {

	static class NetFlowTopNIPMapper extends TableMapper<ImmutableBytesWritable, Put> {
		static NetFlowCSVParser parser;
		private final HashMap<String, Long> srcDstTraffic = new HashMap<String, Long>();
		private final int N = 10;
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
//			super.setup(context);
			parser = NetFlowCSVParser.getInstance();
		}
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
//			super.map(key, value, context);
			String src = NetFlowCSVParser.getDecodedSrcIPRowKey(key.get(), 1);
			String dst = NetFlowCSVParser.getDecodedDstIPRowKey(key.get(), 1);
			String ipPair = src+":"+dst;
			String ipReversePair = dst+":"+src;
//			System.out.println("Key: " + key);
//			System.out.println("IP Pair: " + ipPair);
//			System.out.println("Value: " + value);
			
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
			
			if (srcDstTraffic.containsKey(ipPair)){
				srcDstTraffic.put(ipPair, srcDstTraffic.get(ipPair) + ibyte + obyte);		
			} else if (srcDstTraffic.containsKey(ipReversePair)){
				srcDstTraffic.put(ipReversePair, srcDstTraffic.get(ipReversePair) + ibyte + obyte);
			} else {
				srcDstTraffic.put(ipPair, ibyte + obyte);
			}
			

			
			
		}
		
		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			
			Set<String> keySet = srcDstTraffic.keySet();
			String[] keyArray = keySet.toArray(new String[keySet.size()]);
			Arrays.sort(keyArray, new Comparator<String>(){
				@Override
			    public int compare(String o1, String o2) {
			      // sort descending
//			      return Long.compare(srcDstTraffic.get(o2), srcDstTraffic.get(o1));
			      return Long.valueOf(srcDstTraffic.get(o2)).compareTo(srcDstTraffic.get(o1));
			    }
			});

			for (int i = 0; i < Math.min(keyArray.length, N); i++) {
//				System.out.print(keyArray[i] + " ");
//				System.out.println(srcDstTraffic.get(keyArray[i]));
				context.write(new Text(keyArray[i]), new LongWritable(srcDstTraffic.get(keyArray[i])));
			}
		}
	}
	
	static class NetFlowTopNIPReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		
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
	
	
	public static void topNIP(Configuration conf, Path intermediatePath) {
		try {
			Job job = new Job(conf, "Top N Talkers" );
			job.setJarByClass(NetFlowIPQuery.class);
			
			String tableName = "T1";
			
			Scan scan = new Scan();		// Specify filters e.g. for TopN: obyt+ibyt
			scan.setCaching(500);       // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false); // don't set to true for MR jobs
			
			scan.addColumn("d".getBytes(), Utils.getColumnQualifier("In Byte"));
			scan.addColumn("d".getBytes(), Utils.getColumnQualifier("Out Byte"));
			
			TableMapReduceUtil.initTableMapperJob(
					  tableName,					// input HBase table name
					  scan,           				// Scan instance to control CF and attribute selection
					  NetFlowTopNIPMapper.class,   	// mapper
					  Text.class,             // mapper output key
					  LongWritable.class,             // mapper output value
					  job);
			
			job.setReducerClass(NetFlowTopNIPReducer.class);
			FileOutputFormat.setOutputPath(job, intermediatePath);  // adjust directories as required
			
//			job.setNumReduceTasks(1);   // at least one, adjust as required
			
			
			boolean b = job.waitForCompletion(true);
			if (!b) {
				throw new IOException("error with job!");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
	
	public static void topN(Configuration conf, Path intermadiatePath, Path outputPath){
		try {
			Job job = new Job(conf, "Top N finder from text files" );
			job.setJarByClass(TopNMapR.class);
			
			job.setMapperClass(TopNMapper.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setReducerClass(TopNReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
//			job.setOutputKeyClass(LongWritable.class);
//			job.setOutputValueClass(Text.class);
			
			// FIXME
			job.setNumReduceTasks(1);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job, intermadiatePath.suffix("/part*"));
			FileOutputFormat.setOutputPath(job, outputPath);

			boolean b = job.waitForCompletion(true);
			if (!b) {
				throw new IOException("error with job!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		Path topNIntermediatePath = new Path("/netflow-query/topNIP/"); 
		Path topNOutPutPath = new Path("/netflow-query/topNIPOut/");
		NetFlowIPQuery.topNIP(conf, topNIntermediatePath);
		NetFlowIPQuery.topN(conf, topNIntermediatePath, topNOutPutPath);
		
	}
}
