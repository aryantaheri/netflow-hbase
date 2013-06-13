package no.uis.ux.cipsi.net.query;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.thirdparty.guava.common.base.Splitter;
import org.apache.hadoop.thirdparty.guava.common.collect.Iterators;

public class TopNMapR {

	static class TopNMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private final HashMap<String, Long> keyValues = new HashMap<String, Long>();
		private final int N = 10;
		
		@Override
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			Iterator<String> tokens = Splitter.onPattern("\\s+").trimResults().split(value.toString()).iterator();
			String ipPair = tokens.next();
			String trafficString = tokens.next();
//			System.out.println(ipPair + "->" + trafficString);
			Long traffic = Long.parseLong(trafficString);
			keyValues.put(ipPair, traffic);
		}

		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			
			Set<String> keySet = keyValues.keySet();
			String[] keyArray = keySet.toArray(new String[keySet.size()]);
			Arrays.sort(keyArray, new Comparator<String>(){
				@Override
			    public int compare(String o1, String o2) {
			      // sort descending
//			      return Long.compare(srcDstTraffic.get(o2), srcDstTraffic.get(o1));
			      return Long.valueOf(keyValues.get(o2)).compareTo(keyValues.get(o1));
			    }
			});

			for (int i = 0; i < Math.min(keyArray.length, N); i++) {
				context.write(new LongWritable(keyValues.get(keyArray[i])), new Text(keyArray[i]));
			}
		}
		
	}
	
	static class TopNReducer extends Reducer<LongWritable, Text, Text, LongWritable>{
		private final HashMap<String, Long> keyValues = new HashMap<String, Long>();
		private final int N = 10;
		
		protected void reduce(LongWritable key, Iterable<Text> values,
				org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			for (Text text : values) {
				keyValues.put(text.toString(), new Long(key.get()));
			}
		}
		
		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			Set<String> keySet = keyValues.keySet();
			String[] keyArray = keySet.toArray(new String[keySet.size()]);
			Arrays.sort(keyArray, new Comparator<String>(){
				@Override
			    public int compare(String o1, String o2) {
			      // sort descending
//			      return Long.compare(srcDstTraffic.get(o2), srcDstTraffic.get(o1));
			      return Long.valueOf(keyValues.get(o2)).compareTo(keyValues.get(o1));
			    }
			});

			for (int i = 0; i < Math.min(keyArray.length, N); i++) {
				context.write(new Text(keyArray[i]), new LongWritable(keyValues.get(keyArray[i])));
			}
			
		}
	}
	
	
}
