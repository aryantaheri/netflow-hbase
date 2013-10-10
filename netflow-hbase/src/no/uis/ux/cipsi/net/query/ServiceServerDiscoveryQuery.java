package no.uis.ux.cipsi.net.query;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import no.uis.ux.cipsi.NetFlowCSVParser;
import no.uis.ux.cipsi.Play;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.GenericOptionsParser;

public class ServiceServerDiscoveryQuery {

	private static void hbaseServiceServerDiscovery(Configuration conf,
			int servicePort, InetAddress serviceClient, long startTime,
			long endTime, String tableName) throws IOException {
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();

		// 8 = port(4) + ip(4)
		ByteBuffer rowkey = ByteBuffer.allocate(8);
		byte[] output = new byte[8];

		rowkey.put(Bytes.toBytes(servicePort));
		rowkey.put(serviceClient.getAddress());

		byte[] rowKeyStart = rowkey.array();
//		System.out.println(Play.bytesToBits(rowKeyStart));
		byte[] rowKeyStop = new byte[rowKeyStart.length + 1];
		System.arraycopy(rowKeyStart, 0, rowKeyStop, 0, rowKeyStart.length);
		rowKeyStop[rowKeyStart.length] = (byte) (-1);
//		System.out.println(Play.bytesToBits(rowKeyStop));

		scan.setStartRow(rowKeyStart);
		scan.setStopRow(rowKeyStop);

		int count = 0;
		ResultScanner ss = table.getScanner(scan);
		boolean first = true;
		for (Result r : ss) {
			for (KeyValue kv : r.raw()) {
				if (NetFlowCSVParser.getDecodedFirstSeenRowKey(kv.getRow(), 7).getTime() < endTime &&
						NetFlowCSVParser.getDecodedFirstSeenRowKey(kv.getRow(), 7).getTime() > startTime){
					
//					System.out.println("------------------------------------------------");
					System.out.println(NetFlowCSVParser.decodeRowKey(kv.getRow(), 7));
//					System.out.println("------------------------------------------------");
				}
				// System.out.println(bytesToBits(kv.getRow()));
				// System.out.print(kv.getTimestamp() + " " );
				// System.out.println(NetFlowV5Record.decodeCFQValues(new
				// CFQValue(kv.getFamily(), kv.getQualifier(),
				// kv.getValue())));
				// System.out.println("****************");
			}
			count++;
		}
		System.out.println("Returned results: " + count);
		System.out.println("------------------------------------------------");
		ss.close();

	}
	/**
	 * Heavy: 218.203.219.96:22    ->    161.223.1.218:39311
	 * Light: 161.220.56.181:22    ->    69.232.95.251
	 * sp=22 da=69.73.156.73
	 * @param args
	 * @throws IOException
	 * @throws ParseException
	 */
	public static void main(String[] args) throws IOException, ParseException {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 4) {
			System.err
					.println("Wrong number of arguments: " + otherArgs.length);
			System.exit(-1);
		}
		long qstart = System.currentTimeMillis();
		String servicePort = otherArgs[0];
		System.out.println(servicePort);
		String serviceClient = otherArgs[1];
		System.out.println(serviceClient);
		String ts1 = otherArgs[2];
		System.out.println(ts1);
		String ts2 = otherArgs[3];
		System.out.println(ts2);
		
		String tableName = "T7";
		if (otherArgs.length == 5) {
			tableName = otherArgs[4];
		}
		System.out.println("Table: " + tableName);
		
		

		
		
		int service = Integer.parseInt(servicePort);
		InetAddress client = InetAddress.getByName(serviceClient);

		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		long startTime = dateFormat.parse(ts1).getTime();
		long endTime = dateFormat.parse(ts2).getTime();

		hbaseServiceServerDiscovery(conf, service, client, startTime, endTime, tableName);
		long qend = System.currentTimeMillis();
		System.out.println("Duration: " + (qend - qstart));
	}
}
