package no.uis.ux.cipsi;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import no.uis.ux.cipsi.net.MacAddress;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;

public class NetFlowV5Record {

	// RowKey information should also be added here
	
	/**
	 *  "Date flow start,Date flow end,Duration,Src IP Addr," +
		"Dst IP Addr, Src Pt, Dst Pt ,Proto  ,Flags ,Fwd ,STos   ,In Pkt  ,In Byte  ,Out Pkt ,Out Byte  ,Flows, Input ," +
		"Output ,Src AS ,Dst AS ,SMask ,DMask ,DTos ,Dir      ,Next-hop IP  ,BGP next-hop IP ,SVlan ,DVlan   ," +
		"In src MAC Addr  ,Out dst MAC Addr   ,In dst MAC Addr  ,Out src MAC Addr  ,Router IP,MPLS lbl 1   ,MPLS lbl 2   ," +
		"MPLS lbl 3   ,MPLS lbl 4"
	 */
	
	/**
	long lastSeen;
	short protocol = -1; // 138 by nfdump
	String tcpFlags = 0; // 6 char
	short forwardingStatus = -1;
	byte sourceTOS = 0, destinationTOS = 0;
	long inPacket, outPacket, inByte, outByte, flows // 4 bytes required-->long(8)
	long sourceAS = -1l, destinationAS = -1l,
			nextAS = -1l, previousAS = -1l;				// RFC 4893 introduced 32-bit AS numbers, using long(64) because there is no unsinged int in Java
	InetAddress sourceMask, destinationMask;
	boolean direction = false;
	InetAddress nextHopIP, bGPNextHopIP, routerIP;
	short sourceVID = -1, destinationVID = -1;				//VLAN Identifier (VID): a 12-bit field
	byte[] inSourceMAC, outDestinationMAC, outSourceMAC, inDestinationMAC; 	// MAC-48 and in 100 years EUI-64
	byte[][] mplss = new byte[10][4];						//  MPLS 20-bit label value. 4-bytes in total, 10 labels
	short inIF = -1, outIF = -1;
	 */

	static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	public static List<CFQValue> encodeCFQValues(List<String> fields, List<byte[]> families, List<byte[]> qualifiers) throws Exception{
		if(fields.size() != families.size() || fields.size() != qualifiers.size()) 
			throw new Exception("Number of fields, families, and qualifiers do not match: " + 
					fields.size() + ", " + 
					families.size() + ", " + 
					qualifiers.size());
		List<CFQValue> values = new ArrayList<CFQValue>();
		CFQValue cfqValue = null;
		for (int i = 0; i < qualifiers.size(); i++) {
			switch (Utils.getColumnQualifierString(qualifiers.get(i))) {
			case "Date flow end":
				long lastSeen = dateFormat.parse(fields.get(i)).getTime();
				cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(lastSeen));
				values.add(cfqValue);
				break;

			case "Proto":
				short protocol = Short.parseShort(fields.get(i));
				cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(protocol));
				values.add(cfqValue);
				break;
				
			// FIXME: use 9 bits to handle flags, instead of using a string.
			case "Flags":
				String flags = fields.get(i);
				if (!flags.equals("......")){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(flags));
					values.add(cfqValue);					
				}
				break;
				
			case "Fwd":	
				short forwardingStatus = Short.parseShort(fields.get(i));
				if (forwardingStatus != 0){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(forwardingStatus));
					values.add(cfqValue);					
				}
				break;
				
			case "STos":
				short stos = Short.parseShort(fields.get(i));
				if (stos != 0)	{
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(stos));
					values.add(cfqValue);
				}
				break;

			case "DTos":
				short dtos = Short.parseShort(fields.get(i));
				if (dtos != 0)	{
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(dtos));
					values.add(cfqValue);
				}
				break;
				
			//FIXME: use 4-bytes instead of long/8-bytes. Is that even meaningful
			case "In Pkt":
				long inPacket = Long.parseLong(fields.get(i));
				if (inPacket != 0){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(inPacket));
					values.add(cfqValue);
				}
				break;
				
			//FIXME: use 4-bytes instead of long/8-bytes. Is that even meaningful
			case "Out Pkt":
				long outPacket = Long.parseLong(fields.get(i));
				if (outPacket != 0){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(outPacket));
					values.add(cfqValue);
				}
				break;
				
			//FIXME: use 4-bytes instead of long/8-bytes. Is that even meaningful
			case "In Byte":
				long inByte = Long.parseLong(fields.get(i));
				if (inByte != 0){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(inByte));
					values.add(cfqValue);
				}
				break;				
				
				//FIXME: use 4-bytes instead of long/8-bytes. Is that even meaningful
			case "Out Byte":
				long outByte = Long.parseLong(fields.get(i));
				if (outByte != 0){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(outByte));
					values.add(cfqValue);
				}
				break;		
			
				//FIXME: use 4-bytes instead of long/8-bytes. Is that even meaningful
			case "Flows":
				long flows = Long.parseLong(fields.get(i));
				if (flows != 0){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(flows));
					values.add(cfqValue);
				}
				break;	
				
				// Input ," +
//				"Output ,Src AS ,Dst AS ,SMask ,DMask ,DTos ,Dir      
				
			case "Input":
				short inputIF = Short.parseShort(fields.get(i));
				cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(inputIF));
				values.add(cfqValue);
				break;
				
			case "Output":
				short outputIF = Short.parseShort(fields.get(i));
				cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(outputIF));
				values.add(cfqValue);
				break;
				
			case "Src AS":
				long sourceAS = Long.parseLong(fields.get(i));
				if (sourceAS != 0){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(sourceAS));
					values.add(cfqValue);
				}
				break;
				
			case "Dst AS":
				long destinationAS = Long.parseLong(fields.get(i));
				if (destinationAS != 0){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(destinationAS));
					values.add(cfqValue);
				}
				break;	
				
			case "SMask":
				if (!fields.get(i).equals("0")){
					InetAddress address = InetAddress.getByName(fields.get(i));
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), address.getAddress());
					values.add(cfqValue);
				}
				break;
				
			case "DMask":
				if (!fields.get(i).equals("0")){
					InetAddress address = InetAddress.getByName(fields.get(i));
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), address.getAddress());
					values.add(cfqValue);
				}
				break;
				
			case "Dir":
				if (fields.get(i).equals("I")){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(false));
					values.add(cfqValue);
				} else if (fields.get(i).equals("E")){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(true));
					values.add(cfqValue);
				} else {
					System.err.println("Unable to recognize Direction (I/E): " + fields.get(i));
				}
				break;
				//,Next-hop IP  ,BGP next-hop IP ,SVlan ,DVlan   ," +
//				"In src MAC Addr  ,Out dst MAC Addr   ,In dst MAC Addr  ,Out src MAC Addr  ,MPLS lbl 1   ,MPLS lbl 2   ," +
//				"MPLS lbl 3   ,MPLS lbl 4"
			case "Next-hop IP":
				if (!fields.get(i).equals("0.0.0.0")){
					InetAddress address = InetAddress.getByName(fields.get(i));
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), address.getAddress());
					values.add(cfqValue);
				}
				break;
				
			case "BGP next-hop IP":
				if (!fields.get(i).equals("0.0.0.0")){
					InetAddress address = InetAddress.getByName(fields.get(i));
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), address.getAddress());
					values.add(cfqValue);
				}
				break;
				
			case "Router IP":
				if (!fields.get(i).equals("0.0.0.0")){
					InetAddress address = InetAddress.getByName(fields.get(i));
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), address.getAddress());
					values.add(cfqValue);
				}
				break;
				
			case "SVlan":
				short svlan = Short.parseShort(fields.get(i));
				if (svlan != 0){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(svlan));
					values.add(cfqValue);
				}
				break;
				
			case "DVlan":
				short dvlan = Short.parseShort(fields.get(i));
				if (dvlan != 0){
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), Bytes.toBytes(dvlan));
					values.add(cfqValue);
				}
				break;
				
			case "In src MAC Addr":
				if (!fields.get(i).equals("00:00:00:00:00:00")){
					MacAddress mac = new MacAddress(fields.get(i));
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), mac.getBytes());
					values.add(cfqValue);
				}
				break;
			case "Out dst MAC Addr":
				if (!fields.get(i).equals("00:00:00:00:00:00")){
					MacAddress mac = new MacAddress(fields.get(i));
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), mac.getBytes());
					values.add(cfqValue);
				}
				break;
			case "In dst MAC Addr":
				if (!fields.get(i).equals("00:00:00:00:00:00")){
					MacAddress mac = new MacAddress(fields.get(i));
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), mac.getBytes());
					values.add(cfqValue);
				}
				break;
			case "Out src MAC Addr":
				if (!fields.get(i).equals("00:00:00:00:00:00")){
					MacAddress mac = new MacAddress(fields.get(i));
					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), mac.getBytes());
					values.add(cfqValue);
				}
				break;
//			case "MPLS lbl 1":
//				if (!fields.get(i).equals("0-0-0")){
//					cfqValue = new CFQValue(families.get(i), qualifiers.get(i), mac.getBytes());
//					values.add(cfqValue);
//				}
//				break;
			
				
			default:
				break;
			}
		}
		return values;
	}
	
	
	public static String decodeCFQValues(CFQValue cfqValue) throws UnknownHostException{
		
		String family = new String(cfqValue.getColumnFamily());
		String qualifier = Utils.getColumnQualifierString(cfqValue.getQualifier());
		String value = null;
		
		switch (qualifier) {
		case "Date flow end":
			value = new Date(Bytes.toLong(cfqValue.getValue())).toString();
			break;

		case "Proto":
			value = Bytes.toShort(cfqValue.getValue())+"";
			break;
			
		// FIXME: use 9 bits to handle flags, instead of using a string.
		case "Flags":
			value = new String(cfqValue.getValue());
			break;
			
		case "Fwd":	
			value = Bytes.toShort(cfqValue.getValue())+"";
			break;
			
		case "STos":
			value = Bytes.toShort(cfqValue.getValue()) + "";
			break;

		case "DTos":
			value = Bytes.toShort(cfqValue.getValue()) + "";
			break;
			
		//FIXME: use 4-bytes instead of long/8-bytes. Is that even meaningful
		case "In Pkt":
			value = Bytes.toLong(cfqValue.getValue())+"";
			break;
			
		//FIXME: use 4-bytes instead of long/8-bytes. Is that even meaningful
		case "Out Pkt":
			value = Bytes.toLong(cfqValue.getValue())+"";
			break;
			
		//FIXME: use 4-bytes instead of long/8-bytes. Is that even meaningful
		case "In Byte":
			value = Bytes.toLong(cfqValue.getValue())+"";
			break;				
			
		//FIXME: use 4-bytes instead of long/8-bytes. Is that even meaningful
		case "Out Byte":
			value = Bytes.toLong(cfqValue.getValue())+"";
			break;		
			
		//FIXME: use 4-bytes instead of long/8-bytes. Is that even meaningful
		case "Flows":
			value = Bytes.toLong(cfqValue.getValue())+"";
			break;
			// Input ," +
//			"Output ,Src AS ,Dst AS ,SMask ,DMask ,DTos ,Dir      
			
		case "Input":
			value = Bytes.toShort(cfqValue.getValue())+"";
			break;
			
		case "Output":
			value = Bytes.toShort(cfqValue.getValue())+"";
			break;
			
		case "Src AS":
			value = Bytes.toLong(cfqValue.getValue())+"";
			break;
			
		case "Dst AS":
			value = Bytes.toLong(cfqValue.getValue())+"";
			break;	
			
		case "SMask":
			value = InetAddress.getByAddress(cfqValue.getValue()).getHostAddress();
			break;
			
		case "DMask":
			value = InetAddress.getByAddress(cfqValue.getValue()).getHostAddress();
			break;
			
		case "Dir":
			boolean dir = Bytes.toBoolean(cfqValue.getValue());
			if (dir){
				value = "Egress";
			} else {
				value = "Ingress";
			}
			break;

		case "Next-hop IP":
			value = InetAddress.getByAddress(cfqValue.getValue()).getHostAddress();
			break;
			
		case "BGP next-hop IP":
			value = InetAddress.getByAddress(cfqValue.getValue()).getHostAddress();
			break;
			
		case "Router IP":
			value = InetAddress.getByAddress(cfqValue.getValue()).getHostAddress();
			break;
			
		case "SVlan":
			value = Bytes.toShort(cfqValue.getValue())+"";
			break;
			
		case "DVlan":
			value = Bytes.toShort(cfqValue.getValue())+"";
			break;
			
		case "In src MAC Addr":
			value = new MacAddress(cfqValue.getValue()).toString();
			break;
		case "Out dst MAC Addr":
			value = new MacAddress(cfqValue.getValue()).toString();
			break;
		case "In dst MAC Addr":
			value = new MacAddress(cfqValue.getValue()).toString();
			break;
		case "Out src MAC Addr":
			value = new MacAddress(cfqValue.getValue()).toString();
			break;
//		case "MPLS lbl 1":
//			if (!fields.get(i).equals("0-0-0")){
//				cfqValue = new CFQValue(families.get(i), qualifiers.get(i), mac.getBytes());
//				values.add(cfqValue);
//			}
//			break;
		
			
		default:
			break;
		}
		int size = cfqValue.getColumnFamily().length + cfqValue.getQualifier().length + cfqValue.getValue().length;
		return family+ ":" + cfqValue.getQualifier()[0] + ":" + value + "(Size: " + size + ", Qualifier String: " + qualifier  + " )";
	}
	
	public static void main(String[] args) {
//		prepareCFQValues(fields, families, qualifiers);
	}

}
