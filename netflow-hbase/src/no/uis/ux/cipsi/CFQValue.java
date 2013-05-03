package no.uis.ux.cipsi;

import java.net.UnknownHostException;

public class CFQValue {

	byte[] columnFamily;
	byte[] qualifier;
	byte[] value;
	
	public CFQValue(byte[] columnFamily, byte[] qualifier, byte[] value) {
		this.columnFamily = columnFamily;
		this.qualifier = qualifier;
		this.value = value;
	}

	public byte[] getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(byte[] columnFamily) {
		this.columnFamily = columnFamily;
	}

	public byte[] getQualifier() {
		return qualifier;
	}

	public void setQualifier(byte[] qualifier) {
		this.qualifier = qualifier;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		try {
			return NetFlowV5Record.decodeCFQValues(this);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		} 
	}
	
}
