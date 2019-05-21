package org.fastcatsearch.protocol.mysql;

import java.io.IOException;

import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;

public class ColumnPacket extends Packet {
	
	private String catalog;
	private String schema;
	private String table;
	private String orgTable;
	private String name;
	private String orgName;
	private int charset;
	private int columnLength;
	private int type;
	private int flags;
	private byte decimals;
	
	public String getCatalog() {
		return catalog;
	}
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}
	public String getOrgTable() {
		return orgTable;
	}
	public void setOrgTable(String orgTable) {
		this.orgTable = orgTable;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getOrgName() {
		return orgName;
	}
	public void setOrgName(String orgName) {
		this.orgName = orgName;
	}
	public int getCharset() {
		return charset;
	}
	public void setCharset(int charset) {
		this.charset = charset;
	}
	public int getColumnLength() {
		return columnLength;
	}
	public void setColumnLength(int columnLength) {
		this.columnLength = columnLength;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public int getFlags() {
		return flags;
	}
	public void setFlags(int flags) {
		this.flags = flags;
	}
	public byte getDecimals() {
		return decimals;
	}
	public void setDecimals(byte decimals) {
		this.decimals = decimals;
	}
	
	public ColumnPacket() {
		catalog = "";
		schema = "";
		table = "";
		orgTable = "";
		name = "";
		orgName = "";
		charset = 0;
		columnLength = 0;
		type = 0;
		flags = 0;
		decimals = 0;
	}
	
	@Override
	public void readFrom(DataInput input) throws IOException {
		PacketDataInput in = (PacketDataInput) input;
		in.setPacket(this);
		in.pos(0);
		in.readHeader();
		this.catalog = new String(in.readBuffer());
		this.schema = new String(in.readBuffer());
		this.table = new String(in.readBuffer());
		this.orgTable = new String(in.readBuffer());
		this.name = new String(in.readBuffer());
		this.orgName = new String(in.readBuffer());
		in.skip(1);
		this.charset = in.readInteger(2);
		this.columnLength = in.readInteger(4);
		this.type = in.read();
		this.flags = in.readInteger(2);
		this.decimals = (byte)in.read();
		in.skip(2);
	}
	
	@Override
	public void writeTo(DataOutput output) throws IOException {
		PacketDataOutput out = (PacketDataOutput) output;
		out.setPacket(this);
		out.pos(0);
		out.writeBuffer(catalog.getBytes());
		out.writeBuffer(schema.getBytes());
		out.writeBuffer(table.getBytes());
		out.writeBuffer(orgTable.getBytes());
		out.writeBuffer(name.getBytes());
		out.writeBuffer(orgName.getBytes());
		out.fill(0, 1);
		out.writeInteger(charset, 2);
		out.writeInteger(columnLength, 4);
		out.write(type);
		out.writeInteger(flags, 2);
		out.write(decimals);
		out.fill(0, 2);
		out.writeHeader();
		out.commit();
	}
	
	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		ret .append("catalog : " + catalog + " / schema : " + schema + " / ")
			.append("table : " + table + " / orgTable : " + orgTable + " / ")
			.append("name : " + name +" / orgName : " + orgName + "  ");
		return ret.toString();
	}
}
