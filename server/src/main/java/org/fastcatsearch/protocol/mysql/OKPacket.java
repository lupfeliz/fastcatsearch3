//package org.fastcatsearch.protocol.mysql;
//
//import java.io.IOException;
//
//import org.fastcatsearch.ir.io.DataInput;
//import org.fastcatsearch.ir.io.DataOutput;
//
//public class OKPacket extends Packet {
//
//	private byte header;
//	private long affectedRows;
//	private long lastInsertId;
//	private int statusFlags;
//	private int warnings;
//	private String info;
//	private String sessionStateChanges;
//	private int capabilities;
//
//	public byte getHeader() {
//		return header;
//	}
//
//	public void setHeader(byte header) {
//		this.header = header;
//	}
//
//	public long getAffectedRows() {
//		return affectedRows;
//	}
//
//	public void setAffectedRows(long affectedRows) {
//		this.affectedRows = affectedRows;
//	}
//
//	public long getLastInsertId() {
//		return lastInsertId;
//	}
//
//	public void setLastInsertId(long lastInsertId) {
//		this.lastInsertId = lastInsertId;
//	}
//
//	public int getStatusFlags() {
//		return statusFlags;
//	}
//
//	public void setStatusFlags(int statusFlags) {
//		this.statusFlags = statusFlags;
//	}
//
//	public int getWarnings() {
//		return warnings;
//	}
//
//	public void setWarnings(int warnings) {
//		this.warnings = warnings;
//	}
//
//	public String getInfo() {
//		return info;
//	}
//
//	public void setInfo(String info) {
//		this.info = info;
//	}
//
//	public String getSessionStateChanges() {
//		return sessionStateChanges;
//	}
//
//	public void setSessionStateChanges(String sessionStateChanges) {
//		this.sessionStateChanges = sessionStateChanges;
//	}
//
//	public int getCapabilities() {
//		return capabilities;
//	}
//
//	public void setCapabilities(int capabilities) {
//		this.capabilities = capabilities;
//	}
//	
//	public OKPacket(byte[] buf) {
//		super(buf);
//	}
//
//	@Override
//	public void readFrom(DataInput input) throws IOException {
//		readHeader(input);
//		this.header = (byte)read();
//		this.affectedRows = readNumber();
//		this.lastInsertId = readNumber();
//		this.statusFlags = readInteger(2);
//		this.warnings = readInteger(2);
//		if (getPacketLength() > rskip(0)) {
//			this.info = new String(readBytes(UNTIL_ENDS));
//		}
//		
////		byte[] buf = new byte[1024];
////		int pos = 0;
////		int len = 0;
////		input.readBytes(buf, 0, 4);
////		int packetLength = readNumber(buf, 0, 3);
////		setPacketLength(packetLength);
////		setSequence((byte)readNumber(buf, 3, 1));
////		if (buf.length < packetLength) { buf = new byte[packetLength]; }
////		input.readBytes(buf, 0, packetLength);
////		
////		this.header = (byte) readNumber(buf, pos, len = 1);
////		this.affectedRows = readNumber(buf, pos+=len, len = (int) findLength(buf, pos));
////		this.lastInsertId = readNumber(buf, pos+=len, len = (int) findLength(buf, pos));
////		
////		this.header = (byte) readNumber(buf, pos+=len, len = 1);
////		this.affectedRows = readNumber(buf, pos+=len, len = (int) findLength(buf, pos));
////		this.lastInsertId = readNumber(buf, pos+=len, len = (int) findLength(buf, pos));
////		
////		this.statusFlags = readNumber(buf, pos+=len, len = 2);
////		this.warnings = readNumber(buf, pos+=len, len = 2);
////		
////		if (packetLength > pos) {
////			this.info = new String(buf, pos+=len, packetLength - pos);
////		}
//	}
//
//	@Override
//	public void writeTo(DataOutput output) throws IOException {
//		
//	}
//}
