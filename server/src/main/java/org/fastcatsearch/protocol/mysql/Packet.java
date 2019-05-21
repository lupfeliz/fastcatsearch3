package org.fastcatsearch.protocol.mysql;

import java.util.Arrays;

import org.fastcatsearch.common.io.Streamable;

public abstract class Packet implements Streamable {
	
	public static final int CLIENT_LONG_PASSWORD						= 0x00000001;
	public static final int CLIENT_FOUND_ROWS							= 0x00000002;
	public static final int CLIENT_LONG_FLAG							= 0x00000004;
	public static final int CLIENT_CONNECT_WITH_DB					= 0x00000008;
	public static final int CLIENT_NO_SCHEMA							= 0x00000010;
	public static final int CLIENT_COMPRESS							= 0x00000020;
	public static final int CLIENT_ODBC								= 0x00000040;
	public static final int CLIENT_LOCAL_FILES						= 0x00000080;
	public static final int CLIENT_IGNORE_SPACE						= 0x00000100;
	public static final int CLIENT_PROTOCOL_41						= 0x00000200;
	public static final int CLIENT_INTERACTIVE						= 0x00000400;
	public static final int CLIENT_SSL									= 0x00000800;
	public static final int CLIENT_IGNORE_SIGPIPE					= 0x00001000;
	public static final int CLIENT_TRANSACTIONS						= 0x00002000;
	public static final int CLIENT_RESERVED							= 0x00004000;
	public static final int CLIENT_SECURE_CONNECTION				= 0x00008000;
	public static final int CLIENT_MULTI_STATEMENTS					= 0x00010000;
	public static final int CLIENT_MULTI_RESULTS						= 0x00020000;
	public static final int CLIENT_PS_MULTI_RESULTS					= 0x00040000;
	public static final int CLIENT_PLUGIN_AUTH						= 0x00080000;
	public static final int CLIENT_CONNECT_ATTRS						= 0x00100000;
	public static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA	= 0x00200000;
	public static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS	= 0x00400000;
	public static final int CLIENT_SESSION_TRACK						= 0x00800000;
	public static final int CLIENT_DEPRECATE_EOF						= 0x01000000;
	
	
	 public static final int COM_SLEEP 					= 0x00;
	 public static final int COM_QUIT						= 0x01;
	 public static final int COM_INIT_DB					= 0x02;
	 public static final int COM_QUERY						= 0x03;
	 public static final int COM_FIELD_LIST				= 0x04;
	 public static final int COM_CREATE_DB				= 0x05;
	 public static final int COM_DROP_DB					= 0x06;
	 public static final int COM_REFRESH					= 0x07;
	 public static final int COM_SHUTDOWN					= 0x08;
	 public static final int COM_STATISTICS				= 0x09;
	 public static final int COM_PROCESS_INFO				= 0x0a;
	 public static final int COM_CONNECT					= 0x0b;
	 public static final int COM_PROCESS_KILL				= 0x0c;
	 public static final int COM_DEBUG						= 0x0d;
	 public static final int COM_PING						= 0x0e;
	 public static final int COM_TIME						= 0x0f;
	 public static final int COM_DELAYED_INSERT			= 0x10;
	 public static final int COM_CHANGE_USER				= 0x11;
	 public static final int COM_BINLOG_DUMP				= 0x12;
	 public static final int COM_TABLE_DUMP				= 0x13;
	 public static final int COM_CONNECT_OUT				= 0x14;
	 public static final int COM_REGISTER_SLAVE			= 0x15;
	 public static final int COM_STMT_PREPARE				= 0x16;
	 public static final int COM_STMT_EXECUTE				= 0x17;
	 public static final int COM_STMT_SEND_LONG_DATA		= 0x18;
	 public static final int COM_STMT_CLOSE				= 0x19;
	 public static final int COM_STMT_RESET				= 0x1a;
	 public static final int COM_SET_OPTION				= 0x1b;
	 public static final int COM_STMT_FETCH				= 0x1c;
	 public static final int COM_DAEMON					= 0x1d;
	 public static final int COM_BINLOG_DUMP_GTID		= 0x1e;
	 public static final int COM_RESET_CONNECTION 		= 0x1f;
	
	public final static int HEADER_SIZE = 4;
	public final static int MAX_PACKET_SIZE = 16 * 1024 * 1024;
	
	public final static int UNTIL_NULL = -1;
	public final static int UNTIL_ENDS = -2;
	
	private int packetLength;
	private byte sequence;
	
	public int getPacketLength() {
		return packetLength;
	}
	
	public void setPacketLength(int packetLength) {
		this.packetLength = packetLength;
	}
	
	public byte getSequence() {
		return sequence;
	}
	
	public void setSequence(byte sequence) {
		this.sequence = sequence;
	}
	
	public static byte[] increaseBuf(byte[] buffer, int newSize) {
		if (buffer.length < newSize) {
			buffer = Arrays.copyOf(buffer, newSize);
		}
		return buffer;
	}
	
	public static String binHex(byte[] buf, int pos, int len) {
		return binHex(buf, pos, len, false);
	}
	
	public static String binHex(byte[] buf, int pos, int len, boolean format) {
		int ed = pos;
		StringBuilder ret = new StringBuilder();
		if (pos < 0) { pos = 0; }
		if (len < 0) { 
			ed = buf.length; 
		} else {
			ed = pos + len;
		}
		for (int inx = pos; inx < ed; inx++) {
			String s = Integer.toHexString(buf[inx] & 0xff);
			if(s.length() == 1) { s = "0" + s; }
			if (format && inx - pos > 0 && ((inx - pos) % 2 == 0)) {
				ret.append(" ");
			}
			ret.append(s);
		}
		return ret.toString();
	}
	
	public static String numHex(long num, int len) {
		return numHex(num, len, false);
	}
	
	public static String numHex(long num, int len, boolean format) {
		StringBuilder ret = new StringBuilder();
		for (int inx = 0; inx < len; inx++) {
			String s = Long.toHexString(((num >> (inx * 8)) & 0xff));
			if(s.length() == 1) { s = "0" + s; }
			if (format && inx > 0 && inx % 2 == 0) {
				ret .append(" ");
			}
			ret.append(s);
		}
		return ret.toString();
	}
}