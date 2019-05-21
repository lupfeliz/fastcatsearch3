package org.fastcatsearch.protocol.mysql;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.Arrays;

import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;

public class AuthPacket extends Packet {
	
	private int capabilityFlags;
	private int maxPacket = 0x1 << 24 -1;
	private int charset;
	private String userName;
	private byte[] password;
	private int authResponseLength;
	private String database;
	private String authPluginName;
	
	public int getCapabilityFlags() {
		return capabilityFlags;
	}

	public void setCapabilityFlags(int capabilityFlags) {
		this.capabilityFlags = capabilityFlags;
	}

	public int getMaxPacket() {
		return maxPacket;
	}

	public void setMaxPacket(int maxPacket) {
		this.maxPacket = maxPacket;
	}

	public int getCharset() {
		return charset;
	}

	public void setCharset(int charset) {
		this.charset = charset;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public byte[] getPassword() {
		return password;
	}

	public void setPassword(byte[] password) {
		this.password = password;
	}

	public int getAuthResponseLength() {
		return authResponseLength;
	}

	public void setAuthResponseLength(int authResponseLength) {
		this.authResponseLength = authResponseLength;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getAuthPluginName() {
		return authPluginName;
	}

	public void setAuthPluginName(String authPluginName) {
		this.authPluginName = authPluginName;
	}

	public static byte[] password(String str, byte[]b1, byte[]b2) {
		MessageDigest md = null;
		byte[] ret = null;
		try {
			md = MessageDigest.getInstance("SHA-1");		
			ret = str.getBytes();
			byte[] p1 = md.digest(ret);
			ret = md.digest(p1);
			if (b1 != null && b2 != null) {
				int addlen = b1.length + b2.length;
				ret = Arrays.copyOf(ret, ret.length + addlen);
				System.arraycopy(ret, 0, ret, addlen, ret.length - addlen);
				System.arraycopy(b1, 0, ret, 0, b1.length);
				System.arraycopy(b2, 0, ret, b1.length, b2.length);
				ret = md.digest(ret);
				for (int inx = 0; inx < ret.length; inx++) {
					ret[inx] = (byte)((p1[inx] ^ ret[inx]) & 0xff);
				}
			}
		} catch (Exception ignore) {
		}
		if (ret == null) { ret = new byte[0]; }
		return ret;
	}
	
	@Override
	public void readFrom(DataInput input) throws IOException {
		PacketDataInput in = (PacketDataInput) input;
		in.setPacket(this);
		in.pos(0);
		in.readHeader();
		this.capabilityFlags = in.readInteger(4);
		this.maxPacket = in.readInteger(4);
		in.skip(23);
		this.charset = in.read();
		this.userName = new String(in.readBuffer(UNTIL_NULL));
		if ((this.capabilityFlags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
			this.password = in.readBuffer();
		} else if ((this.capabilityFlags & CLIENT_SECURE_CONNECTION) != 0) {
			this.authResponseLength = in.read();
			this.password = in.readBuffer(authResponseLength);
		} else {
			in.read();
		}
	}

	@Override
	//FIXME:WIP (Not implemented fully)
	public void writeTo(DataOutput output) throws IOException {
		PacketDataOutput out = (PacketDataOutput) output;
		out.setPacket(this);
		out.pos(0);
		out.writeInteger(capabilityFlags, 4);
		out.writeInteger(maxPacket, 4);
		out.fill(0, 23);
		out.write(charset);
		out.writeBytes(userName.getBytes(), UNTIL_NULL);
		if ((this.capabilityFlags & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
			out.writeBytes(password);
		} else if ((this.capabilityFlags & CLIENT_SECURE_CONNECTION) != 0) {
			out.write(authResponseLength);
			out.writeBytes(password, authResponseLength);
		} else {
			out.write(0);
		}
		out.writeHeader();
		out.commit();
	}
	
	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		ret.append("================================================================================\n")
			.append("packetLength : " + getPacketLength() + "\n")
			.append("sequence : " + getSequence() + "\n")
			.append("capabilityFlags : " + numHex(capabilityFlags, 4, true) + "\n")
			.append("maxPacket : " + maxPacket + "\n")
			.append("charset : " + numHex(charset, 1, true) + "\n")
			.append("userName : " + userName + "\n")
			.append("password : " + binHex(password, -1, -1, true) + "\n")
			.append("authResponseLength : " + authResponseLength + "\n")
			.append("================================================================================");
		return ret.toString();
	}
//================================================================================
//  SAMPLE PACKET	
//================================================================================
//	packetLength : 221
//	sequence : 1
//	capabilityFlags : 85a6 9f20
//	maxPacket : 16777216
//	charset : 00
//	userName : user
//	password : c2c1 f6ca 51d6 0fba afcd 9b1d 65c9 fa5b d1c6 4052
//	authResponseLength : 20
//================================================================================
}
