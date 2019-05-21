package org.fastcatsearch.protocol.mysql;

import java.io.IOException;

import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;

public class HandshakePacket extends Packet {

	private int protocolVersion;
	
	private String serverVersion;
	
	private int connectionId;
	
	private byte[][] authData = new byte[2][];
	
	private byte filler;
	
	private byte charset;
	
	private int status;
	
	private byte authDataLen;
	
	private int capabilities;
	
	private String authMethod;
	
	public int getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(int protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	public String getServerVersion() {
		return serverVersion;
	}

	public void setServerVersion(String serverVersion) {
		this.serverVersion = serverVersion;
	}

	public int getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(int connectionId) {
		this.connectionId = connectionId;
	}

	public byte[][] getAuthData() {
		return authData;
	}

	public void setAuthData(byte[][] authData) {
		this.authData = authData;
	}

	public byte getFiller() {
		return filler;
	}

	public void setFiller(byte filler) {
		this.filler = filler;
	}

	public byte getCharset() {
		return charset;
	}

	public void setCharset(byte charset) {
		this.charset = charset;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public byte getAuthDataLen() {
		return authDataLen;
	}

	public void setAuthDataLen(byte authDataLen) {
		this.authDataLen = authDataLen;
	}

	public int getCapabilities() {
		return capabilities;
	}

	public void setCapabilities(int capabilities) {
		this.capabilities = capabilities;
	}

	public String getAuthMethod() {
		return authMethod;
	}

	public void setAuthMethod(String authMethod) {
		this.authMethod = authMethod;
	}
	
	@Override
	public void readFrom(DataInput input) throws IOException {
		PacketDataInput in = (PacketDataInput) input;
		in.setPacket(this);
		in.pos(0);
		in.readHeader();
		this.protocolVersion = in.read();
		this.serverVersion = new String(in.readBuffer(UNTIL_NULL));
		this.connectionId = in.readInteger(4);
		this.authData[0] = in.readBuffer(8);
		this.filler = (byte)in.read();
		int capability1 = in.readInteger(2);
		this.charset = (byte)in.read();
		this.status = in.readInteger(2);
		int capability2 = in.readInteger(2);
		this.capabilities = capability1 | capability2 << 16;
		this.authDataLen = (byte)in.read();
		if ((capabilities & CLIENT_PLUGIN_AUTH) == 0) { 
			authDataLen = 0;
		}
		in.skip(10);
		if ((capabilities & CLIENT_SECURE_CONNECTION) != 0) {
			this.authData[1] = in.readBuffer(UNTIL_NULL);
		} else {
			in.skip(13);
		}
		this.authMethod = new String(in.readBuffer(UNTIL_NULL));
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		PacketDataOutput out = (PacketDataOutput) output;
		out.setPacket(this);
		int capability1 = capabilities & 0xffff;
		int capability2 = (capabilities >> 16) & 0xffff;
		out.pos(0);
		out.write(protocolVersion);
		out.writeBuffer(serverVersion.getBytes(), UNTIL_NULL);
		out.writeInteger(connectionId, 4);
		out.writeBuffer(authData[0], 8);
		out.write(filler);
		out.writeInteger(capability1, 2);
		out.write(charset);
		out.writeInteger(status, 2);
		out.writeInteger(capability2, 2);
		out.write(authDataLen);
		out.fill(0, 10);
		if ((capabilities & CLIENT_SECURE_CONNECTION) != 0) {
			out.writeBuffer(authData[1], UNTIL_NULL);
		} else {
			out.fill(0, 13);
		}
		out.writeBuffer(authMethod.getBytes(), UNTIL_NULL);
		
		out.writeHeader();
		out.commit();
	}
	
	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		int capability1 = capabilities & 0xffff;
		int capability2 = (capabilities >> 16) & 0xffff;
		ret.append("================================================================================\n")
			.append("packetLength : " + getPacketLength() + "\n")
			.append("sequence : " + getSequence() + "\n")
			.append("protocolVersion : " + protocolVersion + "\n")
			.append("serverVersion : " + serverVersion + "\n")
			.append("connectionId : " + connectionId + "\n")
			.append("authData : " + binHex(authData[0], -1, -1, true) + " / " + binHex(authData[1], -1, -1, true) + "\n")
			.append("filler : " + numHex(filler, 1, true) + "\n")
			.append("capabilities : " + numHex(capability1, 2, true) + " / " + numHex(capability2, 2, true) + "\n")
			.append("charset : " + numHex(charset, 1, true) + "\n")
			.append("status : " + numHex(status, 2, true) + "\n")
			.append("authDataLen : " + authDataLen + " / " + (authData[0].length + authData[1].length) + "\n")
			.append("authMethod : " + authMethod + "\n")
			.append("================================================================================");
		return ret.toString();
	}
//================================================================================
//  SAMPLE PACKET	
//================================================================================
//	packetLength : 89
//	sequence : 0
//	protocolVersion : 10
//	serverVersion : 5.5.5-10.1.26-MariaDB
//	connectionId : 189
//	authData : 5d2a 5758 343b 6b2a / 2f49 643a 353b 6775 3957 2c72
//	filler : 00
//	capabilities : fff7 / 3fa0
//	charset : 21
//	status : 0200
//	authDataLen : 21 / 20
//	authMethod : mysql_native_password	
//================================================================================
}
