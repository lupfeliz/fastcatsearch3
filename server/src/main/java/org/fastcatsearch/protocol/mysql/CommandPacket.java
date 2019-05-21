package org.fastcatsearch.protocol.mysql;

import java.io.IOException;

import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;

public class CommandPacket extends Packet {

	private int header;
	private String args;
	
	public int getHeader() {
		return header;
	}

	public void setHeader(int header) {
		this.header = header;
	}

	public String getArgs() {
		return args;
	}

	public void setArgs(String args) {
		this.args = args;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		PacketDataInput in = (PacketDataInput) input;
		in.setPacket(this);
		in.pos(0);
		in.readHeader();
		this.header = in.read();
		this.args = new String(in.readBuffer(UNTIL_ENDS));
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		PacketDataOutput out = (PacketDataOutput) output;
		out.setPacket(this);
		out.pos(0);
		out.write(header);
		out.writeBuffer(args.getBytes(), UNTIL_ENDS);
		out.writeHeader();
		out.commit();
	}
	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		ret.append("================================================================================\n")
			.append("packetLength : " + getPacketLength() + "\n")
			.append("sequence : " + getSequence() + "\n")
			.append("header: " + Packet.numHex(header, 1, true)+ "\n")
			.append("args: " + args + "\n")
			.append("================================================================================");
		return ret.toString();
	}
}