package org.fastcatsearch.protocol.mysql;

import java.io.IOException;

import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;

public class ColumnCountPacket extends Packet {
	
	private long count;

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}
	
	@Override
	public void readFrom(DataInput input) throws IOException {
		PacketDataInput in = (PacketDataInput) input;
		in.setPacket(this);
		in.pos(0);
		in.readHeader();
		this.count = in.readNumber();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		PacketDataOutput out = (PacketDataOutput) output;
		out.setPacket(this);
		out.pos(0);
		out.writeNumber(count);
		out.writeHeader();
		out.commit();
	}
	
	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		ret.append("================================================================================\n")
			.append("packetLength : " + getPacketLength() + "\n")
			.append("sequence : " + getSequence() + "\n")
			.append("count : " + count + "\n")
			.append("================================================================================");
		return ret.toString();
	}
}