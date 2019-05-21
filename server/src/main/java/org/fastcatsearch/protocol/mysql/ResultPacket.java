package org.fastcatsearch.protocol.mysql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;

public class ResultPacket extends Packet {

	private long columnCount;
	private int finished;
	private List<String> data;
	
	public long getColumnCount() {
		return columnCount;
	}

	public void setColumnCount(long columnCount) {
		this.columnCount = columnCount;
	}

	public int getFinished() {
		return finished;
	}
	
	public void setFinished(int finished) {
		this.finished = finished;
	}

	public List<String> getData() {
		return data;
	}

	public void setData(List<String> data) {
		this.data = data;
	}
	
	public ResultPacket(long columnCount) {
		this.columnCount = columnCount;
		this.finished = 0;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		PacketDataInput in = (PacketDataInput) input;
		in.setPacket(this);
		in.pos(0);
		this.data = new ArrayList<String>();
		int flg = in.readRowHeader();
		if (flg == 0xfe) {
			finished = 1;
			return;
		} else if (flg == 0xff) {
			finished = -1;
			return;
		}
		for (long inx = 0; inx < columnCount; inx++) {
			data.add(new String(in.readBuffer()));
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		PacketDataOutput out = (PacketDataOutput) output;
		out.setPacket(this);
		out.pos(0);
		for (int inx = 0; inx < data.size(); inx++) {
			out.writeBuffer(data.get(inx).getBytes());
		}
		out.writeHeader();
		out.commit();
	}
	
	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		ret.append("");
		for (int inx = 0; inx < data.size(); inx++) {
			ret.append("|").append(data.get(inx));
		}
		if (data.size() > 0) { ret.append("|"); }
		return ret.toString();
	}
}