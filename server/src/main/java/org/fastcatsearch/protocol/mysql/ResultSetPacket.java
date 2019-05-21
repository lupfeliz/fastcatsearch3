package org.fastcatsearch.protocol.mysql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.fastcatsearch.ir.io.DataInput;
import org.fastcatsearch.ir.io.DataOutput;

public class ResultSetPacket extends Packet {

	private List<ColumnPacket> columns;
	private Iterator<List<String>> data;
	
	public List<ColumnPacket> getColumns() {
		return columns;
	}

	public void setColumns(List<ColumnPacket> columns) {
		this.columns = columns;
	}

	public Iterator<List<String>> getData() {
		return data;
	}

	public void setData(Iterator<List<String>> data) {
		this.data = data;
	}

	public ResultSetPacket() {
		columns = new ArrayList<ColumnPacket>();
	}
	
	@Override
	public void readFrom(final DataInput input) throws IOException {
		final PacketDataInput in = (PacketDataInput) input;
		ColumnCountPacket count = new ColumnCountPacket();
		count.readFrom(input);
		final ResultPacket row = new ResultPacket(count.getCount());
		for (int inx = 0; inx < count.getCount(); inx++) {
			ColumnPacket column = new ColumnPacket();
			column.readFrom(input);
			columns.add(column);
		}
		{
			////////////////////////////////////////////////////////////////////////////////
			byte[] buf = in.getBuffer();
			in.readBytes(buf, 0, 5);
			if ((buf[4] & 0xff) == 0xfe) {
				in.readBytes(buf, 0, 4);
			}
			////////////////////////////////////////////////////////////////////////////////
		}
		data = new Iterator<List<String>>() {
			boolean hasNext = true;
			List<String> data = null;
			
			@Override public boolean hasNext() {
				hasNext = false;
				try {
					row.readFrom(in);
					int flag = row.getFinished();
					if (flag < 0) {
						hasNext = false;
					} else if (flag != 0) {
						{
							////////////////////////////////////////////////////////////////////////////////
							byte[] buf = in.getBuffer();
							in.readBytes(buf, 0, 4);
							////////////////////////////////////////////////////////////////////////////////
						}
						hasNext = false;
					} else {
						data = row.getData();
						hasNext = true;
					}
				} catch (IOException e) {
					System.out.println("ERROR!");
				}
				return hasNext;
			}
			@Override public List<String> next() { return data; }
		};
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		byte seq = (byte)1;
		PacketDataOutput out = (PacketDataOutput) output;
		ColumnCountPacket count = new ColumnCountPacket();
		count.setSequence(seq);
		count.setCount(columns.size());
		count.writeTo(out);
		seq++;
		for (int inx = 0; inx < columns.size(); inx++) {
			ColumnPacket column = columns.get(inx);
			count.setSequence(seq);
			column.writeTo(out);
			seq++;
		}
		byte[] eof = new byte[] { 
			(byte)0x05, (byte)0x00, (byte)0x00, (byte)0x00, 
			(byte)0xfe, (byte)0x00, (byte)0x00, (byte)0x02, (byte)0x00 
		};
		eof[3] = seq;
		ResultPacket result = new ResultPacket(columns.size());
		out.writeBytes(eof, 0, eof.length);
		seq++;
		for (; data.hasNext(); ) {
			result.setSequence(seq);
			result.setData(data.next());
			result.writeTo(out);
			seq++;
		}
		eof[3] = seq;
		out.writeBytes(eof, 0, eof.length);
	}
	
	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		return ret.toString();
	}
}
