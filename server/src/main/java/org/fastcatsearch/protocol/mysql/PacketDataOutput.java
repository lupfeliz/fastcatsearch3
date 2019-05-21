package org.fastcatsearch.protocol.mysql;

import java.io.IOException;
import java.io.OutputStream;

import org.fastcatsearch.ir.io.DataOutput;

import static org.fastcatsearch.protocol.mysql.Packet.UNTIL_ENDS;
import static org.fastcatsearch.protocol.mysql.Packet.UNTIL_NULL;

public class PacketDataOutput extends DataOutput {
	
	private Packet packet;
	private OutputStream ostream;
	private byte[] buffer;
	private int pos;
	
	public PacketDataOutput(OutputStream ostream, byte[] buffer) {
		this.ostream = ostream;
		this.buffer = buffer;
	}

	@Override public void reset() throws IOException {
		
	}

	@Override public void writeByte(byte data) throws IOException {
		ostream.write(data);
	}

	@Override public void writeBytes(byte[] bytes, int offset, int length) throws IOException {
		ostream.write(bytes, offset, length);
	}
	
	public void commit() throws IOException {
		writeBytes(buffer, 0, pos);
	}
	
	public void writeHeader() throws IOException {
		byte[] header = new byte[4];
		for (int inx = 0; inx < 3; inx++) {
			header[inx] = (byte)((pos >> (inx * 8)) & 0xff);
		}
		header[3] = packet.getSequence();
		writeBytes(header, 0, header.length);
	}
	
	@Override
	public void close() throws IOException {
		ostream.close();
	}
	
	public void setPacket(Packet packet) {
		this.packet = packet;
	}
	
	public void setBuffer(byte[] buffer) {
		this.buffer = buffer;
	}
	
	public byte[] getBuffer() {
		return buffer;
	}
	
	
	public int pos(int pos) {
		return (this.pos = pos);
	}
	
	public int skip(int length) throws IOException {
		return (this.pos += length);
	}
	
	public void fill(int data, int bytes) {
		for (int inx = 0; inx < bytes; inx++) {
			this.buffer[this.pos + inx] = (byte)(data & 0xff);
		}
		this.pos += bytes;
	}
	
	public void write(int data) throws IOException {
		this.buffer[pos] = (byte)(data & 0xff);
		this.pos++;
	}
	
	public int writeLength(long length) throws IOException {
		int ret = 0;
		if (length == 0) {
			ret = 0xfb;
		} else if (length < 0xff) {
			ret = 1;
		} else if (length < 0xffff) {
			ret = 0xfc;
		} else if (length < 0xffffff) {
			ret = 0xfd;
		} else if (length < 0xffffffffffffffffL) {
			ret = 0xfe;
		} else {
			ret = 0xff;
		}
		return ret;
	}
	
	public int writeBuffer(byte[] buf) throws IOException {
		int bytes = buf.length;
		int len = writeLength(bytes);
		if (len == 1) {
			write(bytes);
			writeBuffer(buf, bytes);
		} else if (len > 0) {
			if (!(len == 0xfb || len == 0xff)) {
				write(len);
				if (len == 0xfc) { len = 2; } else 
				if (len == 0xfd) { len = 3; } else 
				if (len == 0xfe) { len = 8; }
				writeInteger(bytes, len);
				writeBuffer(buf, bytes);
			} else {
				write(0);
			}
		}
		return bytes;
	}
	
	public int writeBuffer(byte[] buf, int bytes) throws IOException {
		if (bytes == UNTIL_NULL) {
			bytes = buf.length;
			for (int inx = 0; inx < buf.length; inx++) {
				if (buf[inx] == 0) {
					bytes = inx;
					break;
				}
			}
			System.arraycopy(buf, 0, this.buffer, this.pos, bytes);
			this.pos += bytes + 1;
		} else if (bytes == UNTIL_ENDS) {
			bytes = buf.length;
			System.arraycopy(buf, 0, this.buffer, this.pos, bytes);
			this.pos += bytes;
		} else {
			System.arraycopy(buf, 0, this.buffer, this.pos, bytes);
			this.pos += bytes;
		}
		return bytes;
	}
	
	public void writeInteger(int data, int bytes) throws IOException {
		for (int inx = 0; inx < bytes; inx++) {
			this.buffer[this.pos + inx] = (byte)((data >> (inx * 8)) & 0xff);
		}
		this.pos += bytes;
	}
	
	public void writeLong(long data, int bytes) throws IOException {
		for (int inx = 0; inx < bytes; inx++) {
			this.buffer[this.pos + inx] = (byte)((data >> (inx * 8)) & 0xff);
		}
		this.pos += bytes;
	}
	
	public void writeNumber(long data) throws IOException {
		int len = writeLength(data);
		if (len == 1) {
			write((byte)(data & 0xff));
		} else if (len > 0) {
			if (!(len == 0xfb || len == 0xff)) {
				write(len);
				if (len == 0xfc) { len = 2; } else 
				if (len == 0xfd) { len = 3; } else 
				if (len == 0xfe) { len = 8; }
				writeLong(data, len);
			} else {
				write(0);
			}
		}
	}
}