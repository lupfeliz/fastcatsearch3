package org.fastcatsearch.protocol.mysql;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.fastcatsearch.ir.io.DataInput;

import static org.fastcatsearch.protocol.mysql.Packet.UNTIL_ENDS;
import static org.fastcatsearch.protocol.mysql.Packet.UNTIL_NULL;

public class PacketDataInput extends DataInput {

	private Packet packet;
	private InputStream istream;
	private byte[] buffer;
	private int pos;
	
	public PacketDataInput(InputStream istream, byte[] buffer) {
		this.istream = istream;
		this.buffer = buffer;
	}

	@Override public byte readByte() throws IOException {
		int data = istream.read();
		if (data == -1)
			throw new EOFException();
		return (byte) (data & 0xff);
	}

	@Override public long length() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override public void readBytes(byte[] buffer, int offset, int length) throws IOException {
		while (length > 0) {
			final int cnt = istream.read(buffer, offset, length);
			if (cnt < 0) {
				throw new EOFException();
			}
			length -= cnt;
			offset += cnt;
		}
	}
	
	@Override
	public void close() throws IOException {
		istream.close();
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
	
	public int skip(int mov) {
		return (this.pos += mov);
	}
	
	public int read() {
		int ret = 0;
		ret = (byte) (this.buffer[this.pos] & 0xff);
		this.pos++;
		return ret;
	}
	
	public int readInteger(int bytes) {
		int ret = 0;
		for (int inx = 0; inx < bytes; inx++) {
			ret |= ((this.buffer[this.pos + inx] & 0xff) << (inx * 8));
		}
		this.pos+=bytes;
		return ret;
	}
	
	public long readLong(int bytes) {
		long ret = 0;
		for (int inx = 0; inx < bytes; inx++) {
			ret |= ((this.buffer[this.pos + inx] & 0xff) << (inx * 8));
		}
		this.pos+=bytes;
		return ret;
	}
	
	public long readNumber() {
		long ret = 0;
		int len = readLength();
		if (len == 1) {
			ret = read();
		} else if (len > 0) {
			this.pos++;
			ret = readLong(len);
		} else {
			ret = 0;
		}
		return ret;
	}
	
	public byte[] readBuffer(int bytes) {
		byte[] ret = null;
		if (bytes == UNTIL_NULL) {
			for (int inx = 0; inx < this.buffer.length; inx++) {
				if (buffer[pos + inx] == 0) {
					bytes = inx;
					break;
				}
			}
			ret = new byte[bytes];
			System.arraycopy(this.buffer, pos, ret, 0, bytes);
			pos += bytes + 1;
		} else if (bytes == UNTIL_ENDS) {
			bytes = packet.getPacketLength() - pos;
			ret = new byte[bytes];
			System.arraycopy(this.buffer, pos, ret, 0, bytes);
			pos += bytes;
		} else {
			ret = new byte[bytes];
			System.arraycopy(this.buffer, pos, ret, 0, bytes);
			pos += bytes;
		}
		return ret;
	}
	
	public byte[] readBuffer() {
		int len = readLength();
		if (len == 1) {
			len = read();
		} else if (len > 0) {
			this.pos++;
			len = readInteger(len);
		} else {
			len = 0;
		}
		return readBuffer(len);
	}
	
	private int readLength() {
		int ret = 0;
		int rl = 0;
		rl = this.buffer[this.pos] & 0xff;
		switch(rl) {
			case 0xfb: ret = 0; break;
			case 0xfc: ret = 2; break;
			case 0xfd: ret = 3; break;
			case 0xfe: ret = 8; break;
			case 0xff: ret = -1; break;
			default: ret = 1; break;
		}
		return ret;
	}
	
	public void readHeader() throws IOException {
		readBytes(buffer, pos(0), 4);
		int packetLength = readInteger(3);
		packet.setPacketLength(packetLength);
		packet.setSequence((byte)read());
		this.buffer = Packet.increaseBuf(buffer, packetLength);
		readBytes(buffer, pos(0), packetLength);
	}
	
	public int readRowHeader() throws IOException {
		int ret = 0;
		readBytes(buffer, 0, 5);
		pos(4);
		int flg = read() & 0xff;
		if (flg == 0xfe || flg == 0xff) {
			ret = flg;
		} else {
			pos(0);
			int packetLength = readInteger(3);
			if (packetLength == 0) {
				ret = 0xfe;
			} else {
				packet.setPacketLength(packetLength);
				packet.setSequence((byte)read());
				this.buffer = Packet.increaseBuf(buffer, packetLength);
				System.arraycopy(buffer, 4, buffer, 0, 1);
				readBytes(buffer, pos(0) + 1, packetLength - 1);
				ret = 0;
			}
		}
		return ret;
	}
}
