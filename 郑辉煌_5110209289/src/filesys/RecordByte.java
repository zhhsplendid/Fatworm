package filesys;

import java.nio.ByteBuffer;

public class RecordByte {
	
	ByteBuffer buff;
	int size;
	
	public RecordByte(){
		buff = ByteBuffer.allocate(MyFile.RECORD_PAGE_SIZE);
	}
	
	public boolean ensureCapacity(int i){
		if(buff.capacity() >= size + i){
			return true;
		}
		int pos = buff.position();
		byte[] newBytes = new byte[buff.capacity() * 2];
		byte[] oldBytes = buff.array();
		System.arraycopy(oldBytes, 0, newBytes, 0, size);
		buff = ByteBuffer.wrap(newBytes);
		buff.position(pos);
		return false;
	}
	
	public void putByte(byte b){
		ensureCapacity(1);
		buff.put(b);
		size = Math.max(buff.position(), size);
	}
	
	public void putBool(boolean b){
		if(b){
			putByte((byte)1);
		}
		else{
			putByte((byte)0);
		}
	}
	
	public void putInt(int i){
		ensureCapacity(4);
		buff.putInt(i);
		size = Math.max(buff.position(), size);
	}
	
	public void putLong(long l){
		ensureCapacity(8);
		buff.putLong(l);
		size = Math.max(buff.position(), size);
	}
	
	public void putFloat(float f){
		ensureCapacity(4);
		buff.putFloat(f);
		size = Math.max(buff.position(), size);
	}
	
	public void putDouble(double d){
		ensureCapacity(8);
		buff.putDouble(d);
		size = Math.max(buff.position(), size);
	}
	
	public void putChar(char c){
		ensureCapacity(2);
		buff.putChar(c);
		size = Math.max(buff.position(), size);
	}
	
	/**
	 * this put bytes with byte[] size
	 * @param b
	 */
	public void putBytes(byte[] b){
		putInt(b.length);
		putRawBytes(b, 0, b.length);
	}
	
	public void putString(String s) {
		putBytes(s.getBytes());
	}
	
	public void putRawBytes(byte[] b){
		putRawBytes(b, 0, b.length);
	}
	
	public void putRawBytes(byte[] b, int begin, int end){
		ensureCapacity(end - begin);
		buff.put(b, begin, end);
		size = Math.max(buff.position(), size);
	}
	
	
	public int size(){
		return size;
	}
	
	public byte[] getBytes(){
		return buff.array();
	}

	
}
