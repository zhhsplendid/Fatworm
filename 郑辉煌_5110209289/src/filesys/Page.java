package filesys;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;



public class Page implements PageInterface {
	
	public static TreeSet<RemainPage> pool = new TreeSet<RemainPage>();
	public int pageID;
	public int nextID;
	public int preID;
	public boolean dirty;
	protected MyFile dataFile;
	protected Long lastTime;
	public int size;
	public ByteBuffer buff;
	public int count;
	private boolean hasFlush = false;
	private int inTransaction = 0;
	
	public Page(){}
	public Page(MyFile file, int pageId, boolean create) throws Throwable {
		lastTime = System.currentTimeMillis();
		dataFile = file;
		pageID = pageId;
		size = 0;
		dirty = false;
		hasFlush = false;
		buff = ByteBuffer.wrap(new byte[MyFile.BTREE_PAGE_SIZE]);
		
		if(!create){
			loadPage();
		}else{
			nextID = -1;
			preID = -1;
			size = 4;
			synchronized (pool){
				pool.add(new RemainPage(remainSize(), pageID));
			}
		}
	}
	
	public void loadPage() throws Throwable {
		dataFile.read(buff, pageID);
		fromBytes(buff.array());
	}

	@Override
	public Integer getID() {
		return pageID;
	}

	@Override
	public Long getTime() {
		return lastTime;
	}

	@Override
	public int headerSize() {
		// XXX 
		return -1;
	}

	@Override
	public boolean isPartial() {
		// XXX
		return false;
	}

	@Override
	public void deleteMark() {
		dirty = false;
	}

	
	
	public synchronized void write() throws Throwable {
		if(!dirty) {
			return;
		}
		toBytes();
		if(isInTransaction()){
			return;
		}
		hasFlush = true;
		dataFile.write(buff, pageID);
		dirty = false;
	}
	
	@Override
	public void fromBytes(byte[] b) throws Throwable {
		buff = ByteBuffer.wrap(b);
		buff.position(0);
		size = buff.getInt();
		count = buff.getInt();
		buff.position(0);
	}

	@Override
	public byte[] toBytes() throws Throwable {
		buff.position(0);
		buff.putInt(size);
		buff.putInt(count);
		return buff.array();
	}
	
	public void newEntry() {
		count++;
		dirty = true;
	}
	
	public synchronized void append() {
		dataFile.append(buff);
	}
	
	
	public synchronized void updateSize(int newSize){
		synchronized(pool){
			pool.remove(new RemainPage(remainSize(), pageID));
			if(size < newSize){
				dirty = true;
				size = newSize;
			}
			pool.add(new RemainPage(remainSize(), pageID));
		}
	}
	
	public synchronized static Integer newPoolPage(int wantSize){
		Iterator<RemainPage> iter = pool.iterator();
		while(iter.hasNext()){
			RemainPage next = iter.next();
			if(next.remainSize * 4 < MyFile.BTREE_PAGE_SIZE){
				iter.remove();
			}
			if(next.remainSize >= wantSize){
				return next.pageID;
			}
		}
		return null;
	} 
	
	
	@Override
	public int remainSize() {
		return MyFile.BTREE_PAGE_SIZE - size;
	}
	
	public boolean equals(Object o){
		if(o instanceof Page){
			return pageID == ((Page)o).pageID;
		}
		return false;
	}
	
	public void beginTransaction() {
		inTransaction++;
		//dirty = true;
	}

	public synchronized void commit() throws Throwable{
		if(inTransaction > 0)inTransaction--;
		if(hasFlush){
			write();
		}
	}

	public boolean isInTransaction() {
		return inTransaction != 0;
	}
	
	public synchronized int getInt(int offset) {
		buff.position(offset);
		return buff.getInt();
	}
	
	public synchronized long getLong(int offset){
		buff.position(offset);
		return buff.getLong();
	}
	
	public synchronized String getString(int offset){
		buff.position(offset);
		int length = buff.getInt();
		byte[] stringByte = new byte[length];
		buff.get(stringByte);
		return new String(stringByte);
	}
	
	public synchronized BigDecimal getBigDecimal(int offset){
		int length = getInt(offset);
		offset += 4;
		byte[] b = getBytes(offset);
		return new BigDecimal(new BigInteger(b), length);
	}
	
	private synchronized byte[] getBytes(int offset) {
		buff.position(offset);
		int length = buff.getInt();
		byte[] b = new byte[length];
		buff.get(b);
		return b;
	}
	public synchronized static int getSize(BigDecimal value){
		return 4 + getSize(value.unscaledValue().toByteArray());
	}
	public synchronized static int getSize(byte[] value){
		return 4 + value.length;
	}
	public synchronized static int getSize(String value) {
		return getSize(value.getBytes());
	}
	
	public synchronized int putString(int offset, String s) {
		dirty = true;
		return putBytes(offset, s.getBytes());
	}
	
	public synchronized int putBytes(int offset, byte[] b){
		buff.position(offset);
		buff.putInt(b.length);
		buff.put(b);
		dirty = true;
		updateSize(buff.position());
		return b.length;
	}
	
	public synchronized int putDecimal(int offset, BigDecimal value) {
		int ans = 0;
		ans += putInt(offset, value.scale());
		offset += 4;
		ans += putBytes(offset, value.unscaledValue().toByteArray());
		dirty = true;
		return ans;
	}
	
	public synchronized int putInt(int offset, int value) {
		dirty = true;
		buff.position(offset);
		buff.putInt(value);
		updateSize(buff.position());
		return Integer.SIZE / Byte.SIZE;
	}
	@Override
	public void flush() throws Throwable {
		// TODO Auto-generated method stub
		
	}
}
