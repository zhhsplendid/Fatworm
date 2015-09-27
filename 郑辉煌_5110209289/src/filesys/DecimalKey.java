package filesys;

import java.math.BigDecimal;

import util.Lib;

public class DecimalKey extends BKey {
	
	public static final int MOD_MASK = MyFile.BTREE_PAGE_SIZE / 4;
	
	public BufferManager manager;
	public BigDecimal value;
	public int address = -1;
	public int pageID = -1;
	
	public DecimalKey(BigDecimal v, BufferManager m){
		value = v;
		manager = m;
	}
	
	public DecimalKey(int v, BufferManager m) throws Throwable{
		manager = m;
		address = v;
		pageID = address / MOD_MASK;
		Page page = manager.getRawPage(pageID, false);
		int offset = address % MOD_MASK;
		int curOffset = 8;
		for(int i = 0; i < offset; ++i){
			curOffset += 4 + page.getInt(curOffset);
		}
		curOffset += 4;
		value = page.getBigDecimal(curOffset);
	}
	
	public void toPage() throws Throwable{
		Page page = manager.newRawPage(4 + Page.getSize(value));
		page.beginTransaction();
		int curOffset = 4;
		pageID = page.getID();
		int count = page.count;
		page.newEntry();
		address = pageID * MOD_MASK + count;
		curOffset += 4;
		for(int i = 0; i < count; ++i){
			curOffset += 4 + page.getInt(curOffset);
		}
		int length = page.putDecimal(curOffset + 4, value);
		page.putInt(curOffset, length);
		page.commit();
	}
	@Override
	public int compareTo(BKey o) {
		return value.compareTo(((DecimalKey)o).value);
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof DecimalKey){
			return value.equals(((DecimalKey)o).value);
		}
		return false;
	}

	@Override
	public void remove() throws Throwable {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] toByte() throws Throwable {
		toPage();
		return Lib.intToBytes(address);
	}

}
