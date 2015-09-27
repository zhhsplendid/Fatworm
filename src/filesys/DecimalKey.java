package filesys;

import java.math.BigDecimal;

import util.Lib;

public class DecimalKey extends BKey {
	public long time;
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
		Page page = manager.getPage(pageID, false);
		int offset = address % MOD_MASK;
		/*
		int curOffset = 8;
		for(int i = 0; i < offset; ++i){
			curOffset = curOffset + 4 + page.getInt(curOffset);
		}
		curOffset += 4;*/
		int curOffset = 12;
		for(int i = 0; i < offset; ++i){
			curOffset += page.getInt(curOffset);
		}
		curOffset += (offset << 2);
		value = page.getBigDecimal(curOffset);
	}
	
	public void toPage() throws Throwable{
		int needSize = 4 + Page.getSize(value);
		Page page = manager.newPage(needSize);
		//page.writeBegin();
		int curOffset = 4;
		pageID = page.getID();
		int count = page.count;
		page.newEntry();
		address = pageID * MOD_MASK + count;
		curOffset += 4;
		for(int i = 0; i < count; ++i){
			curOffset += page.getInt(curOffset);
		}
		curOffset += (count << 2);
		int length = page.putDecimal(curOffset + 4, value);
		page.putInt(curOffset, length);
		page.writeOver();
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
