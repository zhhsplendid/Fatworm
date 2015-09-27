package filesys;

import util.Lib;

public class StringKey extends BKey {
	public long time;
	static final int MOD_MASK = MyFile.BTREE_PAGE_SIZE / 4;
	
	public BufferManager manager;
	
	public String value;
	public int pageID = -1;
	public int address = -1; // address = pageID * pageSize + offset_in_that_page

	public StringKey(String s, BufferManager m){
		value = s;
		manager = m;
	}
	
	public StringKey(int fromPosition, BufferManager m) throws Throwable{
		manager = m;
		address = fromPosition;
		pageID = address / MOD_MASK;
		Page page = manager.getPage(pageID, false);
		int offset = address % MOD_MASK;
		int curOffset = 8;
		for(int i = 0; i < offset; ++i){
			curOffset += 4 + page.getInt(curOffset);
			//four byte to the length of entry
		}
		//curOffset += (offset << 2);
		value = page.getString(curOffset);
	}
	
	public void toPage() throws Throwable{
		Page page = manager.newPage(4 + Page.getSize(value));
		//page.writeBegin();
		
		pageID = page.getID();
		int count = page.count;
		page.newEntry();
		address = pageID * MOD_MASK + count;
		int curOffset = 8;
		for(int i = 0; i < count; ++i){
			curOffset += 4 + page.getInt(curOffset);
			
		}
		//System.out.println(page.count);
		//curOffset += (count << 2);
		//System.out.println(curOffset);
		page.putString(curOffset, value);
		page.writeOver();
	} 
	
	@Override
	public int compareTo(BKey o) {
		String v = ((StringKey)o).value;
		return value.compareToIgnoreCase(v);
	}

	@Override
	public boolean equals(Object o) {
		if(o instanceof StringKey){
			return 0 == value.compareToIgnoreCase(((StringKey)o).value);
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
