package fatworm.driver;

import fatworm.driver.Record;
import filesys.MyFile;

public interface Cursor {
	
	
	public static final int MOD = MyFile.RECORD_PAGE_SIZE / 4;
	
	public void beforeFirst() throws Throwable;
	
	public boolean notEnd() throws Throwable;
	
	public boolean hasNext();
	
	public void next() throws Throwable;
	
	public void prev() throws Throwable;
	
	public void delete() throws Throwable;
	
	
	
	public Record getRecord();
	
	public Integer getIndex();
	
	public Object getObject(String col);
	
	public Object[] getObject();
	
	public void close();

	
}
