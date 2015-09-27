package fatworm.driver;

import java.util.ArrayList;

import filesys.RecordPage;

public class RecordCursor implements Cursor {
	
	Integer pageID;
	int offset;
	boolean isEnd;
	IOTable table;
	ArrayList<Record> cache = new ArrayList<Record>();
	
	public RecordCursor(IOTable t){
		table = t;
		beforeFirst();
	}
	
	@Override
	public void beforeFirst(){
		pageID = table.firstPageId;
		offset = 0;
		isEnd = false;
		cache = getRecords(pageID);
	}
	
	private ArrayList<Record> getRecords(Integer pid){
		return new ArrayList<Record>(DatabaseEngine.getInstance().recordManager.getRecords(pid, table.schema));
	}
	
	private Integer nextPage() throws Throwable{
		return DatabaseEngine.getInstance().recordManager.getNextPage(pageID);
	}
	
	private Integer prevPage() throws Throwable {
		return DatabaseEngine.getInstance().recordManager.getPrevPage(pageID);
	}
	
	public boolean appendThisPage(Record r){
		try {
			boolean flag = DatabaseEngine.getInstance().recordManager.getRecordPage(pageID, false).tryAddRecord(r);
			if(flag){
				cache = getRecords(pageID);
			}
			return flag;
		} catch (Throwable e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public void updateRecord(Record record) throws Throwable{
		DatabaseEngine.getInstance().recordManager.getRecordPage(pageID, false).delRecord(table.schema, offset);
		DatabaseEngine.getInstance().recordManager.getRecordPage(pageID, false).updateRecord(record, offset);
		
	}
	@Override
	public boolean hasNext() throws Throwable {
		return !nextPage().equals(table.firstPageId);
		//**
	}

	@Override
	public void next() throws Throwable {
		if(offset < cache.size() - 1){
			++offset;
		}
		else{
			isEnd = !hasNext();
			offset = 0;
			pageID = nextPage();
			cache = getRecords(pageID);
		}
	}

	@Override
	public void prev() throws Throwable {
		if(offset > 0){
			--offset;
		}
		else {
			pageID = prevPage();
			cache = getRecords(pageID);
			offset = cache.size() - 1;
		}
	}

	@Override
	public void delete() throws Throwable {
		RecordPage page = DatabaseEngine.getInstance().recordManager.getRecordPage(pageID, false);
		page.delRecord(table.schema, offset);
		if(offset >= cache.size() - 1){
			isEnd = !hasNext();
			offset = 0;
			pageID = nextPage();
			cache = getRecords(pageID);
		}else{
			cache.remove(offset);
		}
		boolean flag = page.canDrop();
		if(flag && !pageID.equals(table.firstPageId)){
			page.drop();
			table.firstPageId = pageID;
		}
	}

	@Override
	public Object getObject(String col) {
		return getRecord().getField(col);
	}

	@Override
	public Object[] getObject() {
		return getRecord().cols.toArray();
	}

	@Override
	public Record getRecord() {
		return cache.get(offset);
	}

	@Override
	public Integer getIndex() {
		int ans = pageID * MOD + offset;
		return ans;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasCursor() {
		return pageID >= 0 && !isEnd && offset < cache.size();
	}

}
