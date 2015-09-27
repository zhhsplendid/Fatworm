package fatworm.driver;

import java.util.List;

import filesys.*;
import filesys.BTreePage.BTreeCursor;

public class IndexCursor implements Cursor {
	
	
	
	Index index;
	int idx;
	BKeyManager btree;
	BTreeCursor bcursor;
	BTreeCursor first;
	BTreeCursor last;
	
	IOTable table;
	
	public IndexCursor(Index i, IOTable t) throws Throwable{
		btree = new BKeyManager(DatabaseEngine.getInstance().btreeManager, index.pageID, index.column.type, index.table);
		index = i;
		table = t;
		beforeFirst();
	}
	
	public IndexCursor(Index i, BTreeCursor f, BTreeCursor l, IOTable t) throws Throwable{
		this.btree = new BKeyManager(DatabaseEngine.getInstance().btreeManager, index.pageID, index.column.type, index.table);
		first = f;
		last = l;
		this.index = i;
		table = t;
		beforeFirst();
	}
	
	public void first(){
		bcursor = first;
		if(! index.unique){
			idx = 0;
		}
	}
	
	@Override
	public void beforeFirst() throws Throwable {
		if(first == null){
			first = btree.root.firstCursor();
		}
		if(last == null){
			last = btree.root.firstCursor();
		}
		first();
	}

	@Override
	public boolean hasNext() throws Throwable {
		if(bcursor != null && bcursor != last && (! index.unique) && idx < index.bucket.get(bcursor.getValue()).size() - 1){
			return true;
		}
		else if(bcursor.hasNext()){
			return true;
		}
		return false;
	}

	@Override
	public void next() throws Throwable {
		if(index.unique){
			if(bcursor.hasNext()){
				nextBCursor();
			}
			else{
				bcursor = null;
			}
		}
		else{
			++idx;
			if(idx >= index.bucket.get(bcursor.getValue()).size()){
				if(bcursor.hasNext()){
					nextBCursor();
					idx = 0;
				}
				else{
					bcursor = null;
				}
			}
		}
	}

	
	private void nextBCursor() throws Throwable{
		if(bcursor.equals(last)){
			bcursor = null;
		}
		else{
			bcursor = bcursor.next();
		}
	}
	
	@Override
	public void prev() throws Throwable {
		if(index.unique){
			if(bcursor.hasPrev()){
				prevBCursor();
			}
			else{
				bcursor = null;
			}
		}else {
			idx--;
			if(idx < 0){
				if(bcursor.hasPrev()){
					prevBCursor();
					idx = index.bucket.get(bcursor.getValue()).size() - 1;
				}else
					bcursor = null;
			}
		}
	}

	private void prevBCursor() throws Throwable {
		if(bcursor.equals(first)){
			bcursor = null;
		}
		else{
			bcursor = bcursor.prev();
		}
	}

	@Override
	public void delete() throws Throwable {
		// TODO Auto-generated method stub

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
		int value;
		if(index.unique){
			value = bcursor.getValue();
		}
		else{
			value = index.bucket.get(bcursor.getValue()).get(idx);
		}
		
		int pid = value / MOD;
		int offset = value % MOD;
		return getRecords(pid).get(offset);
	}
	
	private List<Record> getRecords(int pid){
		return DatabaseEngine.getInstance().recordManager.getRecords(pid, table.schema);
	}
	@Override
	public Integer getIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	@Override
	public boolean hasCursor() {
		return bcursor != null && bcursor.valid();
	}

}
