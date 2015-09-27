package fatworm.driver;

import java.util.List;

import filesys.*;
import filesys.BTreePage.BTreeCursor;

public class IndexCursor implements Cursor {
	public long time;
	
	
	Index index;
	int idx;
	BKeyManager btree;
	BTreeCursor bcursor;
	BTreeCursor first;
	BTreeCursor last;
	
	IOTable table;
	
	public IndexCursor(Index i, IOTable t) throws Throwable{
		DatabaseEngine dbeng = DatabaseEngine.getInstance();
		index = i;
		btree = new BKeyManager(dbeng.btreeManager, index.pageID, index.column.type, index.table);
		table = t;
		beforeFirst();
	}
	
	public IndexCursor(Index i, BTreeCursor f, BTreeCursor l, IOTable t) throws Throwable{
		DatabaseEngine dbeng = DatabaseEngine.getInstance();
		index = i;
		btree = new BKeyManager(dbeng.btreeManager, index.pageID, index.column.type, index.table);
		first = f;
		last = l;
		table = t;
		beforeFirst();
	}
	
	
	@Override
	public void beforeFirst() throws Throwable {
		if(last == null){
			last = btree.root.firstCursor();
		}
		
		if(first == null){
			first = btree.root.firstCursor();
		}
		
		
		bcursor = first;
		if(! index.unique){
			idx = 0;
		}
	}

	@Override
	public boolean notEnd() throws Throwable {
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
		if(! index.unique){
			++idx;
			boolean cmp = idx >= index.bucket.get(bcursor.getValue()).size();
			if(cmp){
				if(bcursor.hasNext()){
					idx = 0;
					nextBCursor();
				}
				else{
					bcursor = null;
				}
			}
		}
		else{
			if(bcursor.hasNext()){
				nextBCursor();
			}
			else{
				bcursor = null;
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
		if(! index.unique){
			--idx;
			if(idx < 0){
				if(bcursor.hasPrev()){
					prevBCursor();
					int want = index.bucket.get(bcursor.getValue()).size();
					idx = want - 1;
				}
				else{
					bcursor = null;
				}
			}
		}
		else {
			if(bcursor.hasPrev()){
				prevBCursor();
			}
			else{
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
	public boolean hasNext() {
		return bcursor != null && bcursor.valid();
	}

}
