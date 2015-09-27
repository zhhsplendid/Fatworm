package fatworm.driver;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import output.Debug;
import filesys.BKey;
import filesys.BTreePage.BTreeCursor;
import filesys.BKeyManager;

public class Database implements Serializable{
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = -2093017141834730375L;
	
	public String name;
	public HashMap<String, IOTable> tableMap;
	public HashMap<String, Index> indexMap;
	
	public Database(String s){
		name = s;
		tableMap = new HashMap<String, IOTable>();
		indexMap = new HashMap<String, Index>();
	}
	
	public void addTable(String s, IOTable t){
		tableMap.put(s, t);
	}
	
	public void removeTable(String s){
		tableMap.remove(s);
	}
	
	public IOTable getTable(String s){
		return tableMap.get(s);
	}
	
	
	
	public static void createIndex(Record record, Index index, RecordCursor cursor, BKeyManager b) throws Throwable{
		BKey key = b.newBKey(record.getField(index.colIndex));
		BTreeCursor bc = b.root.cursorOfKey(key);
		boolean unique = index.unique;
		if(! unique){
			if(! bc.valid() || !bc.getKey().equals(key)){
				ArrayList<Integer> tmp = new ArrayList<Integer>();
				Integer idx = cursor.getIndex();
				tmp.add(idx);
				bc.insert(key, index.bucket.size());
				index.bucket.add(tmp);
			}
			else{
				Integer idx = cursor.getIndex();
				int val = bc.getValue();
				index.bucket.get(val).add(idx);
			}
		}
		else{
			if(bc.valid() && bc.getKey().equals(key)){
				Debug.warn("duplicate key");
			}
			bc.insert(key, cursor.getIndex());
		}
	}
	
	
	public void dropIndex(String indexName){
		Index index = indexMap.remove(indexName);
		try {
			DatabaseEngine dbeng = DatabaseEngine.getInstance();
			new BKeyManager(dbeng.btreeManager, index.pageID, index.column.type, index.table).root.drop();
		} catch (Throwable e) {
			e.printStackTrace();
			//Debug.err(e.getMessage());
		}
		
		index.table.removeIndexEntry(index);
	}

	public void createIndex(String indexName, String colName, boolean isUnique, IOTable table) {
		Column col = table.schema.getColumn(colName);
		Index index = new Index(table, col, indexName, isUnique);
		DatabaseEngine dbeng = DatabaseEngine.getInstance();
		try {
			BKeyManager b = new BKeyManager(dbeng.btreeManager,  table, col.type);
			for(RecordCursor c = table.newCursor(); c.hasNext(); c.next()){
				Record record = c.getRecord(); 
				createIndex(record, index, c, b);
			}
			index.pageID = b.root.getID();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		indexMap.put(indexName, index);
		table.addIndexEntry(index);
	}

	
}
