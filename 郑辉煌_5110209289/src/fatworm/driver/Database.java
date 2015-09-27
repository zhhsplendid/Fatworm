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
	
	
	
	public static void createIndex(Index index, BKeyManager b, RecordCursor cursor, Record record) throws Throwable{
		BKey key = b.newBKey(record.getField(index.colIndex));
		BTreeCursor bc = b.root.cursorOfKey(key);
		if(index.unique){
			if(bc.valid() && bc.getKey().equals(key)){
				Debug.warn("duplicate key");
			}
			bc.insert(key, cursor.getIndex());
		}else{
			if(bc.valid() && bc.getKey().equals(key)){
				index.bucket.get(bc.getValue()).add(cursor.getIndex());
			}else{
				ArrayList<Integer> tmp = new ArrayList<Integer>();
				tmp.add(cursor.getIndex());
				bc.insert(key, index.bucket.size());
				index.bucket.add(tmp);
			}
		}
	}
	
	public void dropIndex(String indexName){
		//TODO
	}

	public void createIndex(String indexName, String col, boolean unique, IOTable table) {
		Index index = new Index(indexName, table, table.schema.getColumn(col), unique);
		//TODO
	}

	
}
