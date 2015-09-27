package fatworm.driver;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.tree.Tree;

import datatype.DataRecord;
import datatype.Int;
import datatype.Null;

import output.Debug;

import util.Env;
import util.Lib;
import value.Expr;

import main.Main;

import filesys.BKeyManager;
import filesys.BTreePage.BTreeCursor;
import filesys.BufferManager;
import filesys.MyFile;
import filesys.RecordPage;

/**
 * This class is most important for file system connecting to database 
 * It's a table interface and IO interface, including add record to file system and give records to TableScan
 * @author bd
 *
 */
public class IOTable implements Serializable{
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = 6386599490606303402L;
	
	public Schema schema;
	public Integer firstPageId;
	public ArrayList<Index> tableIndex = new ArrayList<Index>();
	
	public IOTable(){
		firstPageId = -1;
		tableIndex = new ArrayList<Index>();
	}
	
	public IOTable(Tree t){
		firstPageId = -1;
		tableIndex = new ArrayList<Index>();
		schema = new Schema(t);
		if(Main.indexMode && schema.primaryKey != null){
			DatabaseEngine dbeng = DatabaseEngine.getInstance();
			Database db = dbeng.getDatabase();
			db.createIndex(Lib.primaryKeyIndexName(schema.primaryKey.name), schema.primaryKey.name, true, this);
		}
	}
	
	public void removeIndexEntry(Index idx){
		tableIndex.remove(idx);
	}
	
	public void addIndexEntry(Index idx){
		tableIndex.add(idx);
	}
	
	public IndexCursor newIndexCursor(Index index){
		try{
			return new IndexCursor(index, this);
		} catch(Throwable e){
			e.printStackTrace();
			return null;
		}
	}
	
	public IndexCursor strictIndexCursor(Index index, DataRecord left, DataRecord right, boolean isEnd){
		try {
			DatabaseEngine dbeng = DatabaseEngine.getInstance();
			BKeyManager b = new BKeyManager(dbeng.btreeManager, 
					index.pageID, index.column.type, index.table);
			BTreeCursor first = null;
			if(left != null && left.type != java.sql.Types.NULL){
				first = b.root.cursorOfKey(b.newBKey(left));
				first = first.adjust();
				//
			}
			
			BTreeCursor last = null;
			if(right != null && right.type != java.sql.Types.NULL){
				last = b.root.cursorOfKey(b.newBKey(right));
				//
			}
			if(last!=null){
				if(isEnd){
					last = last.adjust();
				}
				else{
					last = last.adjustLeft();
				}
				
				boolean flag = isEnd && last.getKey().compareTo(b.newBKey(right)) > 0;
				if(flag){
					last = last.prev();
				}
			}
			
			return new IndexCursor(index, first, last, this);
		} catch (Throwable e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public int update(List<String> colName, List<Expr> expr, Expr e) {
		RecordCursor cursor = newCursor();
		int ans = 0;
		try { //
			while(cursor.hasNext()){
				Record record = cursor.getRecord();
				Env env = new Env();
				env.appendFromRecord(record);
				if(e != null && !e.valuePredicate(env)){
					cursor.next();
					continue;
				}
				int size = expr.size(); 
				for(int i = 0; i < size; ++i){
					Expr ex = expr.get(i);
					DataRecord res = ex.valueExpr(env);
					Schema tmps = record.schema;
					int idx = tmps.indexOf(colName.get(i));
					DataRecord toSet = DataRecord.fromString(tmps.getColumn(idx).type, res.toString());
					record.cols.set(idx, toSet);
					env.put(colName.get(i), res);
				}
				ans++;
				cursor.updateRecord(record);
				cursor.next();
			}
		} catch (Throwable e1) {
			e1.printStackTrace();
		}
		return ans;
	}
	
	public RecordCursor newCursor() {
		return new RecordCursor(this);
	}
	
	
	public void addRecord(Record r) {
		RecordCursor cursor = newCursor();
		DatabaseEngine dbeng = DatabaseEngine.getInstance();
		try{
			if(! cursor.hasNext()){
				BufferManager manager = dbeng.recordManager;
				firstPageId = manager.newPage();
				RecordPage now = manager.getRecordPage(firstPageId, true);
				//now.writeBegin();// swap this and follow sentence?
				now.addRecord(r); //
				now.nextID = now.preID = now.getID();
				now.dirty = true;
				now.writeOver();
				createIndex(r, newCursor());
				return;
			}
			else{
				cursor.prev();
				cursor.appendThisPage(r);
				cursor.next();
				createIndex(r, cursor);
			}
		} catch(Throwable e){
			e.printStackTrace();
		}
	}
	
	private void createIndex(Record record, RecordCursor cursor){
		for(Index index : tableIndex){
			try {
				DatabaseEngine dbeng = DatabaseEngine.getInstance();
				BKeyManager b = new BKeyManager(dbeng.btreeManager, 
						index.pageID, index.column.type, this);
				Database.createIndex(record, index, cursor, b);
			} catch (Throwable e) {
//				Util.error(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	public Schema getSchema(){
		return schema;
	}
	
	public boolean hasIndexOnTable(String col){
		for(int i = 0; i < tableIndex.size(); ++i){
			Index index = tableIndex.get(i);
			if(index.column == schema.getColumn(col)){
				return true;
			}
		}
		return false;
	}
	
	public Index getIndexOnTable(String col){
		for(int i = 0; i < tableIndex.size(); ++i){
			Index index = tableIndex.get(i);
			if(index.column == schema.getColumn(col)){
				return index;
			}
		}
		return null;
	}
	
	public int insert(Tree tree){
		
		Record record = new Record(schema);
		record.autoFill();
		int ans = 0;
		int count = tree.getChildCount();
		for(int i = 0; i < count; ++i){
			Tree child = tree.getChild(i);
			String colName = schema.columnName.get(i);
			Column col = schema.getColumn(colName); 
			DataRecord value = col.getDataRecord(child);
			record.setDataRecord(colName, value);
			if(col.autoIncrement() && value instanceof Int){
				int setAut = Math.max(col.getAutoIndex(), ((Int)value).value + 1);
				col.setAutoIndex(setAut);
			}
		}
		addRecord(record);
		++ans;
		return ans;
	}
	
	public int insert(Tree colTree, Tree valueTree){
		
		Record record = new Record(schema);
		record.autoFill();
		int ans = 0;
		int count = valueTree.getChildCount();
		for(int i = 0; i < count; ++i){
			Tree child = colTree.getChild(i + 1);
			String colName = child.getText();
			DataRecord tmp = schema.getColumn(colName).getDataRecord(valueTree.getChild(i));
			record.setDataRecord(colName, tmp);
		}
		count = record.cols.size();
		for(int i = 0; i < count; ++i){
			if(record.cols.get(i) != null){
				continue;
			}
			Column col = record.schema.getColumn(i);
			if(! col.notNull){
				record.cols.set(i, Null.getInstance());
			} else {
				int val = col.getAutoIndex();
				record.cols.set(i, new Int(val));
			}
			
		}
		addRecord(record);
		++ans;
		return ans;
	}
	
	public int delete(Expr expr) throws Throwable{
		int ans = 0;
		Cursor cur = newCursor(); 
		while(cur.hasNext()){
			Record record = cur.getRecord();
			Env env = new Env();
			env.appendFromRecord(record);
			boolean bool = expr != null && !expr.valuePredicate(env);
			if(bool){
				cur.next();
				continue;
			}
			cur.delete();
		}
		return ans;
	}
	
	public void connectNewRoot(Integer id, Integer id2) {
		int size = tableIndex.size();
		Index index;
		for(int i = 0; i < size; ++i){
			index = tableIndex.get(i);
			if(index.pageID.equals(id)){
				index.pageID = id2;
			}
		}
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof IOTable){
			return schema.equals(((IOTable) o).schema);
		}
		return false;
	}

	public void deleteAll() {
		RecordCursor cursor = newCursor();
		while(cursor.hasNext()){
			try{
				cursor.delete();
			} catch (Throwable e){
				Debug.warn("delete all fail: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	
}
