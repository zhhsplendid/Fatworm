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

	/**
	 * 
	 */
	private static final long serialVersionUID = 6386599490606303402L;
	
	public Integer firstPageId;
	public Schema schema;
	public ArrayList<Index> tableIndex = new ArrayList<Index>();
	
	public IOTable(){
		firstPageId = -1;
	}
	
	public IOTable(Tree t){
		firstPageId = -1;
		schema = new Schema(t);
		if(schema.primaryKey != null && Main.indexMode){
			DatabaseEngine.getInstance().getDatabase().createIndex(
					Lib.primaryKeyIndexName(schema.primaryKey.name), schema.primaryKey.name, true, this);
		}
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
			BKeyManager b = new BKeyManager(DatabaseEngine.getInstance().btreeManager, 
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
				last = first.adjust();
				//
			}
			if(last!=null){
				if(isEnd){
					last = last.adjust();
				}
				else{
					last = last.adjustLeft();
				}
				if(isEnd && last.getKey().compareTo(b.newBKey(right)) > 0){
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
		try {
			for( ;cursor.hasCursor(); cursor.next()){
				Record record = cursor.getRecord();
				Env env = new Env();
				env.appendFromRecord(record);
				if(e != null && !e.valuePredicate(env)){
					continue;
				}
				for(int i = 0; i < expr.size(); ++i){
					DataRecord res = expr.get(i).valueExpr(env);
					int idx = record.schema.indexOf(colName.get(i));
					record.cols.set(idx, DataRecord.fromString(record.schema.getColumn(idx).type, res.toString()));
					env.put(colName.get(i), res);
				}
				cursor.updateRecord(record);
				ans++;
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
		try{
			if(! cursor.hasCursor()){
				BufferManager manager = DatabaseEngine.getInstance().recordManager;
				firstPageId = manager.newPage();
				RecordPage now = manager.getRecordPage(firstPageId, true);
				now.tryAddRecord(r);
				now.beginTransaction();
				now.dirty = true;
				now.nextID = now.preID = now.getID();
				now.commit();
				createIndex(r, newCursor());
				return;
			}
			cursor.prev();
			cursor.appendThisPage(r);
			cursor.next();
			createIndex(r, cursor);
		} catch(Throwable e){
			e.printStackTrace();
		}
	}
	
	private void createIndex(Record record, RecordCursor cursor){
		for(Index index : tableIndex){
			try {
				BKeyManager b = new BKeyManager(DatabaseEngine.getInstance().btreeManager, 
						index.pageID, index.column.type, this);
				Database.createIndex(index, b, cursor, record);
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
		int ans = 0;
		Record record = new Record(schema);
		record.autoFill();
		
		for(int i = 0; i < tree.getChildCount(); ++i){

			Tree child = tree.getChild(i);
			String colName = schema.columnName.get(i);
			Column col = schema.getColumn(colName); 
			DataRecord value = col.getDataRecord(child);
			record.setDataRecord(colName, value);
			if(col.autoIncrement() && value instanceof Int){
				col.setAutoIndex(Math.max(col.getAutoIndex(), ((Int)value).value));
			}
		}
		addRecord(record);
		++ans;
		return ans;
	}
	
	public int insert(Tree colTree, Tree valueTree){
		int ans = 0;
		Record record = new Record(schema);
		record.autoFill();
		for(int i = 0; i < valueTree.getChildCount(); ++i){
			String colName = colTree.getChild(i+1).getText();
			record.setDataRecord(colName, schema.getColumn(colName).getDataRecord(valueTree.getChild(i)));
		}
		for(int i = 0; i < record.cols.size(); ++i){
			if(record.cols.get(i) != null){
				continue;
			}
			Column col = record.schema.getColumn(i);
			if(col.notNull){
				record.cols.set(i, new Int(col.getAutoIndex()));
			} else {
				record.cols.set(i, Null.getInstance());
			}
			
		}
		addRecord(record);
		++ans;
		return ans;
	}
	
	public int delete(Expr expr) throws Throwable{
		int ans = 0;
		for(Cursor cur = newCursor(); cur.hasCursor();){
			Record record = cur.getRecord();
			Env env = new Env();
			env.appendFromRecord(record);
			if(expr != null && !expr.valuePredicate(env)){
				cur.next();
				continue;
			}
			cur.delete();
		}
		return ans;
	}
	
	public void announceNewRoot(Integer id, Integer id2) {
		for(Index index: tableIndex){
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
		while(cursor.hasCursor()){
			try{
				cursor.delete();
			} catch (Throwable e){
				Debug.err(e.getMessage());
			}
		}
	}
	
	
}
