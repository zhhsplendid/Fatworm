package scan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import main.Main;

import output.Debug;

import util.Env;
import util.Lib;
import value.BinaryExpr;
import value.BinaryOp;
import value.IdExpr;
import fatworm.driver.*;



public class TableScan extends Scan {
	public IOTable table;
	public String name; //table name
	Cursor cursor;
	Schema schema;
	public LinkedList<String> orderCol = new LinkedList<String>();
	
	public TableScan(String tableName){
		super();
		name = tableName;
		table = DatabaseEngine.getInstance().getTable(name);
		if(table == null){
			Debug.err("error in TableScan");
		}
		Schema tmpSchema = table.getSchema();
		schema = new Schema(tmpSchema.tableName);
	
		for(String old: tmpSchema.columnName){
			String now = tmpSchema.tableName + "." + Lib.getAttributeName(old);
			schema.colMap.put(now, tmpSchema.getColumn(old));
			schema.columnName.add(now);
		}
		schema.primaryKey = tmpSchema.primaryKey;	
	}
	
	@Override
	public void beforeFirst() {
		try{
			cursor.beforeFirst();
		} catch(Throwable e){
			e.printStackTrace();
		}
	}

	@Override
	public Record next() {
		Record ans = cursor.getRecord();
		try{
			cursor.next();
		} catch(Throwable e){
			e.printStackTrace();
		}
		return ans;
	}

	@Override
	public boolean hasNext() {
		return cursor.hasCursor();
	}

	@Override
	public String toString() {
		return "TableScan (" + name + ")";
	}

	@Override
	public void close() {
		cursor.close();
	}

	@Override
	public Schema getSchema() {
		return schema;
	}
	
	private boolean idAndConst(BinaryExpr be){
		
		if((be.left instanceof IdExpr && be.right.isConst) || (be.right instanceof IdExpr && be.left.isConst)){
			return true;
		}
		
		return false;
	}
	@Override
	public void eval(Env env) {
		hasComputed = true;
		
		if(toScan instanceof SelectScan && Main.indexMode){
			ArrayList<Condition> condList = new ArrayList<Condition>();
			HashMap<String, Range> condInt = new HashMap<String, Range>();
			HashMap<String, ArrayList<Condition>> condGroup = new HashMap<String, ArrayList<Condition>>();
			
			SelectScan select = (SelectScan) toScan;
			
			while(true){
				if(select.expr instanceof BinaryExpr){
					BinaryExpr be = (BinaryExpr)select.expr;
					if(be.op != BinaryOp.NOT_EQUAL && idAndConst(be)){
						Condition cond = new Condition(be);//
						condList.add(cond);
						if(!condGroup.containsKey(cond.name)){
							condGroup.put(cond.name, new ArrayList<Condition>());
						}
						condGroup.get(cond.name).add(cond);
					}
				}
				if(select.toScan instanceof SelectScan){
					select = (SelectScan) select.toScan;
				}
				else{
					break;
				}
			}
			
			
			boolean notEmpty = true;
			for(Map.Entry<String, ArrayList<Condition>> entry: condGroup.entrySet()){
				ArrayList<Condition> conditions = entry.getValue();
				Range r = new Range();
				for(Condition cond: conditions){
					r.intersect(cond.getRange());
					if(r.isEmpty()){
						notEmpty = false;
						break;
					}
				}
				if(!notEmpty){
					break;
				}
				condInt.put(entry.getKey(), r);
			}
			
			if(!notEmpty){
				cursor = new EmptyCursor();
				return;
			}
			
			LinkedList<String> condName = new LinkedList<String>(condInt.keySet());
			int minMeasure = Integer.MIN_VALUE;
			Index bestIndex = null;
			Range bestRange = null;
			
			for(String condname: condName){
				if(table.hasIndexOnTable(condname)){
					Range r = condInt.get(condname);//
					Index i = table.getIndexOnTable(condname);
					
					int measure = r.measureRange();
					if(measure < minMeasure || bestIndex == null){
						minMeasure = measure;
						bestRange = r;
						bestIndex = i;
					}
				}
			}
			if(bestRange != null){
				if(table instanceof IOTable){
					boolean isEnd = false;
					select = (SelectScan) toScan;
					while(true){
						if(select.expr instanceof BinaryExpr){
							BinaryExpr tmp = (BinaryExpr)select.expr;
							if((tmp.op == BinaryOp.GREATER_EQ || tmp.op == BinaryOp.LESS_EQ) && idAndConst(tmp)){
								Condition cond = new Condition((BinaryExpr)select.expr);
								if(cond.name.equalsIgnoreCase(bestIndex.column.name)){
									select.skip = true;
									if(!Lib.isNull(bestRange.max) && cond.value.cmp(BinaryOp.EQUAL, bestRange.max))
										isEnd = true;
								}
							}
						}
						if(select.toScan instanceof SelectScan){
							select = (SelectScan)select.toScan;
						}
						else {
							break;
						}
					}
					IOTable ioTable = (IOTable) table;
					cursor = ioTable.strictIndexCursor(bestIndex, bestRange.min, bestRange.max, isEnd);
					
					
					return;
				}
			}
		}
		
		cursor = table.newCursor();
	}

	@Override
	public void rename(String oldName, String newName) {
		
	}

	@Override
	public List<String> getCol() {
		return new LinkedList<String>(schema.columnName);
	}

	@Override
	public List<String> requestCol() {
		return new LinkedList<String>();
	}

}
