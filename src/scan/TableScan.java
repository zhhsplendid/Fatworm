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



public class TableScan extends ZeroFromScan {
	public long time;
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
			Debug.warn("error in TableScan");
			return;
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
		return cursor.hasNext();
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
			HashMap<String, Range> condRange = new HashMap<String, Range>();
			HashMap<String, ArrayList<Condition>> condGroup = new HashMap<String, ArrayList<Condition>>();
			
			SelectScan select = (SelectScan) toScan;
			
			while(true){
				if(select.expr instanceof BinaryExpr){
					BinaryExpr be = (BinaryExpr)select.expr;
					boolean here = be.op != BinaryOp.NOT_EQUAL && idAndConst(be);
					if(here){
						Condition cond = new Condition(be);//
						condList.add(cond);
						String stmp = cond.name;
						boolean flag = !condGroup.containsKey(stmp);
						if(flag){
							condGroup.put(stmp, new ArrayList<Condition>());
						}
						condGroup.get(stmp).add(cond);
					}
				}
				if(select.toScan instanceof SelectScan){
					select = (SelectScan) select.toScan;
				}
				else{
					break;
				}
			}
			
			
			boolean isEmpty = false;
			for(Map.Entry<String, ArrayList<Condition>> entry: condGroup.entrySet()){
				Range r = new Range();
				ArrayList<Condition> conditions = entry.getValue();
				for(Condition cond: conditions){
					r.intersect(cond.getRange());
					if(r.isEmpty()){
						isEmpty = true;
						break;
					}
				}
				if(isEmpty){
					break;
				}
				condRange.put(entry.getKey(), r);
			}
			
			if(isEmpty){
				cursor = new EmptyCursor();
				return;
			}
			
			LinkedList<String> condName = new LinkedList<String>(condRange.keySet());
			Integer minMeasure = Integer.MAX_VALUE;
			Index bestIndex = null;
			Range bestRange = null;
			
			for(String condname: condName){
				if(table.hasIndexOnTable(condname)){
					Range r = condRange.get(condname);//
					Index i = table.getIndexOnTable(condname);
					
					Integer measure = r.measureRange();
					if(bestIndex == null || measure < minMeasure){
						minMeasure = measure;
						bestRange = r;
						bestIndex = i;
					}
				}
			}
			if(bestRange != null){
				select = (SelectScan) toScan;
				boolean isEnd = false;
				
				while(true){
					if(select.expr instanceof BinaryExpr){
						BinaryExpr tmp = (BinaryExpr)select.expr;
						boolean here = (tmp.op == BinaryOp.GREATER_EQ || tmp.op == BinaryOp.LESS_EQ) && idAndConst(tmp);
						if(here){
							Condition cond = new Condition((BinaryExpr)select.expr);
							String sname = cond.name;
							if(sname.equalsIgnoreCase(bestIndex.column.name)){
								select.skip = true;
								//select.toScan.setFrom(select, select.fromScan);
								boolean btmp = !Lib.isNull(bestRange.max) && cond.value.cmp(BinaryOp.EQUAL, bestRange.max);
								isEnd |= btmp;
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
				cursor = table.strictIndexCursor(bestIndex, bestRange.min, bestRange.max, isEnd);
					
				return;
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
