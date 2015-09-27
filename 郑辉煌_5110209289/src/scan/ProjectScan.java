package scan;

import java.util.LinkedList;
import java.util.List;

import util.Env;
import util.Lib;
import value.Expr;
import fatworm.driver.Column;
import fatworm.driver.Record;
import fatworm.driver.Schema;

public class ProjectScan extends Scan {
	
	public Scan fromScan;
	private Schema schema;
	private Env env;
	public List<Expr> exprs;
	//private List<String> names;
	
	boolean hasNullCol;
	boolean projectAll;
	
	public ProjectScan(Scan from, List<Expr> e, boolean hasProjectAll){
		super();
		
		fromScan = from;
		fromScan.toScan = this; 
		aggregation.addAll(fromScan.getAggregation());
		exprs = e;
		projectAll = hasProjectAll;
		
		schema = new Schema();
		if(projectAll){
			Schema src = fromScan.getSchema();
			schema.colMap.putAll(src.colMap);
			schema.columnName.addAll(src.columnName);
		}
		
		schema.fromList(exprs, fromScan.getSchema());
		//names = schema.columnName;
		//
		//if(exprs.size() == 1){
			//Expr expr = exprs.get(0);
			//TODO
		//}
	}
	
	public boolean isConst(){
		for(Expr e: exprs){
			if(! e.isConst){
				return false;
			}
		}
		return true;
	}
	
	@Override
	public void beforeFirst() {
		fromScan.beforeFirst();
	}

	@Override
	public Record next() {
		Env local = env.clone();
		Record record = fromScan.next();
		Record ans = new Record(schema);
		
		if(projectAll){
			ans.cols.addAll(record.cols);
		}
		//TODO
		local.appendFromRecord(record);
		ans.addFieldFromExpr(local, exprs);
		return ans;
	}

	@Override
	public boolean hasNext() {
		return fromScan.hasNext();
	}

	@Override
	public String toString() {
		return "Project(" + fromScan.toString() + ")";
	}

	@Override
	public void close() {
		fromScan.close();
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void eval(Env env) {
		hasComputed = true;
		fromScan.eval(env);
		this.env = env;
		for(Column col: schema.colMap.values()){
			if(col.type == java.sql.Types.NULL){
				col.type = java.sql.Types.VARCHAR;
			}
		}
	}

	@Override
	public void rename(String oldName, String newName) {
		fromScan.rename(oldName, newName);
		for(Expr e: exprs){
			if(e.getType(fromScan.getSchema()) == java.sql.Types.NULL){
				e.rename(oldName, newName);
			}
		}
	}

	@Override
	public List<String> getCol() {
		return new LinkedList<String>(schema.columnName);
	}

	@Override
	public List<String> requestCol() {
		List<String> list = new LinkedList<String>(fromScan.requestCol());
		Lib.removeRepeat(list, fromScan.getCol());
		return list;
	}

}
