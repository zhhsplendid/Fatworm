package value;

import java.util.LinkedList;
import java.util.List;

import util.Env;
import util.Lib;
import datatype.DataRecord;
import datatype.Null;
import fatworm.driver.Record;
import fatworm.driver.Schema;
import scan.Scan;

public class QueryExpr extends Expr {
	public static long time;
	public Scan scan;
	
	public QueryExpr(Scan s){
		super();
		scan = s;
		aggregation.addAll(scan.getAggregation());
		Schema tmpSchema = scan.getSchema();
		type = tmpSchema.getColumn(0).type;
	}

	@Override
	public DataRecord valueExpr(Env env) {
		scan.eval(env);
		if(scan.hasNext()){
			Record r = scan.next();
			return r.getField(0);
		}
		else return Null.getInstance();
	}
	
	@Override
	public boolean valuePredicate(Env env) {
		return Lib.toBoolean(valueExpr(env));
	}
	
	@Override 
	public String toString(){
		return "@QueryExpr(" + scan.toString() + ")";
	}
	
	@Override
	public List<String> requestCol() {
		return new LinkedList<String>();
	}

	@Override
	public void rename(String oldName, String newName) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasSubquery() {
		return true;
	}

	@Override
	public Expr clone() {
		return new QueryExpr(scan);
	}

}
