package value;

import java.util.LinkedList;
import java.util.List;

import util.Env;
import util.Lib;
import datatype.DataRecord;
import datatype.Null;
import scan.Scan;

public class QueryExpr extends Expr {
	
	public Scan scan;
	
	public QueryExpr(Scan s){
		super();
		scan = s;
		aggregation.addAll(scan.getAggregation());
		type = scan.getSchema().getColumn(0).type;
	}
	
	@Override
	public boolean valuePredicate(Env env) {
		return Lib.toBoolean(valueExpr(env));
	}

	@Override
	public DataRecord valueExpr(Env env) {
		scan.eval(env);
		if(scan.hasNext()){
			return scan.next().cols.get(0);
		}
		else return Null.getInstance();
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
