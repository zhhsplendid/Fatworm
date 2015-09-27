package value;

import java.util.List;

import scan.Scan;
import util.Env;
import datatype.Bool;
import datatype.DataRecord;

public class ExistExpr extends Expr {
	
	public boolean isNot; // expr is NOT EXIST or EXIST
	public Scan scan;
	
	public ExistExpr(Scan s, boolean b){
		super();
		isNot = b;
		scan = s;
		type = java.sql.Types.BOOLEAN;
		aggregation.addAll(scan.getAggregation());
	}
	
	@Override
	public boolean valuePredicate(Env env) {
		scan.eval(env);
		return scan.hasNext() ^ isNot;
	}

	@Override
	public DataRecord valueExpr(Env env) {
		return new Bool(valuePredicate(env));
	}

	@Override
	public String toString(){
		if(isNot){
			return "NOT EXIST " + scan.toString();
		}
		else{
			return "EXIST " + scan.toString();
		}
	}
	
	@Override
	public List<String> requestCol() {
		return scan.requestCol();
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
		return new ExistExpr(scan, isNot);
	}

}
