package value;

import java.util.List;

import scan.Scan;
import util.Env;
import util.Lib;
import datatype.Bool;
import datatype.DataRecord;

public class InExpr extends Expr {

	public boolean not; // NOT IN or IN
	public Scan scan;
	public Expr expr;
	
	public InExpr(Scan s, Expr e, boolean isNotIn){
		super();
		not = isNotIn;
		scan = s;
		expr = e;
		type = java.sql.Types.BOOLEAN;
		aggregation.addAll(scan.getAggregation());
		aggregation.addAll(expr.getAggr());
	}
	
	@Override
	public boolean valuePredicate(Env env) {
		DataRecord base = expr.valueExpr(env);
		scan.eval(env);
		
		boolean tmp = false;
		while(scan.hasNext()){
			if(base.cmp(BinaryOp.EQUAL, scan.next().cols.get(0))){
				tmp = true;
				break;
			}
		}
		return not ^ tmp;
	}

	@Override
	public DataRecord valueExpr(Env env) {
		return new Bool(valuePredicate(env));
	}

	@Override
	public String toString(){
		return expr.toString() + (not ? " NOT": "") + " IN " + scan.toString();
	}
	
	@Override
	public List<String> requestCol() {
		List<String> list = scan.requestCol();
		Lib.addAllCol(list, expr.requestCol());
		return list;
	}

	@Override
	public void rename(String oldName, String newName) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasSubquery() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Expr clone() {
		return new InExpr(scan, expr.clone(), not);
	}

}
