package value;

import java.util.List;

import scan.Scan;
import util.Env;
import util.Lib;
import datatype.*;

public class AllAndAnyExpr extends Expr {
	
	boolean isAll;
	public Scan scan;
	public Expr expr;
	public BinaryOp op;
	
	public AllAndAnyExpr(Scan s, Expr e, BinaryOp b, boolean isAll){
		super();
		this.isAll = isAll;
		scan = s;
		expr = e;
		op = b;
		aggregation.addAll(expr.getAggr());
		aggregation.addAll(scan.getAggregation());
		
		type = java.sql.Types.BOOLEAN;
	}
	
	@Override
	public boolean valuePredicate(Env env) {
		DataRecord base = expr.valueExpr(env);
		scan.eval(env);
		
		if(isAll){
			while(scan.hasNext()){
				if(! base.cmp(op, scan.next().cols.get(0))){
					return false;
				}
			}
			return true;
		}
		else{
			while(scan.hasNext()){
				if(base.cmp(op, scan.next().cols.get(0))){
					return true;
				}
			}
			return false;
		}
	}
	@Override
	public String toString(){
		return expr.toString() + op.toString() + 
				(isAll ? "all ": "any ") + scan.toString();
	}
	
	@Override
	public DataRecord valueExpr(Env env) {
		return new Bool(valuePredicate(env));
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
		return true;
	}

	@Override
	public Expr clone() {
		return new AllAndAnyExpr(scan, expr.clone(), op, isAll);
	}

}
