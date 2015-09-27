package value;

import java.util.List;

import scan.Scan;
import util.Env;
import util.Lib;
import datatype.*;
import fatworm.driver.Record;

public class AllAndAnyExpr extends Expr {
	public static long time;
	boolean isAll;
	public Scan scan;
	public BinaryOp op;
	public Expr expr;
	boolean isAny;
	
	public AllAndAnyExpr(Scan s, Expr e, BinaryOp b, boolean isAll){
		super();
		this.isAll = isAll;
		isAny = !isAll;
		scan = s;
		expr = e;
		Lib.addAll(aggregation, expr.getAggr());
		Lib.addAll(aggregation, scan.getAggregation());
		op = b;
		type = java.sql.Types.BOOLEAN;
	}
	
	@Override
	public boolean valuePredicate(Env env) {
		DataRecord base = expr.valueExpr(env);
		scan.eval(env);
		
		if(isAny){
			while(scan.hasNext()){
				Record r = scan.next();
				if(base.cmp(op, r.getField(0))){
					return true;
				}
			}
			return false;
		}
		else{
			while(scan.hasNext()){
				Record r = scan.next();
				if(! base.cmp(op, r.getField(0))){
					return false;
				}
			}
			return true;
		}
	}
	@Override
	public String toString(){
		return expr.toString() + op.toString() + 
				((isAny && ! isAll) ? "ANY ": "ALL ") + scan.toString();
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
	public Expr clone() {
		return new AllAndAnyExpr(scan, expr.clone(), op, isAll);
	}

	

	@Override
	public void rename(String oldName, String newName) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasSubquery() {
		return true;
	}



}
