package value;

import java.util.LinkedList;
import java.util.List;

import util.Env;
import datatype.DataRecord;

import datatype.*;
public class BoolExpr extends Expr {
	
	public Bool bool;
	
	public BoolExpr(boolean b){
		super();
		isConst = true;
		size = 1;
		bool = new Bool(b);
		type = java.sql.Types.BOOLEAN;
	}
	
	@Override
	public String toString(){
		return bool.toString();
	}
	
	@Override
	public boolean valuePredicate(Env env) {
		return bool.value;
	}

	@Override
	public DataRecord valueExpr(Env env) {
		return bool;
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
		return false;
	}

	@Override
	public Expr clone() {
		return new BoolExpr(bool.value);
	}

}
