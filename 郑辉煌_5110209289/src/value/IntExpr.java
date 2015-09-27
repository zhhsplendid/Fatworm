package value;

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;

import util.Env;
import datatype.*;

public class IntExpr extends Expr {
	
	public DataRecord data;
	
	public IntExpr(BigInteger b){
		super();
		data = new Decimal(b.toString());
		size = 1;
		type = java.sql.Types.DECIMAL;
		value = data;
		isConst = true;
	}
	
	public IntExpr(int x){
		super();
		data = new Int(x);
		size = 1;
		type = java.sql.Types.INTEGER;
		value = data;
		isConst = true;
	}
	
	@Override
	public String toString(){
		return data.toString();
	}
	
	@Override
	public boolean valuePredicate(Env env) {
		return data.cmp(BinaryOp.EQUAL, new Int(0));
	}

	@Override
	public DataRecord valueExpr(Env env) {
		return data;
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
		if(type == java.sql.Types.INTEGER){
			return new IntExpr(((Int)data).value);
		}
		else{
			return new IntExpr(((Decimal)data).value.toBigIntegerExact());
		}
	}

}
