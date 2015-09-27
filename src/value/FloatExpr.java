package value;

import java.util.LinkedList;
import java.util.List;

import util.Env;
import util.Lib;
import datatype.*;
import datatype.Float;

public class FloatExpr extends Expr {
	public static long time;
	public Float data;
	
	public FloatExpr(double d){
		super();
		size = 1;
		isConst = true;
		data = new Float(d);
		type = java.sql.Types.FLOAT;
		value = data;
	}
	
	@Override
	public String toString(){
		return data.toString();
	}
	
	@Override
	public boolean valuePredicate(Env env) {
		return Lib.toBoolean(data);
	}

	@Override
	public DataRecord valueExpr(Env env) {
		return data;
	}
	
	@Override
	public Expr clone() {
		// TODO Auto-generated method stub
		return new FloatExpr(data.value);
	}
	
	@Override
	public List<String> requestCol() {
		return new LinkedList<String>();
	}

	@Override
	public boolean hasSubquery() {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public void rename(String oldName, String newName) {
		// TODO Auto-generated method stub

	}

}
