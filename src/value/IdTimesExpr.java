package value;

import java.util.List;

import output.Debug;

import util.Env;
import util.Lib;
import datatype.*;
import fatworm.driver.Schema;

public class IdTimesExpr extends Expr {
	public static long longtime;
	public int time;
	public IdExpr id;
	private String name;
	
	public IdTimesExpr(IdExpr i, int t){
		id = i;
		time = t;
		size = i.size + 1;
		depth = i.depth + 1;
		isConst = i.isConst;
		if(!isConst){
			value = null;
		}
		else{
			value = new Int(valueRaw(i.value));
		}
	}
	
	private int valueRaw(DataRecord v){
		boolean flag = v.type == java.sql.Types.INTEGER;
		if(flag){
			return time * ((Int)v).value;
		}
		else if(!flag){
			Debug.err("missing type in IdTimes.java");
		}
		return 0;
	}
	
	@Override
	public boolean valuePredicate(Env env) {
		DataRecord d = valueExpr(env);
		return Lib.toBoolean(d);
	}

	@Override
	public DataRecord valueExpr(Env env) {
		if(!isConst){
			DataRecord d = id.valueExpr(env);
			return new Int(valueRaw(d));
		}
		else{
			return value;
		}
	}

	@Override
	public List<String> requestCol() {
		return id.requestCol();
	}

	@Override
	public void rename(String oldName, String newName) {
		id.rename(oldName, newName);
	}

	@Override
	public boolean hasSubquery() {
		return id.hasSubquery();
	}

	@Override
	public Expr clone() {
		return new IdTimesExpr(id, time);
	}
	
	@Override
	public int getType() {
		return java.sql.Types.INTEGER;
	}
	
	@Override
	public int getType(Schema schema) {
		return java.sql.Types.INTEGER;
	}
	
	@Override
	public String toString(){
		if(name == null){
			name = "" + Env.getNewCount();
		}
		return name;
	}

}
