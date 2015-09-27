package value;

import java.util.LinkedList;
import java.util.List;

import output.Debug;

import util.Env;
import util.Lib;
import datatype.DataRecord;
import datatype.Null;
import fatworm.driver.Column;
import fatworm.driver.Record;
import fatworm.driver.Schema;

public class IdExpr extends Expr {
	public static long time;
	public String name;
	public int index = -1;
	
	public IdExpr(String s){
		super();
		size = 1;
		name = s;
	}
	
	public String toString(){
		return name;
	}
	
	public DataRecord valueByIndex(Record r){
		return r.getField(index);
	}
	/*
	public int indexOf(Schema schema){
		index = schema.indexOfStrictString(name);
		return index;
	}*/
	
	@Override
	public boolean valuePredicate(Env env) {
		return Lib.toBoolean(valueExpr(env));
	}

	@Override
	public DataRecord valueExpr(Env env) {
		boolean nameNull = name.equalsIgnoreCase("null");
		if(nameNull){//may be wrong
			return Null.getInstance();
		}
		DataRecord ans = env.get(name);
		nameNull = ans == null;
		if(nameNull){
			ans = env.get(Lib.getAttributeName(name));
		}
		if(ans == null){
			Debug.warn("what's wrong in IdExpr.java");
		}
		return ans;
	}
	
	
	
	@Override
	public int getType(Schema schema){
		Column col = schema.getColumn(name);
		if(col != null){
			return col.type;
		}
		else{
			return java.sql.Types.NULL;
		}
	}
	
	@Override
	public List<String> requestCol() {
		List<String> list = new LinkedList<String>();
		list.add(name);
		return list;
	}

	@Override
	public void rename(String oldName, String newName) {
		// FIXME maybe wrong
		boolean diff = (!name.contains(".") 
				&& Lib.getAttributeName(name).equalsIgnoreCase(Lib.getAttributeName(oldName)));
		if(diff){
			name = newName;
			return;
		}
		boolean same = oldName.equalsIgnoreCase(name);
		if(same){
			name = newName;
			return;
		}
	}

	@Override
	public boolean hasSubquery() {
		return false;
	}

	@Override
	public Expr clone() {
		return new IdExpr(name);
	}
	
	
}
