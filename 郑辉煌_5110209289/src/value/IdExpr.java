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
	
	@Override
	public boolean valuePredicate(Env env) {
		return Lib.toBoolean(valueExpr(env));
	}

	@Override
	public DataRecord valueExpr(Env env) {
		if(name.equalsIgnoreCase("null")){
			return Null.getInstance();
		}
		DataRecord ans = env.get(name);
		if(ans == null){
			ans = env.get(Lib.getAttributeName(name));
			if(ans == null){
				Debug.warn("not found Id");
			}
		}
		return ans;
	}
	
	public DataRecord valueByIndex(Record r){
		return r.cols.get(index);
	}
	/*
	public void indexOf(Schema schema){
		index = schema.indexOfStrictString(name);
	}*/
	
	@Override
	public int getType(Schema schema){
		Column col = schema.getColumn(name);
		if(col != null){
			return col.type;
		}
		return java.sql.Types.NULL;
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
		if((!name.contains(".") && Lib.getAttributeName(oldName).equalsIgnoreCase(name))){
			name = newName;
			return;
		}
		if(oldName.equalsIgnoreCase(name)){
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
