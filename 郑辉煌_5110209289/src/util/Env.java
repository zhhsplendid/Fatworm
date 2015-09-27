package util;

import java.util.*;

import value.Expr;

import datatype.DataRecord;
import datatype.Null;
import fatworm.driver.Record;

public class Env {

	public static int count = 0;
	
	public HashMap<String, DataRecord> nameMap;
	
	public Env(){
		nameMap = new HashMap<String, DataRecord>();
	}
	
	public Env(HashMap<String, DataRecord> map){
		nameMap = map;
	}
	
	public synchronized static int getNewCount(){
		return count++;
	}
	
	public void put(String s, DataRecord d){
		nameMap.put(s.toLowerCase(), d);
	}
	
	public DataRecord get(String a){
		return nameMap.get(a.toLowerCase());
	}
	
	public DataRecord get(String table, String col){
		DataRecord ans = get(col);
		if(ans == null){
			ans = get(table + "." + Lib.getAttributeName(col));
		}
		if(ans == null){
			ans = Null.getInstance();
		}
		return ans;
	}
	
	public DataRecord remove(String s){
		return nameMap.remove(s);
	}
	
	public DataRecord remove(String table, String col){
		DataRecord ans = null;
		if(nameMap.containsKey(col.toLowerCase())){
			ans = nameMap.remove(col.toLowerCase());
		}
		
		String doubleName = table + Lib.getAttributeName(col);
		if(nameMap.containsKey(doubleName)){
			ans = nameMap.remove(doubleName);
		}
		return ans;
	}
	
	/*
	public String toString(){
		return Lib.mapToString(nameMap);
	}*/
	
	public Env clone(){
		return new Env(new HashMap<String, DataRecord>(this.nameMap));
	}
	
	public void appendFromRecord(Record r){
		if(r == null){
			return;
		}
		List<String> onlyColName = r.schema.getOnlyColName();
		List<String> colWithTableName = r.schema.getColWithTableName();
		for(int i = 0; i < r.cols.size(); ++i){
			DataRecord tmp = r.getField(i);
			put(onlyColName.get(i), tmp);
			put(colWithTableName.get(i), tmp);
		}
	}
	
	public void appendFromRecord(List<String> name, List<Integer> offset, List<DataRecord> cols){
		for(int i = 0; i < name.size(); ++i){
			int j = offset.get(i);
			if(j < 0){
				continue;
			}
			put(name.get(i), cols.get(j));
		}
	}
	
	public void appendAlias(String table, List<Expr> expr, List<String> alias){
		for(int i = 0; i < alias.size(); ++i){
			String colName = expr.get(i).toString();
			put(alias.get(i), get(table, colName));
			put(table + "." + alias.get(i), get(table, colName));
		}
	}
	
	public void appendFrom(Env e){
		if(e == null){
			return;
		}
		nameMap.putAll(e.nameMap);
	}
}
