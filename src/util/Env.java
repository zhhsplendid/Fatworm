package util;

import java.util.*;

import value.Expr;

import datatype.DataRecord;
import datatype.Null;
import fatworm.driver.Record;

public class Env {
	public long time;
	public static int count = 0;
	
	public HashMap<String, DataRecord> nameMap;
	
	public Env(){
		nameMap = new HashMap<String, DataRecord>();
	}
	
	public Env(HashMap<String, DataRecord> map){
		nameMap = map;
	}
	
	public synchronized static int getNewCount(){
		return ++count;
	}
	
	public void put(String s, DataRecord d){
		String ss = s.toLowerCase();
		nameMap.put(ss, d);
	}
	
	public DataRecord get(String a){
		String s = a.toLowerCase();
		return nameMap.get(s);
	}
	
	public DataRecord get(String table, String col){
		DataRecord ans = get(col);
		
		if(ans == null){
			String s = table + "." + Lib.getAttributeName(col);
			ans = get(s);
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
		boolean has = false; 
		has = nameMap.containsKey(col.toLowerCase());
		if(has){
			ans = nameMap.remove(col.toLowerCase());
		}
		
		String doubleName = table + Lib.getAttributeName(col);
		has = nameMap.containsKey(doubleName);
		if(has){
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
		if(r != null){
			List<String> onlyColName = r.schema.getOnlyColName();
			List<String> colWithTableName = r.schema.getColWithTableName();
			int size = r.cols.size();
			for(int i = 0; i < size; ++i){
				DataRecord tmp = r.getField(i);
				String only = onlyColName.get(i);
				String with = colWithTableName.get(i);
				put(only, tmp);
				put(with, tmp);
			}
		}
	}
	
	public void appendFromRecord(List<String> name, List<Integer> offset, List<DataRecord> cols){
		int size = name.size();
		for(int i = 0; i < size; ++i){
			int j = offset.get(i);
			if(j < 0){
				continue;
			}
			put(name.get(i), cols.get(j));
		}
	}
	
	public void appendAlias(String table, List<Expr> expr, List<String> alias){
		int size = alias.size();
		for(int i = 0; i < size; ++i){
			String alia = alias.get(i);
			String colName = expr.get(i).toString();
			put(alia, get(table, colName));
			put(table + "." + alia, get(table, colName));
		}
	}
	
	public void appendFrom(Env e){
		if(e != null){
			nameMap.putAll(e.nameMap);
		}
	}
}
