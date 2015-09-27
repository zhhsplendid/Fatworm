package scan;

import java.util.*;

import output.Debug;

import util.Env;
import util.Lib;
import value.Expr;
import value.FunctionExpr;
import datatype.DataRecord;
import datatype.Null;
import fatworm.driver.Record;
import fatworm.driver.Schema;


public class GroupScan extends OneFromScan {
	public long time;
	//public Scan fromScan;
	public String groupBy;
	public Expr having;
	public Schema schema;
	public List<Expr> function;
	public List<Record> results;
	public List<String> orderCols;
	public List<String> alias;
	
	private HashSet<String> requestAttribute;
	private List<String> nameList;
	private List<Integer> offsetList;
	
	public int byIndex;
	public int pointer;
	public boolean hasAlias;
	public GroupScan(Scan from, String by, Expr have, boolean hasAlia, List<Expr> func, List<String> alia){
		super();
		fromScan = from;
		fromScan.toScan = this;
		having = have;
		groupBy = by;
		function = func;
		
		hasAlias = hasAlia;
		pointer = 0;
		alias = alia;
		
		requestAttribute = new HashSet<String>();
		int funcSize = function.size();
		for(int i = 0; i < funcSize; ++i){
			Expr f = function.get(i);
			Lib.addAll(aggregation, f.getAggr());
			List<String> list = f.requestCol();
			for(String s: list){
				requestAttribute.add(s.toLowerCase());
			}
		}
		
		if(having != null){
			aggregation.addAll(having.getAggr());
			List<String> list = having.requestCol();
			for(String s: list){
				requestAttribute.add(s.toLowerCase());
			}
		}
		
		schema = new Schema();
		schema.fromList(function, fromScan.getSchema());
		//System.out.println(fromScan.getSchema());
		Schema src = fromScan.getSchema();
		orderCols = new ArrayList<String>();
		for(String s: src.columnName){
			String old = s;
			s = Lib.getAttributeName(old);
			if(schema.colMap.containsKey(s) || schema.colMap.containsKey(old)){
				continue;
			}
			if(! requestAttribute.contains(old.toLowerCase()) && !requestAttribute.contains(s.toLowerCase())){
				continue;
			}
			
			schema.colMap.put(s, src.getColumn(old));
			schema.columnName.add(s);
		}
		
		if(by != null){
			byIndex = src.indexOf(by);
		}
		
	}
	
	@Override
	public void beforeFirst() {
		pointer = 0;
	}

	@Override
	public Record next() {
		if(! hasComputed){
			Debug.err("Group Scan never computes");
		}
		return results.get(pointer++);
	}

	@Override
	public boolean hasNext() {
		if(! hasComputed){
			Debug.err("Group Scan never computes");
		}
		return pointer < results.size();
	}

	@Override
	public String toString() {
		return "Group (" + fromScan.toString() + ")";
	}

	@Override
	public void close() {
		fromScan.close();
		results = new ArrayList<Record>();
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void eval(Env envInput) {
		hasComputed = true;
		pointer = 0;
		results = new ArrayList<Record>();
		Env env = envInput.clone();
		fromScan.eval(env);
		
		HashMap<DataRecord, Env> aggrMap = new HashMap<DataRecord, Env>();
		HashMap<DataRecord, Record> groupMap = new HashMap<DataRecord, Record>();
		HashMap<DataRecord, List<FunctionExpr.FunctionRecord>> aggrToFunc = new HashMap<DataRecord, List<FunctionExpr.FunctionRecord>>();
		LinkedList<FunctionExpr> evalAggr = new LinkedList<FunctionExpr>();
		LinkedList<Record> tmpList = new LinkedList<Record>();
		offsetList = new ArrayList<Integer>();
		Iterator<FunctionExpr> iter = aggregation.iterator();
		while(iter.hasNext()){
			FunctionExpr f = iter.next();
			if(f.canGetValue(schema)){
				env.remove(f.toString());
				evalAggr.add(f);
				Schema tmps = fromScan.getSchema();
				offsetList.add(tmps.indexOfStrictString(f.col));
			}
		}
		
		while(fromScan.hasNext()){
			DataRecord by;
			Record record = fromScan.next();
			
			if(groupBy == null){
				by = Null.getInstance();
			}
			else{
				by = record.getField(byIndex);
			}
			
			List<FunctionExpr.FunctionRecord> tmpFunc = aggrToFunc.get(by);
			if(tmpFunc != null){
				int i = 0;
				for(FunctionExpr f : evalAggr){
					f.valueFunction(tmpFunc.get(i), record.getField(offsetList.get(i++)));
				}
				
			}
			else{
				int i = 0;
				tmpFunc = new LinkedList<FunctionExpr.FunctionRecord>();
				
				for(FunctionExpr f : evalAggr){
					tmpFunc.add(f.valueFunction(null, record.getField(offsetList.get(i++))));
				}
				aggrToFunc.put(by, tmpFunc);
			}
			
			tmpList.addLast(record);
		}
		
		for(Map.Entry<DataRecord, List<FunctionExpr.FunctionRecord>> e : aggrToFunc.entrySet()){
			int i = 0;
			Env tmpEnv = new Env();
			for(FunctionExpr a : evalAggr){
				String s = a.toString();
				tmpEnv.put(s, e.getValue().get(i++));
			}
			aggrMap.put(e.getKey(), tmpEnv);
		}

		nameList = new ArrayList<String>();
		offsetList = new ArrayList<Integer>();
		
		Iterator<String> siter = requestAttribute.iterator();
		while(siter.hasNext()){
			String s = siter.next();
			nameList.add(s);
			Schema tmps = fromScan.getSchema();
			offsetList.add(tmps.indexOfStrictString(s));
		}
		while(!tmpList.isEmpty()){
			Record record = tmpList.pollFirst();
			DataRecord by;
			if(groupBy != null){
				by = record.getField(byIndex);
			}
			else{
				by = Null.getInstance();
			}
			Record pr = groupMap.get(by);

			if(pr == null){
				Env tmpEnv = aggrMap.get(by);
				tmpEnv.appendFromRecord(nameList, offsetList, record.cols);
				pr = new Record(schema);
				pr.addFieldFromExpr(function, tmpEnv);//
				groupMap.put(by, pr);
				int oSize = orderCols.size();
				for(int i = 0; i < oSize; ++i){
					pr.addField(tmpEnv.get(orderCols.get(i)));
				}
			}
		}
		if(! hasAlias){
			for(DataRecord dr : groupMap.keySet()){
				Env tmpEnv = aggrMap.get(dr);
				boolean bool = having == null || having.valuePredicate(tmpEnv);
				if(bool){
					results.add(groupMap.get(dr));
				}
			}
			
		}
		else{
			Env tmpEnv = null;
			for(DataRecord dr : groupMap.keySet()){
				tmpEnv = aggrMap.get(dr);
				tmpEnv.appendAlias(schema.tableName, function, alias);
				//System.out.println("debug" + tmpEnv.get("totalnr"));
				boolean bool = having == null || having.valuePredicate(tmpEnv);
				if(bool){
					results.add(groupMap.get(dr));//
				}
			}
			env.appendFrom(tmpEnv);
		}
		if(results.isEmpty() && (having == null || having.valuePredicate(env))){
			Record pr = new Record(schema);
			pr.addFieldFromExpr(function, new Env());
			int oSize = orderCols.size();
			for(int i = 0; i < oSize; ++i){
				pr.addField(env.get(orderCols.get(i)));
			}
			results.add(pr);
		}
	}

	@Override
	public void rename(String oldName, String newName) {
		fromScan.rename(oldName, newName);
	}

	@Override
	public List<String> getCol() {
		return new LinkedList<String>(schema.columnName);
	}

	@Override
	public List<String> requestCol() {
		List<String> list = fromScan.requestCol();
		List<String> al = fromScan.getCol();
		Lib.removeRepeat(list, al);
		if(having != null){
			List<String> bl = having.requestCol();
			Lib.addAllCol(list, bl);
		}
		return list;
	}

}
