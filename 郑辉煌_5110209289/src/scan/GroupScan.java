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


public class GroupScan extends Scan {

	public Scan fromScan;
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
	public GroupScan(Scan from, List<Expr> func, String by, Expr have, List<String> alia, boolean hasAlia){
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
		for(int i = 0; i < function.size(); ++i){
			aggregation.addAll(function.get(i).getAggr());
			List<String> list = function.get(i).requestCol();
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
		results = new ArrayList<Record>();
		pointer = 0;
		HashMap<DataRecord, Record> groupMap = new HashMap<DataRecord, Record>();
		HashMap<DataRecord, Env> aggrMap = new HashMap<DataRecord, Env>();
		HashMap<DataRecord, List<FunctionExpr.FunctionRecord>> aggrToFunc = new HashMap<DataRecord, List<FunctionExpr.FunctionRecord>>();
		Env env = envInput.clone();
		
		fromScan.eval(env);
		LinkedList<FunctionExpr> evalAggr = new LinkedList<FunctionExpr>();
		offsetList = new ArrayList<Integer>();
		for(FunctionExpr f : aggregation){
			if(f.canGetValue(schema)){
				env.remove(f.toString());
				evalAggr.add(f);
				offsetList.add(fromScan.getSchema().indexOfStrictString(f.col));
			}
		}
		
		LinkedList<Record> tmpList = new LinkedList<Record>();
		while(fromScan.hasNext()){
			Record record = fromScan.next();
			DataRecord by;
			if(groupBy == null){
				by = Null.getInstance();
			}
			else{
				by = record.getField(byIndex);
			}
			List<FunctionExpr.FunctionRecord> tmpFunc = aggrToFunc.get(by);
			if(tmpFunc == null){
				tmpFunc = new LinkedList<FunctionExpr.FunctionRecord>();
				int i = 0;
				for(FunctionExpr f : evalAggr){
					FunctionExpr.FunctionRecord fr = f.valueFunction(null, record.cols.get(offsetList.get(i++)));
					tmpFunc.add(fr);
				}
				aggrToFunc.put(by, tmpFunc);
			}
			else{
				int i = 0;
				for(FunctionExpr f : evalAggr){
					FunctionExpr.FunctionRecord fr = tmpFunc.get(i);
					f.valueFunction(fr, record.cols.get(offsetList.get(i++)));
				}
			}
			
			tmpList.addLast(record);
		}
		
		for(Map.Entry<DataRecord, List<FunctionExpr.FunctionRecord>> e : aggrToFunc.entrySet()){
			Env tmpEnv = new Env();
			int i = 0;
			for(FunctionExpr a : evalAggr){
				FunctionExpr.FunctionRecord fr = e.getValue().get(i++);
				tmpEnv.put(a.toString(), fr);
			}
			aggrMap.put(e.getKey(), tmpEnv);
		}

		nameList = new ArrayList<String>();
		offsetList = new ArrayList<Integer>();
		for(String s: requestAttribute){
			nameList.add(s);
			offsetList.add(fromScan.getSchema().indexOfStrictString(s));
		}
		while(!tmpList.isEmpty()){
			Record record = tmpList.pollFirst();
			DataRecord by;
			if(groupBy == null){
				by = Null.getInstance();
			}
			else{
				by = record.getField(byIndex);
			}
			Record pr = groupMap.get(by);

			if(pr == null){
				Env tmpEnv = aggrMap.get(by);
				tmpEnv.appendFromRecord(nameList, offsetList, record.cols);
				pr = new Record(schema);
				groupMap.put(by, pr);
				pr.addFieldFromExpr(tmpEnv, function);
				for(int i = 0; i < orderCols.size(); ++i){
					pr.addField(tmpEnv.get(orderCols.get(i)));
				}
			}
		}
		if(! hasAlias){
			for(DataRecord dr : groupMap.keySet()){
				Record record = groupMap.get(dr);
				Env tmpEnv = aggrMap.get(dr);
				if(having == null || having.valuePredicate(tmpEnv)){
					results.add(record);
				}
			}
		}else{
			Env tmpEnv = null;
			for(DataRecord dr : groupMap.keySet()){
				Record record = groupMap.get(dr);
				tmpEnv = aggrMap.get(dr);
				tmpEnv.appendAlias(schema.tableName, function, alias);
				if(having == null || having.valuePredicate(tmpEnv)){
					results.add(record);
				}
			}
			env.appendFrom(tmpEnv);
		}
		if(results.size() == 0 && (having == null || having.valuePredicate(env))){
			Record pr = new Record(schema);
			pr.addFieldFromExpr(new Env(), function);
			for(int i = 0; i < orderCols.size(); ++i){
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
		Lib.removeRepeat(list, fromScan.getCol());
		if(having != null){
			Lib.addAllCol(list, having.requestCol());
		}
		return list;
	}

}
