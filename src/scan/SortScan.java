package scan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import output.Debug;

import parser.FatwormParser;

import util.Env;
import util.Lib;
import value.BinaryOp;
import value.Expr;
import datatype.DataRecord;
import fatworm.driver.Record;
import fatworm.driver.Schema;

public class SortScan extends OneFromScan {
	public long time;
	//public Scan fromScan;
	public Schema schema;
	
	public List<Record> results;
	public int pointer;
	
	public List<Expr> exprs;
	public List<String> requestAttribute;
	
	public List<String> orderName;
	public List<Integer> orderType;
	public List<Integer> orderIndex;
	
	boolean hasSort = false;
	public SortScan(Scan from, List<String> col, List<Expr> func, List<Integer> type) {
		super();
		fromScan = from;
		fromScan.toScan = this;
		orderName = col;
		orderType = type;
		exprs = func;
		aggregation.addAll(fromScan.getAggregation());
		requestAttribute = new ArrayList<String>();
		
		schema = new Schema();
		schema.fromList(func, fromScan.getSchema());
		Schema src = fromScan.getSchema();
		HashSet<String> neededAttr = new HashSet<String>();
		for(String s: orderName){
			String need = Lib.getAttributeName(s).toLowerCase();
			neededAttr.add(need);
		}
		ArrayList<String> columnName = src.columnName;
		String colName;
		int csize = columnName.size();
		for(int i = 0; i < csize; ++i ){
			colName = columnName.get(i);
			String old = colName;
			if(schema.colMap.containsKey(old)){
				continue;
			}
			String need = Lib.getAttributeName(old).toLowerCase();
			if(!neededAttr.contains(need)){
				continue;
			}
			schema.colMap.put(old, src.getColumn(old));
			schema.columnName.add(old);
			requestAttribute.add(old);
		}
		orderIndex = new ArrayList<Integer>();
		for(String name: orderName){
			orderIndex.add(schema.indexOf(name));
		}
	}
	
	@Override
	public void beforeFirst() {
		pointer = 0;
	}

	@Override
	public Record next() {
		if(! hasComputed){
			Debug.err("OrderScan never computed");
		}
		return results.get(pointer++);
	}

	@Override
	public boolean hasNext() {
		if(! hasComputed){
			Debug.err("OrderScan never computed");
		}
		return pointer < results.size();
	}

	@Override
	public String toString() {
		return "OrderScan(" + fromScan.toString() + ")";
	}

	@Override
	public void close() {
		fromScan.close();
		results = new ArrayList<Record>();
	}

	@Override
	public Schema getSchema() {
		return fromScan.getSchema();
	}

	@Override
	public void eval(Env env) {
		hasComputed = true;
		pointer = 0;
		Env local = env.clone();
		results = new ArrayList<Record>();
		
		fromScan.eval(local);
		while(fromScan.hasNext()){
			Record record = fromScan.next();
			local.appendFromRecord(record);
			Record pr = new Record(schema);
			pr.addFieldFromExpr(exprs, local);
			for(String s: requestAttribute){
				pr.addField(local.get(s));
			}
			results.add(pr);
		}
		sort();
		
	}

	private void sort() {
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");   
		Collections.sort(results, new Comparator<Record>(){
			public int compare(Record r1, Record r2){
				int osize = orderIndex.size();
				for(int i = 0; i < osize; ++i){
					DataRecord l = r1.getField(orderIndex.get(i));
					DataRecord r = r2.getField(orderIndex.get(i));
					if(orderType.get(i) == FatwormParser.ASC){
						if(l.cmp(BinaryOp.LESS, r)){
							return -1;
						}
						if(l.cmp(BinaryOp.GREATER, r)){
							return 1;
						}
					} else {
						if(l.cmp(BinaryOp.LESS, r)){
							return 1;
						}
						if(l.cmp(BinaryOp.GREATER, r)){
							return -1;
						}
					}
				}
				return 0;
			}
		});
	}

	@Override
	public void rename(String oldName, String newName) {
		fromScan.rename(oldName, newName);
	}

	@Override
	public List<String> getCol() {
		return fromScan.getCol();
	}

	@Override
	public List<String> requestCol() {
		List<String> list = fromScan.requestCol();
		Lib.removeRepeat(list, fromScan.getCol());
		return list;
	}

}