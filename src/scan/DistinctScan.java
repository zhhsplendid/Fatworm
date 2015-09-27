package scan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import output.Debug;

import util.Env;
import util.Lib;
import fatworm.driver.Record;
import fatworm.driver.Schema;

public class DistinctScan extends OneFromScan {
	public long time;
	int pointer;
	//public Scan fromScan;
	ArrayList<Record> results;
	
	
	public DistinctScan(Scan from){
		super();
		fromScan = from;
		fromScan.toScan = this;
		
		aggregation.addAll(fromScan.getAggregation());
	}
	
	@Override
	public void beforeFirst() {
		pointer = 0;
	}

	@Override
	public Record next() {
		if(! hasComputed){
			Debug.err("DistinctScan never computed");
		}
		return results.get(pointer++);
	}

	@Override
	public boolean hasNext() {
		if(! hasComputed){
			Debug.err("DistinctScan never computed");
		}
		return pointer < results.size();
	}

	@Override
	public String toString() {
		return "DistinctScan (" + fromScan.toString() + ")";
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
		Record record;
		results = new ArrayList<Record>();
		HashSet<Record> set = new HashSet<Record>();
		Env tmp = env.clone();
		fromScan.eval(tmp);
		
		while(fromScan.hasNext()){
			record = fromScan.next();
			if(! set.contains(record)){
				set.add(record);
				results.add(record);
			}
		}
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
		Lib.removeRepeat(list, fromScan.requestCol());
		return list;
	}

}
