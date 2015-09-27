package scan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import output.Debug;

import util.Env;
import util.Lib;
import fatworm.driver.Record;
import fatworm.driver.Schema;

public class DistinctScan extends Scan {
	
	public Scan fromScan;
	ArrayList<Record> results;
	int pointer;
	
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
		results = new ArrayList<Record>();
		pointer = 0;
		Env tmp = env.clone();
		fromScan.eval(tmp);
		HashSet<Record> set = new HashSet<Record>();
		while(fromScan.hasNext()){
			Record record = fromScan.next();
			if(! set.contains(record)){
				results.add(record);
				set.add(record);
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
