package scan;

import java.util.*;

import fatworm.driver.Record;
import fatworm.driver.Schema;

import util.Env;
import value.*;

public abstract class Scan{
	public long time;
	public Scan toScan; // some scan have fromScan
	public Set<FunctionExpr> aggregation;
	public boolean hasComputed;
	
	public Scan(){
		aggregation = new HashSet<FunctionExpr>();
		hasComputed = false;
	}
	
	public Scan(Scan scan){
		toScan = scan;
		aggregation = new HashSet<FunctionExpr>();
	}
	
	public Set<FunctionExpr> getAggregation(){
		return aggregation;
	}
	
	
	public abstract void setFrom(Scan oldFrom, Scan newFrom);
	
	public abstract void beforeFirst();

	public abstract Record next();

	public abstract boolean hasNext();
	
	public abstract String toString();

	public abstract void close();

	public abstract Schema getSchema();

	public abstract void eval(Env env);

	public abstract void rename(String oldName, String newName);
	
	public abstract List<String> getCol();
	
	public abstract List<String> requestCol();
	
	
}
