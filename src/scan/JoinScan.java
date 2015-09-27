package scan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import output.Debug;
import util.Env;
import fatworm.driver.Record;
import fatworm.driver.Schema;

public class JoinScan extends TwoFromScan {
	public long time;
	//public Scan left, right;
	public Record curLeft;
	public Schema schema;
	public boolean hasInit;
	
	public JoinScan(Scan l, Scan r){
		left = l;
		right = r;
		hasInit = false;
		left.toScan = this;
		right.toScan = this;
		curLeft = null;
		aggregation.addAll(left.getAggregation());
		aggregation.addAll(right.getAggregation());
		
		schema = new Schema("schema" + Env.getNewCount());
		
		HashSet<String> commonName = new HashSet<String> (left.getSchema().columnName);
		commonName.retainAll(right.getSchema().columnName);
		String ltable = left.getSchema().tableName;
		ArrayList<String> lcolName = left.getSchema().columnName;
		int tmpSize = lcolName.size();
		for(int i = 0; i < tmpSize; ++i){
			String colName = lcolName.get(i);
			if(commonName.contains(colName)){
				schema.columnName.add(ltable + "." + colName);
				schema.colMap.put(ltable + "." + colName, left.getSchema().colMap.get(colName));
			}
			else{
				schema.columnName.add(colName);
				schema.colMap.put(colName, left.getSchema().colMap.get(colName));
			}
		}
		String rtable = right.getSchema().tableName;
		ArrayList<String> rcolName = right.getSchema().columnName;
		tmpSize = rcolName.size();
		for(int i = 0; i < tmpSize; ++i){
			String colName = rcolName.get(i);
			if(commonName.contains(colName)){
				schema.columnName.add(rtable + "." + colName);
				schema.colMap.put(rtable + "." + colName, right.getSchema().colMap.get(colName));
			}
			else{
				schema.columnName.add(colName);
				schema.colMap.put(colName, right.getSchema().colMap.get(colName));
			}
		}
		//this.schema.isJoin = true;
	}
	
	private Record joinTwoRecords(Record l, Record r){
		Record ans = new Record(schema);
		ans.addField(l.cols);
		ans.addField(r.cols);
		return ans;
	}
	
	@Override
	public void beforeFirst() {
		left.beforeFirst();
		right.beforeFirst();
		curLeft = null;
	}

	@Override
	public Record next() {
		assert hasComputed;
		
		boolean needLeftChange = curLeft == null;
		if(needLeftChange){
			curLeft = left.next();
		}
		
		Record curRight = right.next();
		
		Record ans = joinTwoRecords(curLeft, curRight);
		
		needLeftChange = !right.hasNext() && left.hasNext();
		if(needLeftChange){
			right.beforeFirst();
			curLeft = left.next();
		}
		return ans;
	}

	@Override
	public boolean hasNext() {
		assert hasComputed;
		return (curLeft != null || left.hasNext()) && right.hasNext();
	}

	@Override
	public String toString() {
		return "join (" + left.toString() + ", " + right.toString() + ")";
	}

	@Override
	public void close() {
		left.close();
		right.close();
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void eval(Env env) {
		hasComputed = true;
		curLeft = null;
		Env local = env.clone();
		left.eval(local);
		right.eval(local);
	}

	@Override
	public void rename(String oldName, String newName) {
		left.rename(oldName, newName);
		right.rename(oldName, newName);
	}

	@Override
	public List<String> getCol() {
		List<String> list = new LinkedList<String>(left.getCol());
		list.addAll(right.getCol());
		return list;
	}

	@Override
	public List<String> requestCol() {
		List<String> list = new LinkedList<String>(left.requestCol());
		list.addAll(right.requestCol());
		return list;
	}

}