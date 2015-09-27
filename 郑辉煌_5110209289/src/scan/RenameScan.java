package scan;

import java.util.LinkedList;
import java.util.List;

import util.Env;
import util.Lib;
import fatworm.driver.Record;
import fatworm.driver.Schema;

public class RenameScan extends Scan {
	public Scan fromScan;
	public Schema schema;
	public List<String> asNewName;
	
	public RenameScan(Scan from, List<String> l){
		super();
		fromScan = from;
		fromScan.toScan = this;
		asNewName = l;
		
		aggregation.addAll(fromScan.getAggregation());
		Schema tmpSchema = fromScan.getSchema();//
		schema = new Schema(tmpSchema.tableName);
		for(int i = 0; i < tmpSchema.columnName.size(); ++i){
			String now = schema.tableName + "." + Lib.getAttributeName(asNewName.get(i));
			String old = tmpSchema.columnName.get(i);
			schema.columnName.add(now);
			schema.colMap.put(now, tmpSchema.getColumn(old));
		}
		schema.primaryKey = tmpSchema.primaryKey;
	}
	
	@Override
	public void beforeFirst() {
		fromScan.beforeFirst();
	}

	@Override
	public Record next() {
		Record record = fromScan.next();
		Record ans = new Record(schema);
		ans.cols.addAll(record.cols);
		return ans;
	}

	@Override
	public boolean hasNext() {
		return fromScan.hasNext();
	}

	@Override
	public String toString() {
		return "rename (" + fromScan.toString() + ")";
	}

	@Override
	public void close() {
		fromScan.close();
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void eval(Env env) {
		hasComputed = true;
		fromScan.eval(env);
	}

	@Override
	public void rename(String oldName, String newName) {
		fromScan.rename(oldName, newName);
	}

	@Override
	public List<String> getCol() {
		return new LinkedList<String>(asNewName);
	}

	@Override
	public List<String> requestCol() {
		List<String> list = fromScan.requestCol();
		Lib.removeRepeat(list, fromScan.getCol());
		return list;
	}

}
