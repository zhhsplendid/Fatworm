package scan;

import java.util.LinkedList;
import java.util.List;

import util.Env;
import util.Lib;
import fatworm.driver.Record;
import fatworm.driver.Schema;

public class RenameTableScan extends Scan {
	
	public Scan fromScan;
	public Schema schema;
	public String alia;
	
	public RenameTableScan(Scan from, String s){
		alia = s;
		
		fromScan = from;
		fromScan.toScan = this;
		
		aggregation.addAll(fromScan.getAggregation());
		schema = new Schema(alia);
		
		Schema tmpSchema = fromScan.getSchema();
		for(String tmp: tmpSchema.columnName){
			String withTable = schema.tableName + "." + Lib.getAttributeName(tmp);
			
			schema.columnName.add(withTable);
			schema.colMap.put(withTable, fromScan.getSchema().getColumn(tmp));
		}
		schema.primaryKey = tmpSchema.primaryKey;
	}
	
	@Override
	public void beforeFirst() {
		fromScan.beforeFirst();
	}

	@Override
	public Record next() {
		Record ans = new Record(schema);
		Record record = fromScan.next();
		ans.cols.addAll(record.cols);
		return ans;
	}

	@Override
	public boolean hasNext() {
		return fromScan.hasNext();
	}

	@Override
	public String toString() {
		return "RenameTableScan (" + fromScan.toString() + ")";
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
		return new LinkedList<String> (schema.columnName);
	}

	@Override
	public List<String> requestCol() {
		List<String> list = fromScan.requestCol();
		Lib.removeRepeat(list, fromScan.getCol());
		return list;
	}

}
