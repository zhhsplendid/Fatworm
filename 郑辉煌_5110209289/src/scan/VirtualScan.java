package scan;

import java.util.LinkedList;
import java.util.List;

import util.Env;
import fatworm.driver.Record;
import fatworm.driver.Schema;

/**
 * this class in order to solve select 1+2;
 * @author bd
 *
 */
public class VirtualScan extends Scan{
	
	int pointer;
	public Schema schema;
	public Record record;
	
	
	public VirtualScan(){
		super();
		hasComputed = true;
		schema = new Schema("VIRTUAL_TABLE");
	}
	
	@Override
	public void beforeFirst() {
		pointer = 0;
	}

	@Override
	public Record next() {
		if(pointer > 0) return null;
		++pointer;
		return new Record(schema);
	}

	@Override
	public boolean hasNext() {
		return pointer == 0;
	}

	@Override
	public String toString() {
		return "VIRTUAL";
	}

	@Override
	public void close() {
		pointer = 1;
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void eval(Env env) {
		hasComputed = true;
		beforeFirst();
	}

	@Override
	public void rename(String oldName, String newName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<String> getCol() {
		return new LinkedList<String>();
	}

	@Override
	public List<String> requestCol(){
		return new LinkedList<String>();
	}

}
