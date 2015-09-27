package scan;

import java.util.LinkedList;
import java.util.List;

import util.Env;
import fatworm.driver.Record;
import fatworm.driver.Schema;

public class EmptyScan extends ZeroFromScan{
	public long time;
	public static EmptyScan instance;
	public Schema schema;
	
	public static EmptyScan getInstance(){
		if(instance == null){
			instance = new EmptyScan();
		}
		return instance;
	}
	
	private EmptyScan(){
		super();
		hasComputed = true;
		schema = new Schema("EmptyScan");
		
	}

	@Override
	public void beforeFirst() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Record next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void eval(Env env) {
		// TODO Auto-generated method stub
		
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
	public List<String> requestCol() {
		return new LinkedList<String>();
	}
}
