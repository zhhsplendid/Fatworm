package scan;

import java.util.List;

import util.Env;
import fatworm.driver.Record;
import fatworm.driver.Schema;

public class EmptyScan extends Scan{
	
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
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> requestCol() {
		// TODO Auto-generated method stub
		return null;
	}
}
