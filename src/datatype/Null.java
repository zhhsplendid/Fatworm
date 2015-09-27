package datatype;

import output.Debug;
import value.BinaryOp;
import filesys.RecordByte;

public class Null extends DataRecord{
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = -4362536976758632866L;
	
	static Null instance;
	
	public static Null getInstance(){
		if(instance == null){
			instance = new Null();
		}
		return instance;
	}
	
	public Null(){
		type = java.sql.Types.NULL;
	}

	@Override
	public void buffByte(RecordByte b, int a) {
		// TODO nothing
	}

	@Override
	public boolean cmp(BinaryOp op, DataRecord d) {
		Debug.warn("shouldn't cmp NULL in Null.java");
		return false;
	}

	@Override
	public String toString() {
		return "NULL";
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
}
