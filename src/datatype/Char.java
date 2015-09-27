package datatype;

import filesys.RecordByte;
import util.Lib;
import value.BinaryOp;

public class Char extends DataRecord {
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = -926797748959051944L;
	public String value;
	
	public Char(String s){
		value = Lib.trim(s);
		type = java.sql.Types.CHAR;
	}
	
	public boolean cmp(BinaryOp op, DataRecord r){
		return Lib.cmpString(op, toString(), r.toString());
	}

	
	public String toString(){
		return "'" + value + "'";
	}
	
	@Override
	public void buffByte(RecordByte b, int a) {
		String s = value.substring(0, Math.max(0, Math.min(value.length(), a)));
		b.putString(s);
	}

	@Override
	public int hashCode() {
		return value.hashCode();
	}
}
