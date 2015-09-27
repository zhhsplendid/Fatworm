package datatype;

import filesys.RecordByte;
import util.Lib;
import value.BinaryOp;

public class Varchar extends DataRecord {
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = -4249406856367387987L;
	public String value;
	
	public Varchar(String s){
		type = java.sql.Types.VARCHAR;
		value = Lib.trim(s);
	}

	@Override
	public void buffByte(RecordByte b, int a) {
		if(a < 0){
			a = 0;
		}
		String s = value.substring(0, Math.min(value.length(), a));
		b.putString(s);
	}

	@Override
	public boolean cmp(BinaryOp op, DataRecord d) {
		return Lib.cmpString(op, toString(), d == null ? "" :d.toString());
	}

	@Override
	public String toString() {
		return "'" + value + "'";
	}

	@Override
	public int hashCode() {
		return value.hashCode();
	}
}