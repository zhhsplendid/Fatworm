package datatype;


import filesys.RecordByte;
import util.Lib;
import value.BinaryOp;

public class Timestamp extends DataRecord {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8298818719776679494L;
	public java.sql.Timestamp value;
	

	public Timestamp(String x) {
		type = java.sql.Types.TIMESTAMP;
		value = new java.sql.Timestamp(Long.valueOf((Lib.trim(x))));
	}
	
	public Timestamp(java.sql.Timestamp timestamp) {
		type = java.sql.Types.TIMESTAMP;
		value = timestamp;
	}
	
	public Timestamp(Long x) {
		type = java.sql.Types.TIMESTAMP;
		value = new java.sql.Timestamp(x);
	}

	@Override
	public void buffByte(RecordByte b, int A, int B) {
		b.putLong(value.getTime());
	}

	@Override
	public boolean cmp(BinaryOp op, DataRecord d) {
		switch(d.type){
		case java.sql.Types.TIMESTAMP:
			return Lib.cmp(op, value, ((Timestamp)d).value);
		case java.sql.Types.DATE:
			return Lib.cmp(op, value, ((Date)d).value);
		case java.sql.Types.CHAR:
			return Lib.cmp(op, value, java.sql.Timestamp.valueOf(((Char)d).value));
		case java.sql.Types.VARCHAR:
			return Lib.cmp(op, value, java.sql.Timestamp.valueOf(((Varchar)d).value));
		}
		return false;
	}

	@Override
	public String toString() {
		return value.toString();
	}

	@Override
	public int hashCode() {
		return value.hashCode();
	}
}
