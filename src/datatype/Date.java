package datatype;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import output.Debug;
import util.Lib;
import value.BinaryOp;

import filesys.RecordByte;


public class Date extends DataRecord {
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = 3360992579306815807L;
	public java.sql.Timestamp value;
	

	public Date(String s) {
		type = java.sql.Types.DATE;
		s = Lib.trim(s);
		try{
			value = new java.sql.Timestamp(Long.valueOf(s));
		} catch (NumberFormatException e){
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			try {
				long ans = format.parse(s).getTime();
				value = new java.sql.Timestamp(ans);
			} catch (ParseException ee) {
				ee.printStackTrace();
			}
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	public Date(Long x) {
		type = java.sql.Types.DATE;
		value = new java.sql.Timestamp(x);
	}
	public Date(java.sql.Timestamp v){
		type = java.sql.Types.DATE;
		value = v;
	}
	
	
	@Override
	public void buffByte(RecordByte b, int a) {
		b.putLong(value.getTime());
	}
	@Override
	public String toString() {
		return "'" + value.toString() + "'";
	}
	@Override
	public boolean cmp(BinaryOp op, DataRecord d) {
		switch(d.type){
		case java.sql.Types.DATE:
			return Lib.cmp(op, value, ((Date)d).value);
		case java.sql.Types.TIMESTAMP:
			return Lib.cmp(op, value, ((Timestamp)d).value);
		case java.sql.Types.CHAR:
			return Lib.cmp(op, value, java.sql.Timestamp.valueOf(((Char)d).value));
		case java.sql.Types.VARCHAR:
			return Lib.cmp(op, value, java.sql.Timestamp.valueOf(((Varchar)d).value));
		default:
			Debug.warn("Data cmp err");
		}
		return false;
	}
	@Override
	public int hashCode() {
		return value.hashCode();
	}
	
	
}
