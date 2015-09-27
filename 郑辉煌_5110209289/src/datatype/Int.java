package datatype;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import output.Debug;

import filesys.RecordByte;

import util.Lib;
import value.BinaryOp;

public class Int extends DataRecord {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1129271511812967922L;
	public int value;
	
	public Int(int v) {
		type = java.sql.Types.INTEGER;
		value = v;
	}
	
	public Int(ByteBuffer b){
		type = java.sql.Types.INTEGER;
		value = b.getInt();
	}

	public Int(String x) {
		type = java.sql.Types.INTEGER;
		value = new BigDecimal(Lib.trim(x)).intValueExact();
	}

	public BigDecimal toBigDecimal() {
		return new BigDecimal(value).setScale(10);
	}

	@Override
	public void buffByte(RecordByte b, int A, int B) {
		b.putInt(value);
	}

	@Override
	public boolean cmp(BinaryOp op, DataRecord d) {
		BigDecimal v = toBigDecimal();
		switch(d.type){
		case java.sql.Types.INTEGER:
			return Lib.cmp(op, value, ((Int)d).value);
		case java.sql.Types.DECIMAL:
			return Lib.cmp(op, v, ((Decimal)d).value);
		
		case java.sql.Types.FLOAT:
			return Lib.cmp(op, java.lang.Float.valueOf(value), ((Float)d).value);
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
			return Lib.cmp(op, v, (new Int(d.toString())).toBigDecimal());
		
		default:
			Debug.err("Int cmp err");
		}
		return false;
	}

	@Override
	public String toString() {
		return Integer.toString(value);
	}

	@Override
	public int hashCode() {
		return value;
	}
	
}
