package datatype;

import java.math.BigDecimal;

import output.Debug;


import filesys.RecordByte;

import util.Lib;
import value.BinaryOp;

public class Float extends DataRecord {
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = 2107312234796203038L;
	public float value;
	
	
	public Float(String x) {
		type = java.sql.Types.FLOAT;
		value = java.lang.Float.valueOf(Lib.trim(x));
	}
	
	public Float(float x){
		type = java.sql.Types.FLOAT;
		value = x;
	}

	public Float(double x) {
		type = java.sql.Types.FLOAT;
		value = (float) x;
	}

	public BigDecimal toBigDecimal() {
		return new BigDecimal(value).setScale(10, BigDecimal.ROUND_HALF_EVEN );
	}

	@Override
	public void buffByte(RecordByte b, int a) {
		b.putFloat(value);
	}

	@Override
	public boolean cmp(BinaryOp op, DataRecord d) {
		BigDecimal v = toBigDecimal();
		switch(d.type){
		case java.sql.Types.DECIMAL:
			return Lib.cmp(op, v, ((Decimal)d).value);
		case java.sql.Types.INTEGER:
			return Lib.cmp(op, v, ((Int)d).toBigDecimal());
		case java.sql.Types.FLOAT:
			return Lib.cmp(op, v, ((Float)d).toBigDecimal());
		default:
			Debug.warn("Float cmp err");
		}
		return false;
	}

	@Override
	public String toString() {
		return "" + value;
	}

	@Override
	public int hashCode() {
		return new java.lang.Float(value).hashCode();
	}
}
