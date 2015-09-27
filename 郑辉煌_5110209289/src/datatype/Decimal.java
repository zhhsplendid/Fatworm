package datatype;

import java.math.BigDecimal;

import output.Debug;

import filesys.RecordByte;

import util.Lib;
import value.BinaryOp;


public class Decimal extends DataRecord {

	/**
	 * 
	 */
	private static final long serialVersionUID = 426591677958260047L;
	
	public BigDecimal value;
	public Decimal(String s) {
		type = java.sql.Types.DECIMAL;
		value = new BigDecimal(Lib.trim(s)).setScale(10, BigDecimal.ROUND_HALF_EVEN);
	}
	
	public Decimal(BigDecimal v){
		type = java.sql.Types.DECIMAL;
		value = v.setScale(10, BigDecimal.ROUND_HALF_EVEN);
	}
	
	public BigDecimal toBigDecimal(){
		return value;
	}

	@Override
	public void buffByte(RecordByte b, int A, int B) {
		int scale = value.scale();
		b.putInt(scale);
		b.putBytes(value.unscaledValue().toByteArray());
	}

	@Override
	public boolean cmp(BinaryOp op, DataRecord d) {
		switch(d.type){
		case java.sql.Types.DECIMAL:
			return Lib.cmp(op, value, ((Decimal)d).value);
		case java.sql.Types.INTEGER:
			return Lib.cmp(op, value, ((Int)d).toBigDecimal());
		case java.sql.Types.FLOAT:
			return Lib.cmp(op, value, ((Float)d).toBigDecimal());
		}
		Debug.err("Decimal cmp err");
		return false;
	}

	@Override
	public String toString() {
		return "'" + value.toString() + "'";
	}

	@Override
	public int hashCode() {
		return value.hashCode();
	}
}
