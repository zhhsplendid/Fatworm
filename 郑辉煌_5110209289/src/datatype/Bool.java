package datatype;

import java.math.BigDecimal;

import output.Debug;

import filesys.RecordByte;

import util.Lib;
import value.BinaryOp;



public class Bool extends DataRecord{

	/**
	 * 
	 */
	private static final long serialVersionUID = -1633253543193410294L;
	public boolean value;
	
	public Bool(boolean b){
		value = b;
		type = java.sql.Types.BOOLEAN;
	}
	public Bool(String s) {
		value = Lib.toBoolean(s);
		type = java.sql.Types.BOOLEAN;
	}
	
	public String toString(){
		if(value){
			return "true";
		}
		else{
			return "false";
		}
	}
	
	public boolean cmp(BinaryOp op, DataRecord d){
		switch(d.type){
		case java.sql.Types.BOOLEAN:
		case java.sql.Types.INTEGER:
		case java.sql.Types.FLOAT:
		case java.sql.Types.DECIMAL:
			return Lib.cmp(op, toBigDecimal(), toBigDecimal());
		}
		Debug.err("cmp not correct type");
		return false;
	}
	
	
	public BigDecimal toBigDecimal(){
		return new BigDecimal(value ? 1:0).setScale(10);
	}
	
	@Override
	public void buffByte(RecordByte b, int A, int B){
		b.putBool(value);
	}
	@Override
	public int hashCode() {
		return value? 0:1;
	}
	  
}
