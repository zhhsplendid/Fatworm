package datatype;

import java.math.BigDecimal;

import output.Debug;

import filesys.RecordByte;

import util.Lib;
import value.BinaryOp;



public class Bool extends DataRecord{
	
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = -1633253543193410294L;
	public boolean value;
	
	public Bool(boolean b){
		type = java.sql.Types.BOOLEAN;
		value = b;
	}
	public Bool(String s) {
		type = java.sql.Types.BOOLEAN;
		value = Lib.toBoolean(s);
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
		boolean flag = false;
		switch(d.type){
		case java.sql.Types.BOOLEAN:
		case java.sql.Types.INTEGER:
		case java.sql.Types.FLOAT:
		case java.sql.Types.DECIMAL:
			flag = true;
		}
		if(! flag){
			Debug.warn("cmp not correct type in Bool.java");
		}
		return Lib.cmp(op, toBigDecimal(), d.toBigDecimal());
	}
	
	
	public BigDecimal toBigDecimal(){
		if(value){
			return new BigDecimal(1).setScale(10);
		}
		else{
			return new BigDecimal(0).setScale(10);
		}
	}
	
	@Override
	public void buffByte(RecordByte b, int a){
		b.putBool(value);
	}
	@Override
	public int hashCode() {
		return value? 0:1;
	}
	  
}
