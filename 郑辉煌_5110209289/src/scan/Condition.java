package scan;

import output.Debug;
import datatype.DataRecord;

import util.Env;
import util.Lib;
import value.*;

public class Condition {
	public BinaryOp op;
	public String name;
	public DataRecord value;
	
	public Condition(BinaryExpr e){
		op = e.op;
		if(e.left instanceof IdExpr){
			name = Lib.getAttributeName(((IdExpr)e.left).name);
			value = e.right.valueExpr(new Env());
		}
		else{
			name = Lib.getAttributeName(((IdExpr)e.right).name);
			value = e.left.valueExpr(new Env());
		}
	}
	
	public Range getRange(){
		switch(op){
		case EQUAL:
			return new Range(value, value);
		case GREATER:
		case GREATER_EQ:
			return new Range(value, null);
		case LESS:
		case LESS_EQ:
			return new Range(null, value);
		
		default:
			break;
		}
		Debug.err("Missing range for operator:" + op);
		return null;
	}
}
