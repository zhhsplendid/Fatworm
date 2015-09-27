package scan;

import output.Debug;
import datatype.DataRecord;

import util.Env;
import util.Lib;
import value.*;

public class Condition {
	public long time;
	public BinaryOp op;
	public String name;
	public DataRecord value;
	
	public Condition(BinaryExpr e){
		op = e.op;
		if(e.left instanceof IdExpr){
			String s = ((IdExpr)e.left).name;
			name = Lib.getAttributeName(s);
			value = e.right.valueExpr(new Env());
		}
		else{
			String s = ((IdExpr)e.right).name;
			name = Lib.getAttributeName(s);
			value = e.left.valueExpr(new Env());
		}
	}
	
	public Range getRange(){
		switch(op){
		case EQUAL:
			return new Range(value);
		case GREATER:
		case GREATER_EQ:
			return new Range(value, null);
		case LESS:
		case LESS_EQ:
			return new Range(null, value);
		
		default:
			break;
		}
		Debug.err("Missing range Condition.java:" + op);
		return null;
	}
}
