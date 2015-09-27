package value;

import output.Debug;

public enum BinaryOp {
	AND, OR, EQUAL, NOT_EQUAL, GREATER, GREATER_EQ,
	LESS, LESS_EQ, PLUS, MINUS, MULT, DIV, MOD;
	
	public String toString(){
		switch(this){
		case AND: return "and";
		case OR: return "or";
		case EQUAL: return "=";
		case NOT_EQUAL: return "<>";
		case GREATER: return ">";
		case GREATER_EQ: return ">=";
		case LESS: return "<";
		case LESS_EQ: return "<=";
		case PLUS: return "+";
		case MINUS: return "-";
		case MULT: return "*";
		case DIV: return "/";
		case MOD: return "%";
		default: 
			Debug.err("wrong binary op");
			return null;
		}
	}
}
