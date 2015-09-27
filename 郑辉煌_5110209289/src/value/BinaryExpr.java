package value;

import java.math.BigDecimal;
import java.util.List;
import static java.sql.Types.INTEGER;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.FLOAT;

import output.Debug;

import util.Env;
import util.Lib;
import datatype.*;
import datatype.Float;
import fatworm.driver.Schema;

public class BinaryExpr extends Expr {
	
	public Expr left, right;
	public BinaryOp op;
	String name;
	
	public BinaryExpr(Expr l, BinaryOp op, Expr r){
		super();
		left = l;
		this.op = op;
		right = r;
		depth = Lib.max(left.depth, right.depth) + 1;
		size = left.size + right.size + 1;
		isConst = left.isConst && right.isConst;
		value = isConst ? valueExpr() : null;
		aggregation.addAll(left.getAggr());
		aggregation.addAll(right.getAggr());
		type = evalType();
	}
	
	
	


	private DataRecord valueExpr(){
		return valueRaw(left.value, right.value);
	}
	
	public boolean valuePredicate(Env env) {
		DataRecord d = valueExpr(env);
		return Lib.toBoolean(d);
	}

	public DataRecord valueExpr(Env env) {
		if(isConst) return value;
		DataRecord leftValue = left.valueExpr(env);
		DataRecord rightValue = right.valueExpr(env);
		return valueRaw(leftValue, rightValue);
	}

	private DataRecord valueRaw(DataRecord leftValue, DataRecord rightValue) {
		try{
			switch(op){
			case EQUAL:
			case NOT_EQUAL:
			case LESS:
			case LESS_EQ:
			case GREATER:
			case GREATER_EQ:
				return new Bool(leftValue.cmp(op, rightValue));
			case OR:
				return new Bool(Lib.toBoolean(leftValue) || Lib.toBoolean(rightValue));
			case AND:
				return new Bool(Lib.toBoolean(leftValue) && Lib.toBoolean(rightValue));
			case PLUS:
				return add(leftValue, rightValue);
			case MINUS:
				return minus(leftValue, rightValue);
			case MULT:
				return mult(leftValue, rightValue);
			case DIV:
				return div(leftValue, rightValue);
			case MOD:
				return mod(leftValue, rightValue);
			default:
				Debug.err("BinaryExpr error");
			}
		} catch(Exception e){
			//return Null.getInstance();
			e.printStackTrace();
		}
		return Null.getInstance();
	}


	private DataRecord add(DataRecord leftValue, DataRecord rightValue) {
		
		if(leftValue.type == INTEGER && rightValue.type == INTEGER){
			return new Int(((Int)leftValue).value + ((Int)rightValue).value);
		}
		
		BigDecimal ans = leftValue.toBigDecimal().add(rightValue.toBigDecimal());
		if(leftValue.type == DECIMAL || rightValue.type == DECIMAL){
			return new Decimal(ans);
		}
		if(leftValue.type == FLOAT || rightValue.type == FLOAT){
			return new Float(ans.floatValue());
		}
		return Null.getInstance();
	}
	
	private DataRecord minus(DataRecord leftValue, DataRecord rightValue) {
		
		if(leftValue.type == INTEGER && rightValue.type == INTEGER){
			return new Int(((Int)leftValue).value - ((Int)rightValue).value);
		}
		
		BigDecimal ans = leftValue.toBigDecimal().subtract(rightValue.toBigDecimal());
		if(leftValue.type == DECIMAL || rightValue.type == DECIMAL){
			return new Decimal(ans);
		}
		if(leftValue.type == FLOAT || rightValue.type == FLOAT){
			return new Float(ans.floatValue());
		}
		return Null.getInstance();
	}
	
	private DataRecord mult(DataRecord leftValue, DataRecord rightValue) {
		
		if(leftValue.type == INTEGER && rightValue.type == INTEGER){
			return new Int(((Int)leftValue).value * ((Int)rightValue).value);
		}
		
		BigDecimal ans = leftValue.toBigDecimal().multiply(rightValue.toBigDecimal());
		if(leftValue.type == DECIMAL || rightValue.type == DECIMAL){
			return new Decimal(ans);
		}
		if(leftValue.type == FLOAT || rightValue.type == FLOAT){
			return new Float(ans.floatValue());
		}
		return Null.getInstance();
	}
	
	private DataRecord div(DataRecord leftValue, DataRecord rightValue) {
		//****
		if(leftValue.type == INTEGER && rightValue.type == INTEGER){
			return new Float((float)((Int)leftValue).value / (float)((Int)rightValue).value);
		}
		
		BigDecimal ans = leftValue.toBigDecimal().divide(rightValue.toBigDecimal(), 9, BigDecimal.ROUND_HALF_EVEN);
		if(leftValue.type == DECIMAL || rightValue.type == DECIMAL){
			return new Decimal(ans);
		}
		if(leftValue.type == FLOAT || rightValue.type == FLOAT){
			return new Float(ans.floatValue());
		}
		return Null.getInstance();
	}
	
	private DataRecord mod(DataRecord leftValue, DataRecord rightValue) {
		
		if(leftValue.type == INTEGER && rightValue.type == INTEGER){
			return new Int(((Int)leftValue).value % ((Int)rightValue).value);
		}
		return Null.getInstance();
	}
	
	public int evalType(){
		int leftType = left.getType();
		int rightType = right.getType();
		return decideType(leftType, rightType);
	}
	
	public int getType(Schema schema) {
		if(schema == null) return evalType();
		int leftType = left.getType(schema);
		int rightType = right.getType(schema);
		return decideType(leftType, rightType);
	}
	
	private int decideType(int leftType, int rightType) {
		switch(op){
		case EQUAL:
		case NOT_EQUAL:
		case LESS:
		case LESS_EQ:
		case GREATER:
		case GREATER_EQ:
		case OR:
		case AND:
			return java.sql.Types.BOOLEAN;
		case PLUS:
		case MINUS:
		case MULT:
			if(leftType == INTEGER && rightType == INTEGER){
				return INTEGER;
			}
			if(leftType == DECIMAL || rightType == DECIMAL){
				return DECIMAL;
			}
			if(leftType == FLOAT || rightType == FLOAT){
				return FLOAT;
			}
			return java.sql.Types.NULL;
		case DIV:
			if(leftType == INTEGER && rightType == INTEGER){
				return FLOAT;
			}
			if(leftType == DECIMAL || rightType == DECIMAL){
				return DECIMAL;
			}
			if(leftType == FLOAT || rightType == FLOAT){
				return FLOAT;
			}
			return java.sql.Types.NULL;
		case MOD:
			return INTEGER;
		default:
			Debug.err("BinaryExpr error");
		}
		return java.sql.Types.NULL;
	}

	@Override
	public int hashCode(){
		if(Lib.isSymmetricalOp(op)){
			return left.hashCode() ^ right.hashCode() ^ op.hashCode();
		}
		else{
			return toString().hashCode();
		}
	}
	
	@Override
	public String toString(){
		if(name != null) return name;
		name = "" + Env.getNewCount();
		return name;
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof BinaryExpr){
			BinaryExpr e = (BinaryExpr) o;
			
			if(op.equals(e.op)){
				
				if(Lib.isSymmetricalOp(op) ){
					if(left.depth == e.left.depth && left.equals(e.left) && right.equals(e.right)){
						return true;
					}
				}
				else if(left.depth == e.left.depth && left.equals(e.left) && right.equals(e.right)){
					return true;
				}
			}
		}
		return false;
	}
	
	public BinaryExpr toCNF(){
		Expr l = (left instanceof BinaryExpr)? ((BinaryExpr) left).toCNF() : left; 
		Expr r = (right instanceof BinaryExpr)? ((BinaryExpr) right).toCNF() : right; 
		
		BinaryExpr ans = null;
		if(op == BinaryOp.OR){
			if(Lib.isAnd(l)){
				ans = new BinaryExpr(
						new BinaryExpr(((BinaryExpr)l).left.clone(), BinaryOp.OR, r.clone()),
						BinaryOp.AND,
						new BinaryExpr(((BinaryExpr)l).right.clone(), BinaryOp.OR, r.clone())
						);
			} else if(Lib.isAnd(r)){
				ans = new BinaryExpr(
						new BinaryExpr(l.clone(), BinaryOp.OR, ((BinaryExpr)r).left.clone()),
						BinaryOp.AND,
						new BinaryExpr(l.clone(), BinaryOp.OR, ((BinaryExpr)r).right.clone())
						);
			}
		}
		left = l;
		right = r;
		if(ans == null){
			return this;
		}
		else{
			ans = ans.toCNF();
			return ans;
		}
	}
	
	@Override
	public List<String> requestCol() {
		List<String> list = left.requestCol();
		Lib.addAllCol(list, right.requestCol());
		return list;
	}

	@Override
	public void rename(String oldName, String newName) {
		left.rename(oldName, newName);
		right.rename(oldName, newName);
	}

	@Override
	public boolean hasSubquery() {
		return left.hasSubquery() || right.hasSubquery();
	}

	@Override
	public Expr clone() {
		return new BinaryExpr(left.clone(), op, right.clone());
	}

}
