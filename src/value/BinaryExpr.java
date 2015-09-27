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
	public static long time;
	public BinaryOp op;
	public Expr left, right;
	private String name;
	public int easyErr = 0;
	
	public BinaryExpr(Expr l, Expr r, BinaryOp op){
		super();
		right = r;
		left = l;
		this.op = op;
		depth = Lib.max(left.depth, right.depth) + 1;
		size = right.size + left.size + 1;
		Lib.addAll(aggregation, left.getAggr());
		Lib.addAll(aggregation, right.getAggr());
		isConst = left.isConst && right.isConst;
		value = isConst ? valueExpr() : null;
		type = evalType();
	}
	
	
	public boolean valuePredicate(Env env) {
		DataRecord d = valueExpr(env);
		return Lib.toBoolean(d);
	}

	public DataRecord valueExpr(Env env) {
		if(isConst){
			return value;
		}
		DataRecord leftValue = left.valueExpr(env);
		DataRecord rightValue = right.valueExpr(env);
		return valueRaw(leftValue, rightValue);
	}
	
	private DataRecord valueExpr(){
		return valueRaw(left.value, right.value);
	}
	
	private DataRecord valueRaw(DataRecord leftValue, DataRecord rightValue) {
		try{
			switch(op){
			case OR:
				return new Bool(Lib.toBoolean(leftValue) || Lib.toBoolean(rightValue));
			case AND:
				return new Bool(Lib.toBoolean(leftValue) && Lib.toBoolean(rightValue));
			case EQUAL:
			case NOT_EQUAL:
			case LESS:
			case LESS_EQ:
			case GREATER:
			case GREATER_EQ:
				return new Bool(leftValue.cmp(op, rightValue));
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
				Debug.err("BinaryExpr error in BinaryExpr.java");
			}
		} catch(Exception e){
			//return Null.getInstance();
			e.printStackTrace();
		}
		return Null.getInstance();
	}


	private DataRecord add(DataRecord leftValue, DataRecord rightValue) {

		if(leftValue.type == INTEGER && rightValue.type == INTEGER){
			int lv = ((Int)leftValue).value;
			int rv = ((Int)rightValue).value;
			return new Int(lv + rv);
		}
		BigDecimal l = leftValue.toBigDecimal();
		BigDecimal ans = l.add(rightValue.toBigDecimal());
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
			int lv = ((Int)leftValue).value;
			int rv = ((Int)rightValue).value;
			return new Int(lv - rv);
		}
		BigDecimal l = leftValue.toBigDecimal();
		BigDecimal ans = l.subtract(rightValue.toBigDecimal());
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
			int lv = ((Int)leftValue).value;
			int rv = ((Int)rightValue).value;
			return new Int(lv * rv);
		}
		BigDecimal l = leftValue.toBigDecimal();
		BigDecimal ans = l.multiply(rightValue.toBigDecimal());
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
			int lv = ((Int)leftValue).value;
			int rv = ((Int)rightValue).value;
			return new Float((float)lv / (float)rv);
		}
		BigDecimal l = leftValue.toBigDecimal();
		BigDecimal ans = l.divide(rightValue.toBigDecimal(), 9, BigDecimal.ROUND_HALF_EVEN);
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
			int ans = ((Int)leftValue).value % ((Int)rightValue).value;
			return new Int(ans);
		}
		return Null.getInstance();
	}
	
	public int evalType(){
		int leftType = left.getType();
		int rightType = right.getType();
		return decideType(leftType, rightType);
	}
	
	public int getType(Schema schema) {
		if(schema == null){
			return evalType();
		}
		int leftType = left.getType(schema);
		int rightType = right.getType(schema);
		return decideType(leftType, rightType);
	}
	
	private int decideType(int leftType, int rightType) {
		switch(op){
		case PLUS:
		case MULT:
		case MINUS:
			if(leftType == DECIMAL || rightType == DECIMAL){
				return DECIMAL;
			}
			if(leftType == FLOAT || rightType == FLOAT){
				return FLOAT;
			}
			if(leftType == INTEGER && rightType == INTEGER){
				return INTEGER;
			}
			return java.sql.Types.NULL;
		case EQUAL:
		case NOT_EQUAL:
		case LESS:
		case LESS_EQ:
		case GREATER:
		case GREATER_EQ:
		case OR:
		case AND:
	
			return java.sql.Types.BOOLEAN;
		case MOD:
			return INTEGER;
		case DIV:
			if(leftType == DECIMAL || rightType == DECIMAL){
				return DECIMAL;
			}
			if(leftType == FLOAT || rightType == FLOAT){
				return FLOAT;
			}
			if(leftType == INTEGER && rightType == INTEGER){
				return FLOAT;
			}
			
			return java.sql.Types.NULL;
		
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
		if(name == null) {		
			name = "" + Env.getNewCount();
		}
		return name + " " + left.toString() + " " + op + " " + right.toString();
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof BinaryExpr){
			BinaryExpr e = (BinaryExpr) o;
			
			if(op.equals(e.op)){
				
				if(Lib.isSymmetricalOp(op) ){
					if(left.depth == e.right.depth && left.equals(e.right) && right.equals(e.left)){
						return true;
					}
				}
				if(left.depth == e.left.depth && left.equals(e.left) && right.equals(e.right)){
					return true;
				}
			}
		}
		return false;
	}
	
	public BinaryExpr toCNF(){
		Expr l; 
		if(left instanceof BinaryExpr){
			l = ((BinaryExpr) left).toCNF(); 
		}
		else{
			l = left;
		}
		Expr r;
		if(right instanceof BinaryExpr){
			r = ((BinaryExpr) right).toCNF(); 
		}
		else{
			r = right;
		}
		
		BinaryExpr ans = null;
		if(op == BinaryOp.OR){
			Expr ltmp, rtmp;
			if(Lib.isAnd(l)){
				ltmp = ((BinaryExpr)l).left.clone();
				rtmp = ((BinaryExpr)l).right.clone();
				ans = new BinaryExpr(
						new BinaryExpr(ltmp, r.clone(), BinaryOp.OR),
						new BinaryExpr(rtmp, r.clone(), BinaryOp.OR),
						BinaryOp.AND
						);
			} 
			else if(Lib.isAnd(r)){
				ltmp = ((BinaryExpr)r).left.clone();
				rtmp = ((BinaryExpr)r).right.clone();
				ans = new BinaryExpr(
						new BinaryExpr(l.clone(), ltmp, BinaryOp.OR),
						new BinaryExpr(l.clone(), rtmp, BinaryOp.OR),
						BinaryOp.AND
						);
			}
		}
		right = r;
		left = l;
		if(ans != null){
			ans = ans.toCNF();
			return ans;
		}
		else{
			return this;
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
		return new BinaryExpr(left.clone(), right.clone(), op);
	}

}
