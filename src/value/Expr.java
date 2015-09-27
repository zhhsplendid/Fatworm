package value;

import java.util.HashSet;
import java.util.List;

import util.Env;

import datatype.DataRecord;
import fatworm.driver.Schema;

public abstract class Expr {
	public static long time;
	public int type;
	public Integer size = 1, depth = 1;
	public DataRecord value = null;
	public boolean isConst = false;
	public HashSet<FunctionExpr> aggregation = new HashSet<FunctionExpr>();
	
	
	public Expr(){
		
	}
	
	public HashSet<FunctionExpr> getAggr(){
		return aggregation;
	}
	
	public boolean hasAggr(){
		return ! aggregation.isEmpty();
	}
	
	public int getType(){
		return type;
	}
	public int getType(Schema schema){
		return type;
	}
	@Override
	public boolean equals(Object o){
		if(o instanceof Expr){
			Expr e = (Expr) o;
			return toString().equalsIgnoreCase(e.toString());
		}
		return false;
	}
	
	@Override
	public int hashCode(){
		return toString().hashCode();
	}
	
	//eval as predicate
	public abstract boolean valuePredicate(Env env);
		
	//eval as an expression
	public abstract DataRecord valueExpr(Env env);
		
	public abstract List<String> requestCol();
	
	public abstract void rename(String oldName, String newName);
	
	public abstract boolean hasSubquery();
	
	public abstract Expr clone();
}
