package value;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

import output.Debug;
import datatype.DataRecord;
import datatype.Decimal;
import datatype.Int;
import datatype.Null;
import fatworm.driver.Column;
import fatworm.driver.Schema;
import filesys.RecordByte;
import parser.FatwormParser;
import util.Env;
import util.Lib;

public class FunctionExpr extends Expr{
	public static long time;
	int funcType;
	
	public String col;
	public String name;
	
	boolean hasEvalFunction;
	
	public FunctionExpr(int ft, String s){
		super();
		col = s;
		
		
		hasEvalFunction = false;
		aggregation.add(this);
		type = java.sql.Types.NULL;
		funcType = ft;
		switch(ft){
		case FatwormParser.COUNT: //FIXME
			type = java.sql.Types.INTEGER;
			break;
		case FatwormParser.AVG:
		case FatwormParser.SUM:
			type = java.sql.Types.DECIMAL;
			break;
		}
		
	}

	
	
	public String toString(){
		if(name == null){
			name = "" + Env.getNewCount();
		}
		return name;
	}

	@Override
	public boolean valuePredicate(Env env) {
		return Lib.toBoolean(valueExpr(env));
	}

	@Override
	public DataRecord valueExpr(Env env) {
		/*
		if(! hasEvalFunction){
			Debug.warn("has't aggregate but call function evaluation");
		}*/
		
		DataRecord d = env.get(this.toString());
		if(d == null){
			d = FunctionRecord.newFunc(funcType);
			String s = toString();
			env.put(s, d);
		}
		
		//if(d instanceof FunctionRecord){
		return ((FunctionRecord) d).getResult();
		//}
		//return d;
	}
	
	public void valueFunction(Env env){
		hasEvalFunction = true;
		DataRecord d = env.get(toString());
		if(d == null){
			d = FunctionRecord.newFunc(funcType);
			String s = toString();
			env.put(s, d);
		}
		//if(d instanceof FunctionRecord){
		((FunctionRecord)d).countData(env.get(col));
		//} 
		//else{
		//	Debug.err("what's wrong?");
		//}
	}
	
	public FunctionRecord valueFunction(FunctionRecord f, DataRecord d){
		hasEvalFunction = true;
		if(f == null){
			f = FunctionRecord.newFunc(funcType);
		}
		//if(f instanceof FunctionRecord){
		((FunctionRecord) f).countData(d);
		//} 
		//else{
		//	Debug.err("what's wrong!");
		//}
		return f;
	}
	
	public boolean canGetValue(Schema schema){
		return schema.indexOfStrictString(col) >= 0;
	}
	@Override
	public int hashCode(){
		return toString().hashCode();
	}
	@Override
	public int getType(Schema schema){
		if(type == java.sql.Types.NULL){
			Column colm = schema.getColumn(col);
			type = colm.type;
		}
		return type;
	}
	
	@Override
	public List<String> requestCol() {
		List<String> list = new LinkedList<String> ();
		list.add(col);
		return list;
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof FunctionExpr){
			return ((FunctionExpr)o).toString().equals(toString());
		}
		return false;
	}
	
	@Override
	public void rename(String oldName, String newName) {
		if(oldName.equalsIgnoreCase(col)){
			col = newName;
		}
	}

	@Override
	public boolean hasSubquery() {
		return false;
	}

	@Override
	public Expr clone() {
		return new FunctionExpr(funcType, col);
	}
	
	
	public abstract static class FunctionRecord extends DataRecord{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1052816715469181025L;
		
		public FunctionRecord(){}
		
		@Override
		public boolean cmp(BinaryOp op, DataRecord d){
			//Debug.warn("this function should never use");
			return getResult().cmp(op, d);
			//return false;
		}
		
		@Override
		public String toString(){
			DataRecord res = getResult();
			return res.toString();
		}
		
		public static FunctionRecord newFunc(int funcType){
			switch(funcType){
			case FatwormParser.COUNT:
				return new COUNTRecord();
			case FatwormParser.SUM:
				return new SUMRecord();
			case FatwormParser.AVG:
				return new AVGRecord();
			case FatwormParser.MAX:
				return new MAXRecord();
			case FatwormParser.MIN:
				return new MINRecord();
			default:
				return null;
			}
		}
		
		public abstract DataRecord getResult();
		
		public abstract void countData(DataRecord d);
		
	}
	public static class COUNTRecord extends FunctionRecord{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1498047552195033366L;
		int count;
		
		public COUNTRecord(){
			count = 0;
		}
		
		@Override
		public DataRecord getResult() {
			//*****
			return new Int(count);
		}

		@Override
		public void countData(DataRecord d) {
			++count;
		}

		@Override
		public void buffByte(RecordByte b, int a) {
			// TODO no use
			
		}
		
	}
	public static class SUMRecord extends FunctionRecord{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 6217177522588933839L;
		
		BigDecimal result;
		boolean isNull;
		
		public SUMRecord(){
			result = new BigDecimal(0).setScale(10);
			isNull = true;
		}
		
		@Override
		public DataRecord getResult() {
			if(isNull){
				return Null.getInstance();
			}
			return new Decimal(result);
		}

		@Override
		public void countData(DataRecord d) {
			if(d instanceof Null || d.type == java.sql.Types.NULL){
				return;
			}
			isNull = false;
			result = result.add(d.toBigDecimal());
		}

		@Override
		public void buffByte(RecordByte b, int a) {
			// TODO no use
		}
		
	}
	
	public static class AVGRecord extends FunctionRecord{

		/**
		 * 
		 */
		private static final long serialVersionUID = -7178777817664003707L;
		
		long count;
		BigDecimal result;
		boolean isNull;
		
		public AVGRecord(){
			result = new BigDecimal(0).setScale(10);
			isNull = true;
			count = 0;
		}
		
		@Override
		public DataRecord getResult() {
			if(isNull){
				return Null.getInstance();
			}
			return new Decimal(result.divide(new BigDecimal(count), 10, BigDecimal.ROUND_HALF_EVEN));
		}

		@Override
		public void countData(DataRecord d) {
			if(d instanceof Null || d.type == java.sql.Types.NULL){
				return;
			}
			isNull = false;
			++count;
			result = result.add(d.toBigDecimal());
		}

		@Override
		public void buffByte(RecordByte b, int a) {
			// TODO no use
			
		}
		
	}
	
	public static class MAXRecord extends FunctionRecord{

		/**
		 * 
		 */
		private static final long serialVersionUID = -3379462524032148773L;
		
		DataRecord result;
		boolean isNull;
		
		public MAXRecord(){
			isNull = true;
			result = Null.getInstance();
		}
		
		
		@Override
		public DataRecord getResult() {
			return result;
		}

		@Override
		public void countData(DataRecord d) {
			if(d instanceof Null || d.type == java.sql.Types.NULL){
				return;
			}
			if(isNull){
				isNull = false;
				result = d;
				//type = d.type;
			}
			else if(result.cmp(BinaryOp.LESS, d)){
				result = d;
				//type = d.type;
			}
		}

		@Override
		public void buffByte(RecordByte b, int a) {
			// TODO no use
			
		}
		
	}
	
	public static class MINRecord extends FunctionRecord{

		/**
		 * 
		 */
		private static final long serialVersionUID = 8553490032849178398L;

		DataRecord result;
		boolean isNull;
		
		public MINRecord(){
			isNull = true;
			result = Null.getInstance();
		}
		
		
		@Override
		public DataRecord getResult() {
			return result;
		}

		@Override
		public void countData(DataRecord d) {
			if(d instanceof Null || d.type == java.sql.Types.NULL){
				return;
			}
			if(isNull){
				isNull = false;
				result = d;
				//type = d.type;
			}
			else if(result.cmp(BinaryOp.GREATER, d)){
				result = d;
				//type = d.type;
			}
		}

		@Override
		public void buffByte(RecordByte b, int a) {
			// TODO no use
			
		}
		
	}
	
}
