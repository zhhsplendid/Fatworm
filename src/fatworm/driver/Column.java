package fatworm.driver;

import java.io.Serializable;
import java.util.GregorianCalendar;

import org.antlr.runtime.tree.Tree;

import util.Env;
import util.Lib;

import datatype.*;


public class Column implements Serializable{
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = -3968956033308150315L;
	
	public String name;
	public int type;
	
	boolean notNull;
	boolean autoIncrement;
	boolean primaryKey;
	
	private int autoIndex;
	
	public Object defaultValue;

	public int datalength1;
	public int datalength2;
	
	public Column(String s, int t, boolean nullFlag, boolean autoFlag, DataRecord dv){
		name = s;
		type = t;
		notNull = nullFlag;
		autoIncrement = autoFlag;
		defaultValue = dv;
		autoIndex = 1;
	}
	
	public Column(String s, int t){
		this(s, t, false, false, null);
	}
	
	public void setAutoIndex(int x){
		autoIndex = x;
	}
	
	public int getAutoIndex(){
		return autoIndex++;
	}
	
	public boolean notNull(){
		return notNull;
	}
	
	public boolean autoIncrement(){
		return autoIncrement;
	}
	
	public boolean primaryKey(){
		return primaryKey;
	}
	
	public boolean hasDefault(){
		return defaultValue != null;
	}
	
	public DataRecord getDefault(){
		return DataRecord.fromObject(defaultValue);
	}

	public DataRecord getDataRecord(Tree t) {
		if(t == null || t.getText().equalsIgnoreCase("null") || t.getText().equalsIgnoreCase("default") ){
			if(hasDefault()){
				return getDefault();
			} 
			else if(autoIncrement()){
				return new Int(getAutoIndex());
			} 
			else if(type == java.sql.Types.TIMESTAMP){
				long time = (new GregorianCalendar()).getTimeInMillis();
				return new Timestamp(new java.sql.Timestamp(time));
			}
			else if(type == java.sql.Types.DATE){
				long time = (new GregorianCalendar()).getTimeInMillis();
				return new Date(new java.sql.Timestamp(time));
			} 
		}
		return DataRecord.fromString(type, Lib.getExpr(t).valueExpr(new Env()).toString());
	}
	
}
