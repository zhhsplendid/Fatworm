package fatworm.driver;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import output.Debug;
import util.Env;
import util.Lib;
import value.Expr;
import value.FunctionExpr;
import datatype.*;
import filesys.RecordByte;

/**
 * class Record is a class just has schema and a array list to maintain DataRecord
 * @author bd
 *
 */

public class Record implements Serializable{
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = -8098436159433663823L;
	
	public ArrayList<DataRecord> cols = new ArrayList<DataRecord>();
	public Schema schema;
	
	public Record(Schema s){
		schema = s;
	}
	
	public DataRecord getField(String s) {
		int index = schema.indexOf(s);
		return getField(index);
	}
	
	public DataRecord getField(int index) {
		boolean indexWarn = index < 0;
		if(indexWarn){
			Debug.warn("index < 0");
			return Null.getInstance();
		}
		return cols.get(index);
	}
	
	
	public void addField(DataRecord d){
		cols.add(d);
	}
	
	public void addField(Collection<DataRecord> c){
		cols.addAll(c);
	}

	public void autoFill() {
		int size = schema.columnName.size();
		for(int i = 0; i < size; ++i){
			String colName = schema.columnName.get(i);
			Column column = schema.getColumn(colName);
			if(column.type == java.sql.Types.TIMESTAMP || column.hasDefault()){
				DataRecord defaultValue = column.getDataRecord(null);
				cols.add(defaultValue);
			}
			else{
				cols.add(null);
			}
		}
		
	}

	public byte[] toByte() {
		RecordByte rb = new RecordByte();
		int nullMap = 0;
		int size = schema.columnName.size();
		for(int i = 0; i < size; ++i){
			if(cols.get(i) == null || cols.get(i).type == java.sql.Types.NULL){
				nullMap |= (1 << i);
			}
		}
		rb.putInt(nullMap);
		for(int i = 0; i < size; ++i){
			if(cols.get(i) != null && cols.get(i).type != java.sql.Types.NULL){
				Column c = schema.getColumn(i);
				cols.get(i).buffByte(rb, c.datalength1);
			}
		}
		int len = rb.size();
		byte[] ans = new byte[len];
		System.arraycopy(rb.getBytes(), 0, ans, 0, len);
		return ans;
	}

	public static Record fromByte(byte[] b, Schema schema) {
		Record record = new Record(schema);
		ByteBuffer buff = ByteBuffer.wrap(b);
		Integer nullMap = buff.getInt();
		int size = schema.columnName.size();
		for(int i = 0; i < size; ++i){
			if(((nullMap >> i) & 1) == 1){
				record.cols.add(Null.getInstance());
			}
			else{
				record.cols.add(DataRecord.fromBytes(buff, schema.getColumn(i).type));
			}
		}
		return record;
	}

	public void setDataRecord(String colName, DataRecord record) {
		cols.set(schema.indexOf(colName), record);
	}
	
	public void addFieldFromExpr(List<Expr> function, Env env) {
		if(function.size() != 0 && !Lib.trim(function.get(0).toString()).equals("*")){
			int size = function.size();
			for(int i = 0; i < size; ++i){
				Expr e = function.get(i);
				DataRecord dr = env.get(e.toString());
				if(dr == null || dr instanceof FunctionExpr.FunctionRecord){
					dr = e.valueExpr(env);
				}
				cols.add(dr);
			}
		}
		else{
			int size = schema.columnName.size();
			for(int i = 0; i < size; ++i){
				String col = schema.columnName.get(i);
				DataRecord dr = env.get(col);
				if(dr == null){
					String toGet = Lib.getAttributeName(col);
					dr = env.get(toGet);
				}
				cols.add(dr);
			}
		}
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Record)){
			return false;
		}
		return Lib.collectionEquals(cols, ((Record) o).cols);
	}
	
	@Override
	public int hashCode() {
		return (int)Lib.collectionHashCode(cols);
	}
	
	@Override
	public String toString(){
		String s = "(";
		for(int i = 0; i < cols.size(); ++i){
			s = s + cols.get(i) + " ";
		}
		s = s + ")";
		return s;
	}
}
