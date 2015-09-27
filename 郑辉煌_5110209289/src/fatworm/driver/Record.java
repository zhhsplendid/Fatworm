package fatworm.driver;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
		if(index < 0){
			Debug.warn("index < 0");
			return Null.getInstance();
		}
		return cols.get(index);
	}
	
	
	public void addField(DataRecord d){
		cols.add(d);
	}

	public void autoFill() {
		for(int i = 0; i < schema.columnName.size(); ++i){
			String colName = schema.columnName.get(i);
			Column column = schema.getColumn(colName);
			if(column.hasDefault() || column.type == java.sql.Types.TIMESTAMP){
				cols.add(column.getDataRecord(null));
			}
			else{
				cols.add(null);
			}
		}
		
	}

	public byte[] toByte() {
		RecordByte rb = new RecordByte();
		int nullMap = 0;
		for(int i = 0; i < schema.columnName.size(); ++i){
			if(cols.get(i) == null || cols.get(i).type == java.sql.Types.NULL){
				nullMap |= (1 << i);
			}
		}
		rb.putInt(nullMap);
		for(int i = 0; i < schema.columnName.size(); ++i){
			if(cols.get(i) != null && cols.get(i).type != java.sql.Types.NULL){
				Column c = schema.getColumn(i);
				cols.get(i).buffByte(rb, c.datalength1, c.datalength2);
			}
		}
		byte[] ans = new byte[rb.size()];
		System.arraycopy(rb.getBytes(), 0, ans, 0, ans.length);
		return ans;
	}

	public static Record fromByte(Schema schema, byte[] b) {
		Record record = new Record(schema);
		ByteBuffer buff = ByteBuffer.wrap(b);
		Integer nullMap = buff.getInt();
		for(int i = 0; i < schema.columnName.size(); ++i){
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
	
	public void addFieldFromExpr(Env env, List<Expr> function) {
		if(function.size() == 0 || Lib.trim(function.get(0).toString()).equals("*")){
			for(int i = 0; i < schema.columnName.size(); ++i){
				String col = schema.columnName.get(i);
				DataRecord dr = env.get(col);
				if(dr == null){
					dr = env.get(Lib.getAttributeName(col));
				}
				cols.add(dr);
			}
		}else{
			for(int i = 0; i < function.size(); ++i){
				Expr e = function.get(i);
				DataRecord dr = env.get(e.toString());
				if(dr == null || dr instanceof FunctionExpr.FunctionRecord){
					dr = e.valueExpr(env);
				}
				cols.add(dr);
			}
		}
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Record))
			return false;

		Record r = (Record) o;

		return Lib.collectionEquals(cols, r.cols);
	}
	
	@Override
	public int hashCode() {
		return Lib.collectionHashCode(cols);
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
