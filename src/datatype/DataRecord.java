package datatype;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import value.BinaryOp;

import filesys.RecordByte;



public abstract class DataRecord implements Serializable{
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = 6031165435271719803L;
	
	public int type;
	
	/**
	 * construct a data record from input string and int type
	 * @param type
	 * @param s
	 * @return
	 */
	public static DataRecord fromString(int type, String s){
		
		if(s.equalsIgnoreCase("null")){
			return Null.getInstance();
		}
		switch(type){
		case java.sql.Types.INTEGER:
			return new Int(s);
		case java.sql.Types.FLOAT:
			return new Float(s);
		case java.sql.Types.BOOLEAN:
			return new Bool(s);
		case java.sql.Types.VARCHAR:
			return new Varchar(s);
		case java.sql.Types.CHAR:
			return new Char(s);
		case java.sql.Types.DATE:
			return new Date(s);
		case java.sql.Types.DECIMAL:
			return new Decimal(s);
		case java.sql.Types.NULL:
			return Null.getInstance();
		case java.sql.Types.TIMESTAMP:
			return new Timestamp(s);
		
		
		default:
			return null;
		}
	}
	
	public static DataRecord fromBytes(ByteBuffer buff, int type){
		int length = 0;
		long ltmp;
		
		switch(type){
		
		case java.sql.Types.INTEGER:
			return new Int(buff.getInt());
		case java.sql.Types.BOOLEAN:
			return new Bool(buff.get()!=0);
		case java.sql.Types.FLOAT:
			return new Float(buff.getFloat());
		case java.sql.Types.CHAR:
			length = buff.getInt();
			byte [] b = new byte[length];
			buff.get(b, 0, length);
			return new Char(new String(b));
		case java.sql.Types.VARCHAR:
			length = buff.getInt();
			b = new byte[length];
			buff.get(b, 0, length);
			return new Varchar(new String(b));
		case java.sql.Types.DECIMAL:
			int scale = buff.getInt();
			length = buff.getInt();
			b = new byte[length];
			buff.get(b, 0, length);
			BigDecimal bdtmp = new BigDecimal(new BigInteger(b), scale);
			return new Decimal(bdtmp);
		case java.sql.Types.NULL:
			return Null.getInstance();
		case java.sql.Types.TIMESTAMP:
			ltmp = buff.getLong();
			return new Timestamp(ltmp);
		case java.sql.Types.DATE:
			ltmp = buff.getLong();
			return new Date(ltmp);
		
		default:
			return Null.getInstance();
		}
	}

	public static DataRecord fromObject(Object f) {
		if(f instanceof DataRecord){
			return (DataRecord)f;
		}
		
		if(f instanceof Boolean)
			return new Bool((Boolean)f);
		if(f instanceof Integer)
			return new Int((Integer)f);
		if(f instanceof String)
			return new Varchar((String)f);
		if(f instanceof BigDecimal)
			return new Decimal((BigDecimal)f);
		if(f instanceof java.lang.Float)
			return new Float((java.lang.Float)f);
		if(f instanceof java.sql.Timestamp)
			return new Timestamp((java.sql.Timestamp)f);
		if(f instanceof String && ((String)f).length() == 1)
			return new Char((String)f);
		if(f instanceof java.sql.Date)
			return new Date(new java.sql.Timestamp(((java.sql.Date)f).getTime()));
		
		return Null.getInstance();
	}

	public static Object getObject(DataRecord f) {
		//TODO delete a example sentence
		if(f == null){
			return null;
		}
		
		switch(f.type){
		case java.sql.Types.NULL:
			return null;
		case java.sql.Types.INTEGER:
			return new Integer(((Int)f).value);
		case java.sql.Types.DECIMAL:
			return ((Decimal)f).toBigDecimal();
		case java.sql.Types.FLOAT:
			return new java.lang.Float(((Float)f).value);
		case java.sql.Types.BOOLEAN:
			return new Boolean(((Bool)f).value);
		case java.sql.Types.CHAR:
			return new String(((Char)f).value);
		case java.sql.Types.VARCHAR:
			return new String(((Varchar)f).value);
		case java.sql.Types.DATE:
			return new java.sql.Timestamp(((Date)f).value.getTime());
		case java.sql.Types.TIMESTAMP:
			return new java.sql.Timestamp(((Timestamp)f).value.getTime());
		
		
		default:
			return new String(f.toString());
		}
	}
	
	//FIXME
	public boolean equals(Object o){
		if(!(o instanceof DataRecord)){
			return false;
		}
		return toString().equals(o.toString());
	}
	
	
	public BigDecimal toBigDecimal() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public abstract void buffByte(RecordByte b, int a);
	public abstract boolean cmp(BinaryOp op, DataRecord d);
	@Override
	public abstract String toString();
	//@Override
	//public abstract int hashCode();
	
}
