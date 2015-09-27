package filesys;

import java.nio.charset.Charset;

import output.Debug;

import datatype.*;
import fatworm.driver.IOTable;


public class BKeyManager{
	public long time;
	public static final int MOD = MyFile.BTREE_PAGE_SIZE / 4;
	public static final int BYTES_PER_CHAR = (int) Charset.defaultCharset().newEncoder().maxBytesPerChar();
	public int keyType;
	
	public IOTable table;
	public BTreePage root;
	public BufferManager manager;
	public int easyErrCount = 0; // for debug;
	
	public BKeyManager(BufferManager m, IOTable t, int type) throws Throwable{
		manager = m;
		keyType = type;
		table = t;
		root = manager.getBTreePage(manager.newPage(), type, this, true);
	}
	
	public BKeyManager(BufferManager m, Integer pageID, int type, IOTable t) throws Throwable{
		manager = m;
		keyType = type;
		table = t;
		root = manager.getBTreePage(pageID, type, this, false);
	}
	
	private void countWrite(){
		++easyErrCount;
	}
	
	public BKey newBKey(DataRecord d){
		java.sql.Timestamp timevalue;
		switch(d.type){
		case java.sql.Types.BOOLEAN:
			boolean flag = ((Bool)d).value;
			if(flag){
				return new IntKey(1);
			}
			else{
				return new IntKey(0);
			}
		case java.sql.Types.INTEGER:
			return new IntKey(((Int)d).value);
		case java.sql.Types.FLOAT:
			return new FloatKey(((datatype.Float)d).value);
		case java.sql.Types.CHAR:
			return new StringKey(((datatype.Char)d).value, manager);
		case java.sql.Types.VARCHAR:
			return new StringKey(((Varchar)d).value, manager);
		case java.sql.Types.DECIMAL:
			return new DecimalKey(((Decimal)d).value, manager);
		case java.sql.Types.DATE:
			timevalue = ((Date)d).value;
			return new TimestampKey(timevalue);
		case java.sql.Types.TIMESTAMP:
			timevalue = ((Timestamp)d).value;
			return new TimestampKey(timevalue);
		default:
			countWrite();
			timevalue = null;
			Debug.err("not type for bkey");
		}
		return null;
	}
	
	public BKey getBKey(int n, int type) throws Throwable{
		countWrite();
		switch(type){
		case java.sql.Types.FLOAT:
			return new FloatKey(n);
		case java.sql.Types.INTEGER:
		case java.sql.Types.BOOLEAN:
			return new IntKey(n);
		case java.sql.Types.DECIMAL:
			return new DecimalKey(n, manager);
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
			return new StringKey(n, manager);
		default:
			Debug.err("not suitable bkey");
		}
		return null;
	}
	
	public BKey getBKey(long n, int type){
		if(type != java.sql.Types.TIMESTAMP && type != java.sql.Types.DATE){
			Debug.err("BKeyManager.java getBkey(long, int) err");
		}
		return new TimestampKey(n);
		/*
		switch(type){
		case java.sql.Types.TIMESTAMP:
		case java.sql.Types.DATE:
			return new TimestampKey(n);
		default:
			Debug.err("not suitable bkey");
		}
		return null;
		*/
	}
	
	
}
