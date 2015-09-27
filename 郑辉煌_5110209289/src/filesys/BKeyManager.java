package filesys;

import java.nio.charset.Charset;

import output.Debug;

import datatype.*;
import fatworm.driver.IOTable;


public class BKeyManager{
	
	public static final int MOD = MyFile.BTREE_PAGE_SIZE / 4;
	public static final int BYTES_PER_CHAR = (int) Charset.defaultCharset().newEncoder().maxBytesPerChar();
	public static final int LONG_SIZE = Long.SIZE / Byte.SIZE;
	public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
	
	public int keyType;
	public BTreePage root;
	public BufferManager manager;
	public IOTable table;
	
	public BKeyManager(BufferManager m, int type, IOTable t) throws Throwable{
		manager = m;
		keyType = type;
		table = t;
		root = manager.getBTreePage(this, manager.newPage(), type, true);
	}
	
	public BKeyManager(BufferManager m, Integer pageID, int type, IOTable t) throws Throwable{
		manager = m;
		keyType = type;
		table = t;
		root = manager.getBTreePage(this, pageID, type, false);
	}
	
	public BKey newBKey(DataRecord d){
		switch(d.type){
		case java.sql.Types.BOOLEAN:
			return new IntKey(((Bool)d).value ? 1 : 0);
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
			return new TimestampKey(((Date)d).value);
		case java.sql.Types.TIMESTAMP:
			return new TimestampKey(((Timestamp)d).value);
		default:
			Debug.err("not type for bkey");
		}
		return null;
	}
	
	public BKey getBKey(int n, int type) throws Throwable{
		switch(type){
		case java.sql.Types.BOOLEAN:
		case java.sql.Types.INTEGER:
			return new IntKey(n);
		case java.sql.Types.FLOAT:
			return new FloatKey(n);
		case java.sql.Types.CHAR:
		case java.sql.Types.VARCHAR:
			return new StringKey(n, manager);
		case java.sql.Types.DECIMAL:
			return new DecimalKey(n, manager);
		default:
			Debug.err("not suitable bkey");
		}
		return null;
	}
	
	public BKey getBKey(long n, int type){
		switch(type){
		case java.sql.Types.DATE:
		case java.sql.Types.TIMESTAMP:
			return new TimestampKey(n);
		default:
			Debug.err("not suitable bkey");
		}
		return null;
	}
	
	
}
