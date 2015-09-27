package fatworm.driver;

import java.io.Serializable;
import java.util.*;

public class Index implements Serializable {
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = 6486033379818468336L;
	
	public boolean unique;
	public String indexName;
	public IOTable table;
	public Column column;
	public int colIndex;
	public Integer pageID;

	public ArrayList<List<Integer>> bucket = new ArrayList<List<Integer>>();
	
	public Index(IOTable t, Column c, String s, boolean u){
		indexName = s;
		table = t;
		column = c;
		unique = u;
		Schema tmp = t.getSchema();
		colIndex = tmp.indexOf(c.name);
	}
	
	public Index(IOTable t, Column c, String s){
		indexName = s;
		table = t;
		column = c;
		unique = false;
		Schema tmp = t.getSchema();
		colIndex = tmp.indexOf(c.name);
	}

}
