package fatworm.driver;

import java.io.Serializable;
import java.util.*;

public class Index implements Serializable {

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
	
	public Index(String s, IOTable t, Column c, boolean u){
		indexName = s;
		table = t;
		column = c;
		unique = u;
		colIndex = t.schema.indexOf(c.name);
	}
	
	public Index(String s, IOTable t, Column c){
		indexName = s;
		table = t;
		column = c;
		unique = false;
		colIndex = t.getSchema().indexOf(c.name);
	}

}
