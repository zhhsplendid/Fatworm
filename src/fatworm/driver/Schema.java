package fatworm.driver;

import java.io.Serializable;
import java.util.*;

import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

import datatype.DataRecord;
import output.Debug;

import parser.FatwormParser;
import util.Lib;
import value.Expr;



public class Schema implements Serializable{
	public long time;
	/**
	 * 
	 */
	private static final long serialVersionUID = -2091027189043543173L;
	
	public String tableName;
	
	public ArrayList<String> columnName;
	
	public Column primaryKey;
	public HashMap<String, Column> colMap;
	
	
	public Schema(){
		tableName = "toBeDone";
		columnName = new ArrayList<String>();
		colMap = new HashMap<String, Column>();
	}
	
	public Schema(String t){
		columnName = new ArrayList<String>();
		colMap = new HashMap<String, Column>();
		tableName = t;
	}
	
	/**
	 * this function must be called for a tree to construct a schema
	 * @param t
	 */
	public Schema(Tree tree){
		columnName = new ArrayList<String>();
		colMap = new HashMap<String, Column>();
		tableName = tree.getChild(0).getText();
		int count = tree.getChildCount();
		for(int i = 1; i < count; ++i){
			Tree child = tree.getChild(i);
			String colName = child.getChild(0).getText();
			int type = child.getType();
			switch(type){
			case FatwormParser.CREATE_DEFINITION:
				columnName.add(colName);
				Tree dataType = child.getChild(1);
				Column col = null;
				switch(dataType.getType()){
				case FatwormParser.INT:
					col = new Column(colName, java.sql.Types.INTEGER);
					break;
				case FatwormParser.FLOAT:
					col = new Column(colName, java.sql.Types.FLOAT);
					break;
				
				case FatwormParser.BOOLEAN:
					col = new Column(colName, java.sql.Types.BOOLEAN);
					break;
				case FatwormParser.CHAR:
					col = new Column(colName, java.sql.Types.CHAR);
					col.datalength1 = Integer.parseInt(dataType.getChild(0).getText());
					break;
				case FatwormParser.VARCHAR:
					col = new Column(colName, java.sql.Types.VARCHAR);
					col.datalength1 = Integer.parseInt(dataType.getChild(0).getText());
					break;
				case FatwormParser.TIMESTAMP:
					col = new Column(colName, java.sql.Types.TIMESTAMP);
					break;
				case FatwormParser.DATETIME:
					col = new Column(colName, java.sql.Types.DATE);
					break;
				case FatwormParser.DECIMAL:
					col = new Column(colName, java.sql.Types.DECIMAL);
					int cnt = dataType.getChildCount();
					if(cnt == 2){
						col.datalength1 = Integer.parseInt(dataType.getChild(0).getText());
						col.datalength2 = Integer.parseInt(dataType.getChild(1).getText());
					}
					else if(cnt == 1){
						col.datalength1 = Integer.parseInt(dataType.getChild(0).getText());
					}
					break;		
				default:
					Debug.err("Schema undefined dataType");
				}
				colMap.put(colName, col);
				int countChild = child.getChildCount();
				for(int j = 2; j < countChild; ++j){
					Tree option = child.getChild(j);
					switch(option.getType()){
					case FatwormParser.DEFAULT:
						DataRecord tmp = col.getDataRecord(option.getChild(0));
						col.defaultValue = DataRecord.getObject(tmp);
						break;
					case FatwormParser.NULL:
						col.notNull = option.getChildCount() != 0;
						break;
					case FatwormParser.AUTO_INCREMENT:
						col.autoIncrement = true;
						break;
					}
				}
				break;
			case FatwormParser.PRIMARY_KEY:
				//this has to be postponed...
				break;
				default:
					Debug.err("tree error");
			}
		}
		count = tree.getChildCount();
		for(int i = 1; i < count; ++i){
			Tree child = tree.getChild(i);
			String colName = child.getChild(0).getText();
			if(child.getType() == FatwormParser.PRIMARY_KEY){//may be wrong
				primaryKey = getColumn(colName);
				primaryKey.primaryKey = true;
				break;
			}
		}
	}
	
	public Column getColumn(int index) {
		return getColumn(columnName.get(index));
	}
	
	public Column getColumn(String name){
		boolean contain = colMap.containsKey(name);
		if(contain){
			return colMap.get(name);
		}
		
		String attr = Lib.getAttributeName(name);
		contain = colMap.containsKey(attr);
		if(contain){
			return colMap.get(attr);
		}
		
		String col = tableName + "." + attr;
		contain = colMap.containsKey(col);
		if(contain){
			return colMap.get(col);
		}
		// FIXME hack for table name mismatch due to Join
		int i = indexOf(name);
		if(i < 0){
			return null;
		}
		
		return colMap.get(columnName.get(i));
		//return null;
	}
	
	public int indexOf(String s) {
		if(s == null){
			return -1;
		}
		int size = columnName.size();
		for(int i = 0; i < size; ++i){
			String y = columnName.get(i);
			if(s.equalsIgnoreCase(y) || s.equalsIgnoreCase(this.tableName + "." + y)){
				return i;
			}
		}
		size = columnName.size();
		for(int i = 0; i < size; ++i){
			String y = columnName.get(i);
			if(!y.contains(".")){
				continue;
			}
			// FIXME hack for table name mismatch due to Join
			if(Lib.getAttributeName(s).equalsIgnoreCase(Lib.getAttributeName(y))){
				return i;
			}
		}
		return -1;
	}
	
	public int indexOfStrictString(String col) {
		int size = columnName.size();
		for(int i = 0; i < size; ++i){
			String name = columnName.get(i);
			if(Lib.colNameEqual(col, name)){
				return i;
			}
		}
		return -1;
	}
	
	
	/**for temporary use */
	private ArrayList<String> onlyColName;
	private ArrayList<String> colWithTableName;


	
	public List<String> getOnlyColName(){
		if(onlyColName != null){
			return onlyColName;
		}
		onlyColName = new ArrayList<String>();
		int csize = columnName.size();
		for(int i = 0; i < csize; ++i){
			String s = columnName.get(i);
			onlyColName.add(Lib.getAttributeName(s));
		}
		
		return onlyColName;
	}
	
	public List<String> getColWithTableName(){
		if(colWithTableName != null){
			return colWithTableName;
		}
		colWithTableName = new ArrayList<String>();
		for(String s: columnName){
			String tmp;
			if(! s.contains(".")){
				tmp = tableName + "." + Lib.getAttributeName(s);
			}
			else{
				tmp = s;
			}
			colWithTableName.add(tmp);
		}
		return colWithTableName;
	}

	public void fromList(List<Expr> exprs, Schema schema) {
		
		tableName = schema.tableName;
		if(exprs.size() != 0 && !Lib.trim(exprs.get(0).toString()).equals("*")){
			int size = exprs.size();
			for(int i = 0; i < size; ++i){
				Expr e = exprs.get(i);
				String colName = e.toString();
				Column col = schema.getColumn(colName);
				boolean isNull = col == null;
				if(isNull){
					col = new Column(colName, e.getType(schema));
					//System.out.println(colName + " " + e.getType());
				}
				//System.out.println(colName + " " + col.type);
				colMap.put(colName, col);
				columnName.add(colName);
			}
		}
		else {
			columnName.addAll(schema.columnName);
			colMap.putAll(schema.colMap);
		}
	}
	
	public int size(){
		return columnName.size();
	}
	
	public String toString(){
		String s = "Schema[" + tableName + "](";
		for(String colName: columnName){
			int t = colMap.get(colName).type;
			s = s + colName + " " + t + ",";
		}
		s = s + ")";
		return s;
	}
	
}
