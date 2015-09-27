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
		for(int i = 1; i < tree.getChildCount(); ++i){
			Tree child = tree.getChild(i);
			String colName = child.getChild(0).getText();
			switch(child.getType()){
			case FatwormParser.CREATE_DEFINITION:
				columnName.add(colName);
				Tree dataType = child.getChild(1);
				Column col = null;
				switch(dataType.getType()){
				case FatwormParser.BOOLEAN:
					col = new Column(colName, java.sql.Types.BOOLEAN);
					break;
				case FatwormParser.CHAR:
					col = new Column(colName, java.sql.Types.CHAR);
					col.datalength1 = Integer.parseInt(dataType.getChild(0).getText());
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
				case FatwormParser.FLOAT:
					col = new Column(colName, java.sql.Types.FLOAT);
					break;
				case FatwormParser.INT:
					col = new Column(colName, java.sql.Types.INTEGER);
					break;
				case FatwormParser.TIMESTAMP:
					col = new Column(colName, java.sql.Types.TIMESTAMP);
					break;
				case FatwormParser.VARCHAR:
					col = new Column(colName, java.sql.Types.VARCHAR);
					col.datalength1 = Integer.parseInt(dataType.getChild(0).getText());
					break;
					default:
						Debug.err("Schema undefined dataType");
				}
				colMap.put(colName, col);
				for(int j = 2; j < child.getChildCount(); ++j){
					Tree option = child.getChild(j);
					switch(option.getType()){
					case FatwormParser.AUTO_INCREMENT:
						col.autoIncrement = true;
						break;
					case FatwormParser.DEFAULT:
						col.defaultValue = DataRecord.getObject(col.getDataRecord(option.getChild(0)));
						break;
					case FatwormParser.NULL:
						col.notNull = option.getChildCount() != 0;
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
		for(int i = 1; i < tree.getChildCount(); ++i){
			Tree child = tree.getChild(i);
			String colName = child.getChild(0).getText();
			switch(child.getType()){
			case FatwormParser.CREATE_DEFINITION:
				break;
			case FatwormParser.PRIMARY_KEY:
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
		if(colMap.containsKey(name)){
			return colMap.get(name);
		}
		
		String attr = Lib.getAttributeName(name);
		if(colMap.containsKey(attr)){
			return colMap.get(attr);
		}
		
		String col = tableName + "." + attr;
		if(colMap.containsKey(col)){
			return colMap.get(col);
		}
		// FIXME hack for table name mismatch due to Join
		int i = indexOf(name);
		if(i < 0)return null;
		String col2 = columnName.get(i);
		return colMap.get(col2);
		//return null;
	}
	
	public int indexOf(String s) {
		if(s == null){
			return -1;
		}
		
		for(int i = 0; i < columnName.size(); ++i){
			String y = columnName.get(i);
			if(s.equalsIgnoreCase(y) || s.equalsIgnoreCase(this.tableName + "." + y))
				return i;
		}
		for(int i = 0; i < columnName.size(); ++i){
			String y = columnName.get(i);
			if(!y.contains("."))continue;
			// FIXME hack for table name mismatch due to Join
			if(Lib.getAttributeName(s).equalsIgnoreCase(Lib.getAttributeName(y)))
				return i;
		}
		return -1;
	}
	
	public int indexOfStrictString(String col) {
		for(int i = 0; i < columnName.size(); ++i){
			String name = columnName.get(i);
			if(name.equalsIgnoreCase(col) || Lib.getAttributeName(col).equalsIgnoreCase(name)
					|| col.equalsIgnoreCase(Lib.getAttributeName(name))){
				return i;
			}
		}
		return -1;
	}
	
	
	/**for temporary use */
	private ArrayList<String> onlyColName;
	private ArrayList<String> colWithTableName;


	
	public List<String> getOnlyColName(){
		if(onlyColName == null){
			onlyColName = new ArrayList<String>();
			for(String s: columnName){
				onlyColName.add(Lib.getAttributeName(s));
			}
		}
		return onlyColName;
	}
	
	public List<String> getColWithTableName(){
		if(colWithTableName == null){
			colWithTableName = new ArrayList<String>();
			for(String s: columnName){
				String tmp;
				if(s.contains(".")){
					tmp = s;
				}
				else{
					tmp = tableName + "." + Lib.getAttributeName(s);
				}
				colWithTableName.add(tmp);
			}
		}
		return colWithTableName;
	}

	public void fromList(List<Expr> exprs, Schema schema) {
		
		tableName = schema.tableName;
		if(exprs.size() == 0 || Lib.trim(exprs.get(0).toString()).equals("*")){
			columnName.addAll(schema.columnName);
			colMap.putAll(schema.colMap);
		}
		else {
			for(int i = 0; i < exprs.size(); ++i){
				Expr e = exprs.get(i);
				String colName = e.toString();
				Column col = schema.getColumn(colName);
				if(col == null){
					col = new Column(colName, e.getType(schema));
					//System.out.println(colName + " " + e.getType());
				}
				//System.out.println(colName + " " + col.type);
				colMap.put(colName, col);
				columnName.add(colName);
			}
		}
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
