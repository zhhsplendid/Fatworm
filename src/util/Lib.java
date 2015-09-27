package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.antlr.runtime.tree.BaseTree;
import org.antlr.runtime.tree.Tree;

import output.Debug;
import parser.FatwormParser;
import scan.Scan;
import scan.ScanPlan;
import value.*;
import datatype.*;
import datatype.Float;
import fatworm.driver.Column;


/**
 * this class includes tools which used among the project
 * @author bd
 *
 */
public class Lib {
	public static long time;
	/** 
	 * called when main need input query
	 * @return scanner from System.in
	 */
	public static Scanner getScanner(){
		Scanner scanner = new Scanner(System.in);
		return scanner;
	}
	
	/** 
	 * called when main need input query
	 * @return scanner from file
	 */
	public static Scanner getScanner(String path){
		Scanner scanner = null;
		try{
			File file = new File(path);
			FileInputStream fs = new FileInputStream(file);
			scanner = new Scanner(fs);
		}catch(IOException e){
			e.printStackTrace();
			System.exit(1);
		}
		return scanner;
	}
	
	/**
	 * in fatworm, TAs have some sentence as comment
	 * @param s
	 * @return
	 */
	public static boolean isNotComment(String s){
		return s.length() > 1 && (!s.substring(0, 1).equals("@"));
	}
	
	public static String getAttributeName(String s){
		int pos = s.indexOf('.');
		if(pos != -1){
			return s.substring(pos+1);
		}
		return s;
	}
	/*
	public static DataRecord getDataRecord(Column column, Tree tree) {
		if(tree == null || tree.getText().equalsIgnoreCase("default") 
				|| tree.getText().equalsIgnoreCase("null")){
			if(column.hasDefault()){
				return column.getDefault();
			} else if(column.autoIncrement()){
				return new Int(column.getAutoIndex());
			} else if(column.type == java.sql.Types.DATE){
				return new Date(new java.sql.Timestamp((new GregorianCalendar()).getTimeInMillis()));
			} else if(column.type == java.sql.Types.TIMESTAMP){
				return new Timestamp(new java.sql.Timestamp((new GregorianCalendar()).getTimeInMillis()));
			}
			Debug.warn("type warn");
		}
		return DataRecord.fromString(column.type, getExpr(tree).valueExpr(new Env()).toString());
	}
	*/
	
	public static Scan getSelectScan(Tree tree, boolean optimize){
		//TODO
		return ScanPlan.getScan(tree, optimize);
	}
	
	public static Expr getExpr(Tree tree){
		switch(tree.getType()){
		case FatwormParser.SELECT:
		case FatwormParser.SELECT_DISTINCT:
			return new QueryExpr(getSelectScan(tree, false));
		case FatwormParser.TRUE:
			return new BoolExpr(true);
		case FatwormParser.FALSE:
			return new BoolExpr(false);
		case FatwormParser.AND:
			return new BinaryExpr(getExpr(tree.getChild(0)), getExpr(tree.getChild(1)), BinaryOp.AND);
		case FatwormParser.OR:
			return new BinaryExpr(getExpr(tree.getChild(0)), getExpr(tree.getChild(1)), BinaryOp.OR);
			
		case FatwormParser.INTEGER_LITERAL:
			String numberStr = tree.getText();
			try {
				Integer number = Integer.parseInt(numberStr);
				return new IntExpr(number);
			} catch (NumberFormatException e) {
				return new IntExpr(new BigInteger(numberStr));
			}
		case FatwormParser.FLOAT_LITERAL:
			return new FloatExpr(java.lang.Float.parseFloat(tree.getText()));
		case FatwormParser.STRING_LITERAL:
			return new StringExpr(getInnerQuotation(tree.getText()));
		
		
		case FatwormParser.EXISTS:
			return new ExistExpr(getSelectScan(tree.getChild(0), false), false);
		case FatwormParser.NOT_EXISTS:
			return new ExistExpr(getSelectScan(tree.getChild(0), false), true);
		case FatwormParser.IN:
			return new InExpr(getSelectScan(tree.getChild(1), false), getExpr(tree.getChild(0)), false);
		case FatwormParser.SUM:
		case FatwormParser.AVG:
		case FatwormParser.COUNT:
		case FatwormParser.MAX:
		case FatwormParser.MIN:
			String funcAttr = getAttributeName(tree.getChild(0));
			return new FunctionExpr(tree.getType(), funcAttr);
	
		case FatwormParser.ANY:
		case FatwormParser.ALL:
			Scan scan = getSelectScan(tree.getChild(2), false);
			Expr first = getExpr(tree.getChild(0));
			BinaryOp aop = getBinaryOp(tree.getChild(1).getText());
			boolean isAll = tree.getType() == FatwormParser.ALL;
			return new AllAndAnyExpr(scan, first, aop, isAll);
		default:
			String ops = tree.getText();
			BinaryOp op = getBinaryOp(ops);
			if(op != null && tree.getChildCount() == 2){
				Expr left = getExpr(tree.getChild(0));
				Expr right = getExpr(tree.getChild(1));
				/*
				if(op == BinaryOp.MULT){
					Int times = null;
					IdExpr id = null;
					if(left.isConst && left.getType() == java.sql.Types.INTEGER && right instanceof IdExpr){
						times = (Int)left.value;
						id = (IdExpr)right;
						return new IdTimesExpr(id, times.value);
					}
					if(right.isConst && left instanceof IdExpr && right.getType() == java.sql.Types.INTEGER){
						times = (Int)right.value;
						id = (IdExpr)left;
						return new IdTimesExpr(id, times.value);
					}
					
				}*/
				return new BinaryExpr(left, right, op);
			}
			else if(op != null){ //but tree child count == 1 which implies numbers like -1
				Tree child = tree.getChild(0);
				Expr childExpr = getExpr(child);
				DataRecord dtmp = null;
				if(childExpr instanceof IntExpr){
					dtmp = ((IntExpr)childExpr).data;
					if(dtmp instanceof Decimal){
						Decimal deci = (Decimal)dtmp;
						return new IntExpr(deci.value.negate().toBigIntegerExact());
					}
					else{
						Int inte = (Int)dtmp;
						return new IntExpr(-inte.value);
					}
				}
				else if(childExpr instanceof FloatExpr){
					dtmp = ((FloatExpr) childExpr).data;
					return new FloatExpr(- ((Float)dtmp).value);
				}
				else if(childExpr.isConst && childExpr.value instanceof Int){
					dtmp = childExpr.value;
					return new IntExpr(- ((Int)dtmp).value);
				}
				else{
					IntExpr left = new IntExpr(BigInteger.valueOf(0));
					return new BinaryExpr(left, childExpr, op);
				}
				
			}
			else{
				String attr = getAttributeName(tree);
				return new IdExpr(attr);
			}
		}
	}
	
	//
	private static BinaryOp getBinaryOp(String s) {
		if(s.equalsIgnoreCase("and")){
			return BinaryOp.AND;
		}
		else if(s.equalsIgnoreCase("or")){
			return BinaryOp.OR;
		}
		else if(s.equals("=")){
			return BinaryOp.EQUAL;
		}
		else if(s.equals("<>")){
			return BinaryOp.NOT_EQUAL;
		}
		else if(s.equals(">")){
			return BinaryOp.GREATER;
		}
		else if(s.equals(">=")){
			return BinaryOp.GREATER_EQ;
		}
		else if(s.equals("<")){
			return BinaryOp.LESS;
		}
		else if(s.equals("<=")){
			return BinaryOp.LESS_EQ;
		}
		else if(s.equals("+")){
			return BinaryOp.PLUS;
		}
		else if(s.equals("-")){
			return BinaryOp.MINUS;
		}
		else if(s.equals("*")){
			return BinaryOp.MULT;
		}
		else if(s.equals("/")){
			return BinaryOp.DIV;
		}
		else if(s.equals("%")){
			return BinaryOp.MOD;
		}
		//Debug.err("illegal binary operation");
		return null;
	}

	public static String getInnerQuotation(String s){
		return s.substring(1, s.length() - 1);
	}
	
	public static String getAttributeName(Tree tree) {
		if(tree.getText().equals(".")){
			return tree.getChild(0).getText() + "." + tree.getChild(1).getText();
		}
		return tree.getText();
	}

	public static boolean toBoolean(String s) {
		s = s.trim();
		if(s.equalsIgnoreCase("true") || s.equals("1")){
			return true;
		}
		return false;
	}
	
	public static boolean toBoolean(DataRecord d) {
		return toBoolean(d.toString());
	}

	public static String trim(String s) {
		String ans = s.trim();
		if(ans.startsWith("'") && ans.endsWith("'")){
			ans = ans.substring(1, s.length() - 1);
		}
		else if(ans.startsWith("\"") && ans.endsWith("\"")){
			ans = ans.substring(1, s.length() - 1);
		}
		return ans;
	}

	public static String primaryKeyIndexName(String name) {
		return name + ".PrimaryIndex"; 
	}
	
	public static String databaseListFileName(String name){
		return name + ".databaseList";
	}
	
	public static String btreeFileName(String name){
		return name + ".btree";
	}
	
	public static String recordFileName(String name){
		return name + ".record";
	}

	public static byte[] intToBytes(int value) {
		//byte[] tmp = new byte[Integer.SIZE / Byte.SIZE];
		byte[] tmp = new byte[4];
		ByteBuffer buff = ByteBuffer.wrap(tmp);
		buff.putInt(value);
		return buff.array();
	}

	public static byte[] floatToBytes(float value) {
		//byte[] tmp = new byte[java.lang.Float.SIZE / Byte.SIZE];
		byte[] tmp = new byte[4];
		ByteBuffer buff = ByteBuffer.wrap(tmp);
		buff.putFloat(value);
		return buff.array();
	}

	public static byte[] longToByte(long value) {
		//byte[] tmp = new byte[java.lang.Long.SIZE / Byte.SIZE];
		byte[] tmp = new byte[8];
		ByteBuffer buff = ByteBuffer.wrap(tmp);
		buff.putLong(value);
		return buff.array();
	}

	public static <T> boolean cmp(BinaryOp op, Comparable<T> a, T b) {
		switch(op){
		case EQUAL: return a.compareTo(b) == 0;
		case NOT_EQUAL: return a.compareTo(b) != 0;
		
		case GREATER: return a.compareTo(b) > 0;
		case GREATER_EQ: return a.compareTo(b) >= 0;
		
		case LESS: return a.compareTo(b) < 0;
		case LESS_EQ: return a.compareTo(b) <= 0;
		
		default:
			Debug.err("this op can't compare");
		}
		
		return false;
	}
	
	public static boolean cmpString(BinaryOp op, String a, String b){
		switch(op){
		case EQUAL: return a.compareToIgnoreCase(b) == 0;
		case NOT_EQUAL: return a.compareToIgnoreCase(b) != 0;
		
		case GREATER: return a.compareToIgnoreCase(b) > 0;
		case GREATER_EQ: return a.compareToIgnoreCase(b) >= 0;
		
		case LESS: return a.compareToIgnoreCase(b) < 0;
		case LESS_EQ: return a.compareToIgnoreCase(b) <= 0;
		
		default:
			Debug.err("this op can't compare");
		}
		
		return false;
	}
	/*
	public static <K, V> String mapToString(HashMap<K, V> map) {
		StringBuffer ans = new StringBuffer();
		ans.append("{");
		
		for(Map.Entry<K, V> element: map.entrySet()){
			K key = element.getKey();
			V value = element.getValue();
			ans.append(key==null?"null": key.toString());
			ans.append("=");
			ans.append(value==null?"null": value.toString());
			ans.append(", ");
		}
		ans.append("}");
		return ans.toString();
	}*/

	public static boolean isAnd(Expr e){
		if(e instanceof BinaryExpr){
			return ((BinaryExpr)e).op == BinaryOp.AND;
		}
		return false;
	}
	
	public static void collectCondition(Collection<Expr> c, Expr e){
		if(!isAnd(e)){
			c.add(e);
		}
		else{
			collectCondition(c, ((BinaryExpr)e).left);
			collectCondition(c, ((BinaryExpr)e).right);;
		}
	}

	public static Integer max(Integer a, Integer b) {
		return a > b ? a : b ;
	}

	public static boolean isSymmetricalOp(BinaryOp op) {
		switch(op){
		case EQUAL:
		case NOT_EQUAL:
		case OR:
		case AND:
		case PLUS:
		case MULT:
			return true;
		default:
			return false; 
		}
	}

	public static void addAllCol(Collection<String> to, Collection<String> from) {
		for(String s1: from){
			boolean flag = true;
			for(String s2: to){
				if(s2.equalsIgnoreCase(s1)){
					flag = false;
					break;
				}
			}
			if(flag){
				to.add(s1);
			}
		}
	}
	
	public static boolean isNull(DataRecord d){
		return d == null || d.type == java.sql.Types.NULL;
	}
	
	public static boolean endsWith(String s1, String s2){
		return s1.toLowerCase().endsWith(s2.toLowerCase()) 
				|| s2.toLowerCase().endsWith(s1.toLowerCase());
	}
	
	public static void removeRepeat(Collection<String> to,
			Collection<String> from) {
		HashSet<String> tmp = new HashSet<String>();
		for(String s1: from){
			for(String s2: to){
				if(endsWith(s1, s2)){
					tmp.add(s1);
				}
			}
		}
		to.removeAll(tmp);
	}

	/**
	 * this function return whether "sub" is subset of "set" by means of attribute name
	 * @param requestCol
	 * @param col
	 * @return
	 */
	public static boolean isSubset(List<String> sub, List<String> set) {
		for(String x: sub){
			boolean found = false;
			for(String y: set){
				String s1 = x.toLowerCase();
				String s2 = y.toLowerCase();
				if(s2.endsWith(s1)){
					if(s2.indexOf(s1) == 0 || s2.indexOf(s1) == s2.indexOf(".") + 1){
						found = true;
						break;
					}
				}
			}
			if(! found){
				return false;
			}
		}
		return true;
	}

	public static File createNewFile(String path) throws IOException {
		int pos = path.lastIndexOf("/");
		if(pos == -1){
			File file = new File(path);
			if(! file.exists()){
				file.createNewFile();
			}
			return file;
		}
		else{
			File dir = new File(path.substring(0, pos));
			if(!dir.exists()){
				dir.mkdirs();
			}
			File file = new File(dir, path.substring(pos + 1, path.length()));
			if(!file.exists()){
				file.createNewFile();
			}
			return file;
		}
	}

	

	public static <T> boolean collectionEquals(List<T> c1,
			List<T> c2) {
		int size1 = c1.size();
		
		if(size1 != c2.size()){
			return false;
		}
		
		for(int i = 0; i < size1; ++i){
			if(!c1.get(i).equals(c2.get(i))){
				return false;
			}
		}
		return true;
	}

	public static<T> long collectionHashCode(List<T> cols) {
		long hash = 0;
		for(T o: cols){
			hash ^= o.hashCode();
		}
		return hash;
	}

	public static void addAll(Collection<FunctionExpr> to, Collection<FunctionExpr> from) {
		to.addAll(from);
	}

	public static boolean colNameEqual(String col, String name) {
		return name.equalsIgnoreCase(col) || Lib.getAttributeName(col).equalsIgnoreCase(name)
				|| col.equalsIgnoreCase(Lib.getAttributeName(name));
	}

	public static File createNewFile(File file) throws IOException {
		File parent = file.getParentFile();
		if(parent != null && !parent.exists()){
			parent.mkdirs();
		}
		
		if(!file.exists()){
			file.createNewFile();
		}
		return file;
	}
	
}
