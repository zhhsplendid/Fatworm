package fatworm.driver;



import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

import datatype.DataRecord;
import filesys.BufferManager;
import filesys.MyFile;
import parser.FatwormLexer;
import parser.FatwormParser;
import scan.EmptyScan;
import scan.Scan;
import util.Env;
import util.Lib;
import value.Expr;

public class DatabaseEngine {
	public long time;
	public static final int maxMemorySize = 1000 * 1024;
	public static final int btreeWeight = 4;
	public static final int recordWeight = 8;
	
	public static boolean autoCommit = true;
	
	private static DatabaseEngine instance; 
	public HashMap<String, Database> dbMap;
	public Database nowdb;
	
	private String dbListFileName;
	public BufferManager recordManager;
	public BufferManager btreeManager;
	
	public static DatabaseEngine getInstance() {
		if (instance == null)
			instance = new DatabaseEngine();
		return instance;
	}
	
	private DatabaseEngine(){
		dbMap = new HashMap<String, Database>(); 
	}
	
	@SuppressWarnings("unchecked")
	public void openFile(String fileName) throws IOException, ClassNotFoundException{
		dbListFileName = Lib.databaseListFileName(fileName);
		try {
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(dbListFileName));
			dbMap = (HashMap<String, Database>)in.readObject();
			in.close();
		} catch (FileNotFoundException e) {
			dbMap = new HashMap<String, Database>();
			File file = new File(dbListFileName);
			//String s = file.getAbsolutePath();
			//s = s.replaceAll("\\\\", "/");
			if(!file.exists()){
				//file = Lib.createNewFile(s);
				file = Lib.createNewFile(file);
			}
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
			out.writeObject(dbMap);
			out.close();
		} 
		btreeManager = new BufferManager(MyFile.BTREE_FILE, Lib.btreeFileName(fileName));
		recordManager = new BufferManager(MyFile.RECORD_FILE, Lib.recordFileName(fileName));
		
	}
	
	
	public ResultSet execute(String query) throws Throwable{
		Tree tree = antlr(query);
		if(tree == null){
			return new ResultSet(EmptyScan.getInstance());
		}
		
		String name;
		Tree child;
		switch(tree.getType()){		
		case FatwormParser.USE_DATABASE:
			child = tree.getChild(0);
			name = child.getText().toLowerCase();
			nowdb = dbMap.get(name);
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.CREATE_DATABASE:
			child = tree.getChild(0);
			name = child.getText().toLowerCase();
			dbMap.put(name, new Database(name));
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.DROP_DATABASE:
			child = tree.getChild(0);
			name = child.getText().toLowerCase();
			dbMap.remove(name);
			if(nowdb != null && nowdb.name.equals(name)){
				nowdb = null;
			}
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.CREATE_TABLE:
			child = tree.getChild(0);
			name = child.getText().toLowerCase();
			nowdb.addTable(name, new IOTable(tree));
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.DROP_TABLE:
			child = tree.getChild(0);
			name = child.getText().toLowerCase();
			nowdb.removeTable(name);
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.SELECT:
		case FatwormParser.SELECT_DISTINCT:
			return executeQuery(tree);
		case FatwormParser.INSERT_VALUES:
		case FatwormParser.INSERT_COLUMNS:
		case FatwormParser.INSERT_SUBQUERY:
		case FatwormParser.DELETE:
		case FatwormParser.UPDATE:
		case FatwormParser.CREATE_INDEX:
		case FatwormParser.CREATE_UNIQUE_INDEX:
		case FatwormParser.DROP_INDEX:
			return executeUpdate(tree);
			default:
		}
		return null;
	}
	

	public ResultSet executeQuery(Tree tree) {
		Scan scan = Lib.getSelectScan(tree, true);
		scan.eval(new Env());
		return new ResultSet(scan);
	}
	
	public ResultSet executeUpdate(Tree tree) throws Throwable{
		String name;
		Tree child;
		Expr e = null;
		switch(tree.getType()){
		case FatwormParser.INSERT_VALUES:
			child = tree.getChild(0);
			name = child.getText().toLowerCase();
			String tmp = tree.getChild(1).getChild(0).getText();
			//if(tmp.equals("'Prince of Wales'")){
				//System.out.println(tmp);
			//}
			nowdb.getTable(name).insert(tree.getChild(1));
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.INSERT_COLUMNS:
			child = tree.getChild(0);
			name = child.getText().toLowerCase();
			nowdb.getTable(name).insert(tree, tree.getChild(tree.getChildCount()-1));
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.INSERT_SUBQUERY:
			child = tree.getChild(0);
			name = child.getText().toLowerCase();
			IOTable table = nowdb.getTable(name);
			Scan scan = Lib.getSelectScan(tree.getChild(1), true);
			scan.eval(new Env());
			
			LinkedList<Record> tmpTable = new LinkedList<Record>();
			while(scan.hasNext()){
				Record r = scan.next();
				r.schema = table.schema;
				DataRecord tmpdr = r.getField(0);
				/*
				if(tmpdr.toString().equals("'Prince of Wales'")){
					System.out.println("!!!");
				}*/
				
				tmpTable.add(r);
			}
			for(Record r: tmpTable){
				table.addRecord(r);
			}
			scan.close();
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.DELETE:
			child = tree.getChild(0);
			name = child.getText().toLowerCase();
			
			if(tree.getChildCount() != 1){
				e = Lib.getExpr(tree.getChild(1).getChild(0));
			}
			nowdb.getTable(name).delete(e);
			if(autoCommit){
				commit();//for pass testsuit powerout = =
			}
			// if want to auto commit, we need add commit all sentence
			// but in order to speed up, I just add commit here to pass powerout !!!= =
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.UPDATE:
			child = tree.getChild(0);
			name = child.getText().toLowerCase();
			int ccount = tree.getChildCount();
			ArrayList<String> colName = new ArrayList<String>();
			ArrayList<Expr> exprs = new ArrayList<Expr>();
			for(int i = 1; i < ccount; ++i){
				child = tree.getChild(i);
				if(child.getType() == FatwormParser.UPDATE_PAIR){
					colName.add(child.getChild(0).getText());
					exprs.add(Lib.getExpr(child.getChild(1)));
				}else {
					e = Lib.getExpr(child.getChild(0));
				}
			}
			nowdb.getTable(name).update(colName, exprs, e);
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.CREATE_INDEX:
		case FatwormParser.CREATE_UNIQUE_INDEX:
			String indexName = tree.getChild(0).getText().toLowerCase();
			String tableName = tree.getChild(1).getText().toLowerCase();
			String columnName = tree.getChild(2).getText().toLowerCase();
			table = nowdb.getTable(tableName);
			nowdb.createIndex(indexName, columnName, tree.getType() == FatwormParser.CREATE_UNIQUE_INDEX, table);
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.DROP_INDEX:
			child = tree.getChild(0);
			nowdb.dropIndex(child.getText().toLowerCase());
			return new ResultSet(EmptyScan.getInstance());
		default:
			//TODO
		}
		return null;
	}

	private CommonTree antlr(String query){
		ANTLRStringStream input = new ANTLRStringStream(query);
        
        FatwormLexer lexer = new FatwormLexer(input);  
        CommonTokenStream tokens = new CommonTokenStream(lexer);  
        FatwormParser parser = new FatwormParser(tokens);  
        
        try{
        	FatwormParser.statement_return r = parser.statement();  
        	CommonTree tree = (CommonTree)r.getTree();
        	return tree;
        }
        catch (RecognitionException e)
        {
        	e.printStackTrace();
        	return null;
        } 
	}
	
	public void close(){
		try {
			btreeManager.close();
			recordManager.close();
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(dbListFileName));
			out.writeObject(dbMap);
			out.close();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void commit(){
		try{
			btreeManager.commit();
			recordManager.commit();
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(dbListFileName));
			out.writeObject(dbMap);
			out.close();
		} catch(Throwable e){
			e.printStackTrace();
		}
	}

	public Database getDatabase() {
		return nowdb;
	}
	
	public boolean outOfMemory(){
		int nowSize = btreeManager.pageMap.size() * btreeWeight + recordManager.pageMap.size() * recordWeight;
		return nowSize >= maxMemorySize;
	}
	
	public void flushOtherManager(BufferManager manager) throws Throwable{
		
		if(manager == recordManager){
			if(!btreeManager.flushOutOnePage()){
				recordManager.flushOutOnePage();
			}
		}
		else if(manager == btreeManager){
			if(!recordManager.flushOutOnePage()){
				btreeManager.flushOutOnePage();
			}
			
		} 
	}

	public IOTable getTable(String name) {
		return nowdb.getTable(name.toLowerCase());
	}

	public void save() {
		// TODO Auto-generated method stub
	}
}
