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

import filesys.BufferManager;
import filesys.MyFile;


import parser.FatwormLexer;
import parser.FatwormParser;
import scan.EmptyScan;
import scan.Scan;
import util.Env;
import util.Lib;

public class DatabaseEngine {
	public static final int maxMemorySize = 1000 * 1024;
	public static final int btreeWeight = 4;
	public static final int recordWeight = 8;
	
	
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
			if(!file.exists()){
				file = Lib.createNewFile(dbListFileName);
			}
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
			out.writeObject(dbMap);
			out.close();
			
		} 
		recordManager = new BufferManager(Lib.recordFileName(fileName), MyFile.RECORD_FILE);
		btreeManager = new BufferManager(Lib.btreeFileName(fileName), MyFile.BTREE_FILE);
	}
	
	
	public ResultSet execute(String query){
		Tree tree = antlr(query);
		if(tree == null){
			return new ResultSet(EmptyScan.getInstance());
		}
		
		String name;
		Scan scan;
		switch(tree.getType()){
		
		case FatwormParser.USE_DATABASE:
			name = tree.getChild(0).getText().toLowerCase();
			nowdb = dbMap.get(name);
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.CREATE_DATABASE:
			name = tree.getChild(0).getText().toLowerCase();
			dbMap.put(name, new Database(name));
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.DROP_DATABASE:
			name = tree.getChild(0).getText().toLowerCase();
			dbMap.remove(name);
			if(nowdb != null && nowdb.name.equals(name)){
				nowdb = null;
			}
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.CREATE_TABLE:
			name = tree.getChild(0).getText().toLowerCase();
			nowdb.addTable(name, new IOTable(tree));
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.DROP_TABLE:
			name = tree.getChild(0).getText().toLowerCase();
			nowdb.removeTable(name);
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.INSERT_VALUES:
			name = tree.getChild(0).getText().toLowerCase();
			nowdb.getTable(name).insert(tree.getChild(1));
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.SELECT:
		case FatwormParser.SELECT_DISTINCT:
			scan = Lib.getSelectScan(tree, true);
			scan.eval(new Env());
			return new ResultSet(scan);
		case FatwormParser.INSERT_COLUMNS:
			name = tree.getChild(0).getText().toLowerCase();
			nowdb.getTable(name).insert(tree, tree.getChild(tree.getChildCount()-1));
			return new ResultSet(EmptyScan.getInstance());
		case FatwormParser.INSERT_SUBQUERY:
			//TODO
		case FatwormParser.DELETE:
			//TODO
		case FatwormParser.UPDATE:
			//TODO
		case FatwormParser.CREATE_INDEX:
			//TODO
		case FatwormParser.CREATE_UNIQUE_INDEX:
			//TODO
		case FatwormParser.DROP_INDEX:
			default:
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
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(dbListFileName));
			out.writeObject(dbMap);
			out.close();
			recordManager.close();
			btreeManager.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	public Database getDatabase() {
		return nowdb;
	}
	
	public boolean outOfMemory(){
		return btreeManager.pageMap.size() * btreeWeight+ recordManager.pageMap.size() * recordWeight >= maxMemorySize;
	}
	
	public void flushOtherManager(BufferManager manager) throws Throwable{
		if(manager == btreeManager){
			if(!recordManager.flushOutOnePage()){
				btreeManager.flushOutOnePage();
			}
			
		} else if(manager == recordManager){
			if(!btreeManager.flushOutOnePage()){
				recordManager.flushOutOnePage();
			}
		}
	}

	public IOTable getTable(String name) {
		return nowdb.getTable(name.toLowerCase());
	}
}
