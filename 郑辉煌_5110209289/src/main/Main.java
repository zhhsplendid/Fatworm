package main;

import java.util.Scanner;

import output.Output;

import fatworm.driver.*;

import util.Lib;

public class Main {
	static final String inputFile = "input.txt";
	static final public boolean debugMode = true;
	static final public boolean indexMode = true;
	
	public static void test(String[] args){
		Scanner in = Lib.getScanner(inputFile);
		DatabaseEngine db = DatabaseEngine.getInstance();
		
		try{
			db.openFile("/db/fatworm");
			while(in.hasNextLine()){
				String query = in.nextLine();
				if(!Lib.isNotComment(query)){
					continue;
				}
				ResultSet rs = db.execute(query);
				while(rs.next()){
					Output.filePrintln(rs.getObject(0).toString());
				}
			}
		} catch(Exception e){
			e.printStackTrace();
		}
		
		in.close();
		db.close();
	}
}
