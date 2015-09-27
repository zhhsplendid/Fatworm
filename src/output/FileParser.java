package output;

import java.io.*;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.Tree;

import planner.Statement;
import parser.FatwormLexer;
import parser.FatwormParser;

public class FileParser {
	
	static String directory = "../testcases_revised/basic/arno/";
	static String name = "query.fwt";
	
	public static String getFileString(String path) throws IOException{
		File file = new File(path);
		if(!file.exists()||file.isDirectory()){
            throw new FileNotFoundException();
		}
		
		BufferedReader buffer = new BufferedReader(new FileReader(file));
        String temp = null;
        StringBuffer sb = new StringBuffer();
        temp = buffer.readLine();
        while(temp != null){
        	if(temp.length() < 1 || !temp.substring(0, 1).equals("@")){
        		sb.append(temp + " ");
        	}
            temp = buffer.readLine();
        }
        return sb.toString();
	} 
	
	public static String[] splitInput(String context){
		String breakSign = ";";
		return context.split(breakSign);		
	}
	
	public static void outputParser() throws IOException{
		String fileString = getFileString(directory + name);
		String[] querys = splitInput(fileString);
		
		for(int i = 0; i < querys.length - 1; ++i){
			Output.println(name + " " + i + " query:");
			
			String query = querys[i];
	        ANTLRStringStream input = new ANTLRStringStream(query);
	        
	        FatwormLexer lexer = new FatwormLexer(input);  
	        CommonTokenStream tokens = new CommonTokenStream(lexer);  
	        FatwormParser parser = new FatwormParser(tokens);  
	        try{
	        	FatwormParser.statement_return r = parser.statement();  
	        	Tree tree = (Tree) r.getTree();
	      
	        	Statement stat = new Statement(tree);
	        	stat.execute();
	        	//System.out.println(tree.toStringTree()); 
	        }
	        catch (RecognitionException e)
	        {
	        	e.printStackTrace();
	        	System.exit(1);
	        } 
	        
	        Output.println("#########################");
		}
	}
}
