package planner;



import org.antlr.runtime.ANTLRInputStream;  
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;  
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.Tree;  

import output.FileParser;


import parser.*;

/**
 * This class is where starts planning 
 * @author bd
 *
 */
public class Planner {
	/**
	 * The function is main function to begin
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void test(String[] args) throws Exception {  
		//FileParser.outputParser();
		//ANTLRInputStream input = new ANTLRInputStream(System.in);
		
		String query = "select a, count(b) as countB " + 
					"from A " +
					"where b in (select b from B where b > 0) " + 
					"group by a " +
					"order by countB";
;
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
        	System.exit(0);
        }
        catch (RecognitionException e)
        {
        	e.printStackTrace();
        	System.exit(1);
        } 
        
    }
	
}
