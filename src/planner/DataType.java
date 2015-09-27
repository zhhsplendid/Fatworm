package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import parser.FatwormParser;

/**
 * This class is abstract for date_type 
 * @author bd
 *
 */
public class DataType {
	Tree tree;
	int type;
	public DataType(Tree t){
		tree = t;
		type = tree.getType();
	}
	
	public void execute(){
		switch(type){
		case FatwormParser.INT:
			TreeFormat.println("int");
			break;
		case FatwormParser.FLOAT:
			TreeFormat.println("float");
			break;
		case FatwormParser.DATETIME:
			TreeFormat.println("DATETIME");
			break;
		case FatwormParser.BOOLEAN:
			TreeFormat.println("BOOLEAN");
			break;
		case FatwormParser.TIMESTAMP:
			TreeFormat.println("TIMESTAMP");
			break;
		case FatwormParser.CHAR:
			TreeFormat.println("CHAR");
			TreeFormat.up();
			IntegerLiteral char_int = new IntegerLiteral(tree.getChild(0));
			char_int.execute();
			TreeFormat.down();
			break;
		case FatwormParser.VARCHAR:
			TreeFormat.println("VARCHAR");
			TreeFormat.up();
			IntegerLiteral varchar_int = new IntegerLiteral(tree.getChild(0));
			varchar_int.execute();
			TreeFormat.down();
			break;
		case FatwormParser.DECIMAL:
			TreeFormat.println("DECIMAL");
			TreeFormat.up();
			IntegerLiteral int1 = new IntegerLiteral(tree.getChild(0)); 
			int1.execute();
			
			IntegerLiteral int2 = new IntegerLiteral(tree.getChild(1));
			int2.execute();
			TreeFormat.down();
			break;
		}
	}
}
