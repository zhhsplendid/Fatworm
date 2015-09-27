package planner;

import org.antlr.runtime.tree.Tree;

import parser.FatwormParser;

import output.TreeFormat;

public class ConstValue extends Expr{
	
	Tree tree;
	int type;
	
	public ConstValue(Tree t){
		tree = t;
		type = t.getType();
	}
	
	public void execute(){
		switch(type){
		case FatwormParser.DEFAULT:
			TreeFormat.println("DEFAULT");
			break;
		case FatwormParser.NULL:
			TreeFormat.println("NULL");
			break;
		case FatwormParser.TRUE:
			TreeFormat.println("true");
			break;
		case FatwormParser.FALSE:
			TreeFormat.println("false");
			break;
		case FatwormParser.STRING_LITERAL:
			String string_literal = tree.getText();
			
			TreeFormat.println("STRING_LITERAL");
			TreeFormat.up();
			TreeFormat.println(string_literal);
			TreeFormat.down();
			break;
		case FatwormParser.INTEGER_LITERAL:
			IntegerLiteral integer_literal = new IntegerLiteral(tree);
			integer_literal.execute();
			/*
			String int_string = tree.getText();
			Integer integer_literal = new Integer(int_string);
			
			TreeFormat.println("INTEGER_LITERAL");
			TreeFormat.up();
			TreeFormat.println(int_string);
			TreeFormat.down();
			*/
			break;
		case FatwormParser.FLOAT_LITERAL:
			String float_string = tree.getText();
			double float_literal = Double.valueOf(float_string);
			
			TreeFormat.println("FLOAT_LITERAL");
			TreeFormat.up();
			TreeFormat.println(float_string);
			TreeFormat.down();
			break;
		}
	}
}
