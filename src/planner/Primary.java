package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import parser.FatwormParser;

public class Primary extends Expr{
	Tree tree;
	
	public Primary(Tree t){
		tree = t;
	}
	
	public void execute(){
		int type = tree.getType();
		Subquery subquery;
		Value value;
		switch(type){
		case FatwormParser.T__114:
		case FatwormParser.T__115:
		case FatwormParser.T__116:
		case FatwormParser.T__117:
		case FatwormParser.T__118:
		case FatwormParser.T__119:
			TreeFormat.println(tree.getText());
			TreeFormat.up();
			Value v0 = new Value(tree.getChild(0));
			v0.execute();
			Value v1 = new Value(tree.getChild(1));
			v1.execute();
			TreeFormat.down();
			break;
		case FatwormParser.EXISTS:
			TreeFormat.println("EXISTS");
			TreeFormat.up();
			subquery = new Subquery(tree.getChild(0));
			subquery.execute();
			TreeFormat.down();
			break;
		case FatwormParser.NOT_EXISTS:
			TreeFormat.println("NOT_EXISTS");
			TreeFormat.up();
			subquery = new Subquery(tree.getChild(0));
			subquery.execute();
			TreeFormat.down();
			break;
		case FatwormParser.IN:
			TreeFormat.println("IN");
			TreeFormat.up();
			value = new Value(tree.getChild(0));
			value.execute();
			subquery = new Subquery(tree.getChild(1));
			subquery.execute();
			TreeFormat.down();
			break;
		case FatwormParser.ANY:
			TreeFormat.println(tree.getChild(1).getText()+"ANY");
			TreeFormat.up();
			value = new Value(tree.getChild(0));
			value.execute();
			subquery = new Subquery(tree.getChild(2));
			subquery.execute();
			TreeFormat.down();
			break;
		case FatwormParser.ALL:
			TreeFormat.println(tree.getChild(1).getText()+"ALL");
			TreeFormat.up();
			value = new Value(tree.getChild(0));
			value.execute();
			subquery = new Subquery(tree.getChild(2));
			subquery.execute();
			TreeFormat.down();
			break;
		default:
			BoolExpr boolexpr = new BoolExpr(tree);
			boolexpr.execute();
		}
	}
}
