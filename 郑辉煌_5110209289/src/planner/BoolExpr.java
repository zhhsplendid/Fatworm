package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import parser.FatwormParser;

public class BoolExpr extends Expr{
	Tree tree;
	
	public BoolExpr(Tree t){
		tree = t;
	}
	
	//first AND, second OR
	public void execute(){
		logicalOr(tree);
	}
	
	private void logicalOr(Tree t){
		int type = t.getType();
		switch(type){
		case FatwormParser.OR:
			TreeFormat.println("OR");
			TreeFormat.up();
			logicalAND(t.getChild(0));
			logicalAND(t.getChild(1));
			TreeFormat.down();
			break;
		default:
			logicalAND(t);
		}
	}
	
	private void logicalAND(Tree t){
		int type = t.getType();
		switch(type){
		case FatwormParser.AND:
			TreeFormat.println("AND");
			TreeFormat.up();
			Primary p0 = new Primary(t.getChild(0));
			p0.execute();
			Primary p1 = new Primary(t.getChild(1));
			p1.execute();
			TreeFormat.down();
			break;
		default:
			Primary p = new Primary(t);
			p.execute();
		}
	}

}
