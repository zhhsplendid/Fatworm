package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import parser.FatwormParser;

public class SelectExpr {
	Tree tree;

	public SelectExpr(Tree t){
		tree = t;
	}
	
	public void execute(){
		int type = tree.getType();
		switch(type){
		case FatwormParser.T__108:
			TreeFormat.println("*");
			break;
		case FatwormParser.AS:
			TreeFormat.println("AS");
			TreeFormat.up();
			Value value = new Value(tree.getChild(0));
			value.execute();
			Alias alias = new Alias(tree.getChild(1));
			alias.execute();
			TreeFormat.down();
			break;
		default:
			Value v = new Value(tree);
			v.execute();
		}
	}
}
