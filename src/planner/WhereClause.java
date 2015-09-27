package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class WhereClause {
	Tree tree;
	
	public WhereClause(Tree t){
		tree = t;
	}
	
	public void execute(){
		TreeFormat.println("WHERE");
		TreeFormat.up();
		BoolExpr boolexpr = new BoolExpr(tree.getChild(0));
		boolexpr.execute();
		TreeFormat.down();
	}
}
