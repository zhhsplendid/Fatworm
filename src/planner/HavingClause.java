package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class HavingClause {
	Tree tree;
	
	public HavingClause(Tree t){
		tree = t;
	}
	
	public void execute(){
		TreeFormat.println("HAVING");
		TreeFormat.up();
		BoolExpr boolexpr = new BoolExpr(tree.getChild(0));
		boolexpr.execute();
		TreeFormat.down();
	}
}
