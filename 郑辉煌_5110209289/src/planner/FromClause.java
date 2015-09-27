package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class FromClause {
	Tree tree;
	
	public FromClause(Tree t){
		tree = t;
	}
	
	public void execute(){
		int count = tree.getChildCount();
		TreeFormat.println("FROM");
		TreeFormat.up();
		for(int i = 0; i < count; ++i){
			TableReference tbl_ref = new TableReference(tree.getChild(i));
			tbl_ref.execute();
		}
		TreeFormat.down();
	}
}
