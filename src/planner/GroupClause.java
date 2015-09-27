package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class GroupClause {
	Tree tree;
	ColName col_name;
	public GroupClause(Tree t){
		tree = t;
		col_name = new ColName(t.getChild(0));
	}
	
	public void execute(){
		TreeFormat.println("GROUP");
		TreeFormat.up();
		col_name.execute();
		TreeFormat.down();
	}
}
