package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class Func{
	Tree tree;
	
	public Func(Tree t){
		tree = t;
	}
	
	public void execute(){
		TreeFormat.println(tree.getText());
		TreeFormat.up();
		ColName col_name = new ColName(tree.getChild(0));
		col_name.execute();
		TreeFormat.down();
	}
}
