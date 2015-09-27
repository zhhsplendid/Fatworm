package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class PrimaryKey extends Token{
	
	public PrimaryKey(Tree t){
		super(t);
	}
	
	public void execute(){
		TreeFormat.println("PRIMARY_KEY");
		TreeFormat.up();
		ColName col_name = new ColName(tree.getChild(0));
		col_name.execute();
		TreeFormat.down();
	}
}
