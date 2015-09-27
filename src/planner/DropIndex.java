package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class DropIndex extends Token{
	public DropIndex(Tree t){
		super(t);
	}
	
	public void execute(){
		TreeFormat.println("DROP_INDEX");
		TreeFormat.up();
		TreeFormat.println(tree.getChild(0).getText());
		TreeFormat.println(tree.getChild(1).getText());
		TreeFormat.down();
	}
}
