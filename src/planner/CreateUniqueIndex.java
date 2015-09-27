package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;


public class CreateUniqueIndex extends Token{
	public CreateUniqueIndex(Tree t){
		super(t);
	}
	
	public void execute(){
		TreeFormat.println("CREATE_UNIQUE_INDEX");
		TreeFormat.up();
		TreeFormat.println(tree.getChild(0).getText());
		TreeFormat.println(tree.getChild(1).getText());
		ColName col_name = new ColName(tree.getChild(2));
		col_name.execute();
		TreeFormat.down();
	}
}
