package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class DropTable extends Token{
	public DropTable(Tree t){
		super(t);
	}
	
	public void execute(){
		TreeFormat.println("DROP_TABLE");
		TreeFormat.up();
		int count = tree.getChildCount();
		for(int i = 0; i < count; ++i){
			TreeFormat.println(tree.getChild(i).getText());
		}
		TreeFormat.down();
	}
}
