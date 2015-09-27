package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class ValuesClause {
	
	Tree tree;
	public ValuesClause(Tree t){
		tree = t;
	}
	
	public void execute(){
		
		TreeFormat.println("VALUES");
		TreeFormat.up();
		
		int count = tree.getChildCount();
		for(int i = 0; i < count; ++i){
			Value value = new Value(tree.getChild(i));
			value.execute();
		}
		TreeFormat.down();
	}
}
