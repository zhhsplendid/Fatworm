package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class OrderClause {
	Tree tree;
	
	public OrderClause(Tree t){
		tree = t;
	}
	
	public void execute(){
		int count = tree.getChildCount();
		TreeFormat.println("ORDER");
		TreeFormat.up();
		for(int i = 0; i < count; ++i){
			OrderColName order_name = new OrderColName(tree.getChild(i));
			order_name.execute();
		}
		TreeFormat.down();
	}
}
