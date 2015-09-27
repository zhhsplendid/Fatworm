package logical;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import planner.SelectExpr;

import parser.FatwormParser;

public class Project {
	Tree tree;
	public Project(Tree t){
		tree = t;
	}
	
	public void execute(){
		int count = tree.getChildCount();
		boolean flag = false; 
		TreeFormat.println("Project");
		TreeFormat.up();
		for(int i = 0; i < count; ++i){
			Tree child = tree.getChild(i);
			int type = child.getType();
			
			switch(type){
			case FatwormParser.FROM:
			case FatwormParser.WHERE:				
			case FatwormParser.GROUP:				
			case FatwormParser.HAVING:				
			case FatwormParser.ORDER:
				flag = true;
				TreeFormat.up();
				Order order = new Order(tree, count - 1);
				order.execute();
				TreeFormat.down();
				break;
			default:
				SelectExpr select_expr = new SelectExpr(child);
				select_expr.execute();
			};
			
			if(flag){
				break;
			}
		}
		TreeFormat.down();
	}
}
