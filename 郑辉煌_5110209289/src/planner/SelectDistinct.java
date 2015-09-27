package planner;

import logical.Project;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;


public class SelectDistinct extends Token{
	public SelectDistinct(Tree t){
		super(t);
	}
	
	public void execute(){
		//int count = tree.getChildCount();
		TreeFormat.println("SELECT DISTINCT");
		TreeFormat.up();
		logical();
		/*
		for(int i = 0; i < count; ++i){
			Tree child = tree.getChild(i);
			int type = child.getType();
			
			switch(type){
			
			case FatwormParser.FROM:
			case FatwormParser.WHERE:				
			case FatwormParser.GROUP:				
			case FatwormParser.HAVING:				
			case FatwormParser.ORDER:
				SelectSuffix select_suffix = new SelectSuffix(child);
				select_suffix.execute();
				break;
			default:
				SelectExpr select_expr = new SelectExpr(child);
				select_expr.execute();
			}
		}
		*/
		TreeFormat.down();
	}
	
	public void logical(){
		Project pro = new Project(tree);
		pro.execute();
	}
}
