package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import parser.FatwormParser;

public class OrderColName {
	Tree tree;
	
	public OrderColName(Tree t){
		tree = t;
	}
	
	public void execute(){
		int type = tree.getType();
		ColName col_name;
		switch(type){
		case FatwormParser.ASC:
			TreeFormat.println("ASC");
			TreeFormat.up();
			col_name = new ColName(tree.getChild(0));
			col_name.execute();
			TreeFormat.down();
			break;
		case FatwormParser.DESC:
			TreeFormat.println("DESC");
			TreeFormat.up();
			col_name = new ColName(tree.getChild(0));
			col_name.execute();
			TreeFormat.down();
			break;
		default:
			//order_by_col_name only give col_name
			//we default it is ASC
			TreeFormat.println("ASC");
			TreeFormat.up();
			col_name = new ColName(tree);
			col_name.execute();
			TreeFormat.down();
		}
	} 
}
