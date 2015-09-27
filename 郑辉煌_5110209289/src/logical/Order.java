package logical;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import planner.SelectSuffix;

import parser.FatwormParser;

public class Order {
	Tree tree;
	int pos;
	
	public Order(Tree t, int p){
		tree = t;
		pos = p;
	}
	
	public void execute(){
		Tree child = tree.getChild(pos);
		int type = child.getType();
		Having having;
		switch(type){
		case FatwormParser.ORDER:
			SelectSuffix select_suffix = new SelectSuffix(child);
			select_suffix.execute();
			
			TreeFormat.up();
			having = new Having(tree, pos - 1);
			having.execute();
			TreeFormat.down();
			break;
		case FatwormParser.HAVING:
		case FatwormParser.GROUP:
		case FatwormParser.WHERE:
		case FatwormParser.FROM:
			having = new Having(tree, pos);
			having.execute();
			break;
		}
	}
}
