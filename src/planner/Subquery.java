package planner;

import org.antlr.runtime.tree.Tree;

import parser.FatwormParser;

public class Subquery{
	Tree tree;
	
	public Subquery(Tree t){
		tree = t;
	}
	
	public void execute(){
		int type = tree.getType();
		switch(type){
		case FatwormParser.SELECT:
			Select select = new Select(tree);
			select.execute();
			break;
		case FatwormParser.SELECT_DISTINCT:
			SelectDistinct select_distinct = new SelectDistinct(tree);
			select_distinct.execute();
			break;
		}
	}
}
