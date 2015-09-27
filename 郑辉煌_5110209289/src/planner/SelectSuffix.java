package planner;

import org.antlr.runtime.tree.Tree;

import parser.FatwormParser;

public class SelectSuffix {
	Tree tree;
	
	public SelectSuffix(Tree t){
		tree = t;
	}
	
	public void execute(){
		int type = tree.getType();
		switch(type){
		case FatwormParser.FROM:
			FromClause from = new FromClause(tree);
			from.execute();
			break;
		case FatwormParser.WHERE:
			WhereClause where = new WhereClause(tree);
			where.execute();
			break;
		case FatwormParser.GROUP:
			GroupClause group = new GroupClause(tree);
			group.execute();
			break;
		case FatwormParser.HAVING:
			HavingClause having = new HavingClause(tree);
			having.execute();
			break;
		case FatwormParser.ORDER:
			OrderClause order = new OrderClause(tree);
			order.execute();
			break;
		}
	}
}
