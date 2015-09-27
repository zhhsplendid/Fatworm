package logical;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;
import planner.SelectSuffix;
import parser.FatwormParser;

public class Group {
	Tree tree;
	int pos;
	
	public Group(Tree t, int p){
		tree = t;
		pos = p;
	}
	
	public void execute(){
		Tree child = tree.getChild(pos);
		int type = child.getType();
		Where where;
		switch(type){
		case FatwormParser.ORDER:
		case FatwormParser.HAVING:
			System.out.println("double select clause");
			break;
		case FatwormParser.GROUP:
			SelectSuffix select_suffix = new SelectSuffix(child);
			select_suffix.execute();
			
			TreeFormat.up();
			where = new Where(tree, pos - 1);
			where.execute();
			TreeFormat.down();
			break;
		case FatwormParser.WHERE:
		case FatwormParser.FROM:
			where = new Where(tree, pos);
			where.execute();
			break;
		}
	}
}
