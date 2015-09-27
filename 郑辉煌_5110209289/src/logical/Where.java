package logical;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;
import planner.SelectSuffix;
import parser.FatwormParser;

public class Where {
	Tree tree;
	int pos;
	
	public Where(Tree t, int p){
		tree = t;
		pos = p;
	}
	
	public void execute(){
		Tree child = tree.getChild(pos);
		int type = child.getType();
		From from;
		switch(type){
		case FatwormParser.ORDER:
		case FatwormParser.HAVING:
		case FatwormParser.GROUP:
			System.out.println("double select clause");
			break;
		case FatwormParser.WHERE:
			SelectSuffix select_suffix = new SelectSuffix(child);
			select_suffix.execute();
			
			TreeFormat.up();
			from = new From(tree, pos - 1);
			from.execute();
			TreeFormat.down();
			break;
		case FatwormParser.FROM:
			from = new From(tree, pos);
			from.execute();
			break;
		}
	}
}
