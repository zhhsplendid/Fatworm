package logical;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;
import planner.SelectSuffix;
import parser.FatwormParser;

public class Having {
	Tree tree;
	int pos;
	
	public Having(Tree t, int p){
		tree = t;
		pos = p;
	}
	
	public void execute(){
		Tree child = tree.getChild(pos);
		int type = child.getType();
		Group group;
		switch(type){
		case FatwormParser.ORDER:
			System.out.println("double select clause");
			break;
		case FatwormParser.HAVING:
			SelectSuffix select_suffix = new SelectSuffix(child);
			select_suffix.execute();
			
			TreeFormat.up();
			group = new Group(tree, pos - 1);
			group.execute();
			TreeFormat.down();
			break;
		case FatwormParser.GROUP:
		case FatwormParser.WHERE:
		case FatwormParser.FROM:
			group = new Group(tree, pos);
			group.execute();
			break;
		}
	}
}
