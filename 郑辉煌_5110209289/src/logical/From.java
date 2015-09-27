package logical;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;
import planner.SelectSuffix;
import parser.FatwormParser;

public class From {
	Tree tree;
	int pos;
	
	public From(Tree t, int p){
		tree = t;
		pos = p;
	}
	
	public void execute(){
		Tree child = tree.getChild(pos);
		int type = child.getType();
		switch(type){
		case FatwormParser.ORDER:
		case FatwormParser.HAVING:
		case FatwormParser.GROUP:
		case FatwormParser.WHERE:
			System.out.println("double select clause");
			break;
		case FatwormParser.FROM:
			SelectSuffix select_suffix = new SelectSuffix(child);
			select_suffix.execute();
			break;
		}
	}
}
