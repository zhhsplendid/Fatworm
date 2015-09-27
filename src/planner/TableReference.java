package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import parser.FatwormParser;

public class TableReference {
	Tree tree;
	public TableReference(Tree t){
		tree = t;
	}
	
	public void execute(){
		int type = tree.getType();
		if(type == FatwormParser.ID){
			TreeFormat.println(tree.getText());
			return;
		}
		
		TreeFormat.println("AS");
		Tree child = tree.getChild(0);
		type = child.getType();
		
		TreeFormat.up();
		switch(type){
		case FatwormParser.ID:
			TreeFormat.println(child.getText());
			break;
		default:
			Subquery subquery = new Subquery(child);
			subquery.execute();
		}
		Alias alias = new Alias(tree.getChild(1));
		alias.execute();
		TreeFormat.down();
	}
}
