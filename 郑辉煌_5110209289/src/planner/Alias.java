package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import parser.FatwormParser;

public class Alias {
	Tree tree;
	
	public Alias(Tree t){
		tree = t;
	}
	
	public void execute(){
		int type = tree.getType();
		switch(type){
		case FatwormParser.AVG:
		case FatwormParser.COUNT:
		case FatwormParser.MIN:
		case FatwormParser.MAX:
		case FatwormParser.SUM:
			TreeFormat.println(tree.getText());
			break;
		default:
			TreeFormat.println(tree.getText());
		}
	}
}
