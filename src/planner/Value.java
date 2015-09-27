package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import parser.FatwormParser;

public class Value extends Expr{
	Tree tree;
	
	public Value(Tree t){
		tree = t;
	}
	
	public void execute(){
		int type = tree.getType();
		switch(type){
		case FatwormParser.T__109:
		case FatwormParser.T__111:
			// + -
			if(tree.getChildCount() == 2){
				TreeFormat.println(tree.getText());
				TreeFormat.up();
				MultiPlicative mp0 = new MultiPlicative(tree.getChild(0));
				mp0.execute();
				MultiPlicative mp1 = new MultiPlicative(tree.getChild(1));
				mp1.execute();
				TreeFormat.down();
			}
			else if(tree.getChildCount() == 1){ // - atom
				TreeFormat.println(tree.getText());
				TreeFormat.up();
				Atom atom = new Atom(tree.getChild(0));
				atom.execute();
				TreeFormat.down();
			}
			break;
		default:
			MultiPlicative mp = new MultiPlicative(tree);
			mp.execute();
		}
		
	}
}
