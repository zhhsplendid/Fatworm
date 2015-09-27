package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import parser.FatwormParser;

public class MultiPlicative extends Expr{
	Tree tree;
	
	public MultiPlicative(Tree t){
		tree = t;	
	}
	
	public void execute(){
		int type = tree.getType();
		switch(type){
		case FatwormParser.T__108:
		case FatwormParser.T__105:
		case FatwormParser.T__113:
			//* / %
			TreeFormat.println(tree.getText());
			TreeFormat.up();
			Atom atom0 = new Atom(tree.getChild(0));
			atom0.execute();
			Atom atom1 = new Atom(tree.getChild(1));
			atom1.execute();
			TreeFormat.down();
			break;
		default:
			Atom atom = new Atom(tree);
			atom.execute();
		}
	}
}
