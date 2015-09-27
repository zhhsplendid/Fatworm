package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class UpdatePair extends Token{
	ColName col_name;
	Value value;
	public UpdatePair(Tree t){
		super(t);
		col_name = new ColName(t.getChild(0));
		value = new Value(t.getChild(1));
	}
	
	public void execute(){
		TreeFormat.println("UPDATE_PAIR");
		TreeFormat.up();
		col_name.execute();
		value.execute();
		TreeFormat.down();
	}
}
