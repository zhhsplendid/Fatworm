package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class UseDatabase extends Token {
	String dbname;
	public UseDatabase(Tree t){
		super(t);
		dbname = tree.getChild(0).getText();
	}
	
	public void execute(){
		
		TreeFormat.println("USE_DATABASE");
		TreeFormat.up();
		TreeFormat.println(dbname);
		TreeFormat.down();
	}
}
