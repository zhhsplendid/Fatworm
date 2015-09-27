package planner;

import org.antlr.runtime.tree.Tree;
import output.TreeFormat;

public class DropDatabase extends Token {
	String dbname;
	public DropDatabase(Tree t){
		super(t);
		dbname = tree.getChild(0).getText();
	}
	
	public void execute(){
		
		TreeFormat.println("DROP_DATABASE");
		TreeFormat.up();
		TreeFormat.println(dbname);
		TreeFormat.down();
	}
}
