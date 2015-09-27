package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

/**
 * Class for token CREATE_DATABASE
 * @author bd
 *
 */
public class CreateDatabase extends Token {
	String dbname;
	public CreateDatabase(Tree t){
		super(t);
		dbname = tree.getChild(0).getText();
	}
	
	public void execute(){
		
		TreeFormat.println("CREATE_DATABASE");
		TreeFormat.up();
		TreeFormat.println(dbname);
		TreeFormat.down();
	}
}
