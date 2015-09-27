package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class Delete extends Token{
	String tbl_name;
	
	public Delete(Tree t){
		super(t);
		tbl_name = t.getChild(0).getText();
	}
	
	public void execute(){
		TreeFormat.println("DELETE");
		TreeFormat.up();
		TreeFormat.println(tbl_name);
		WhereClause where = new WhereClause(tree.getChild(1));
		where.execute();
		TreeFormat.down();
	}
}
