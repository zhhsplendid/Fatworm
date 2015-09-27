package planner;

import org.antlr.runtime.tree.Tree;


import output.TreeFormat;

public class InsertSubquery extends Token{
	String tbl_name;
	
	public InsertSubquery(Tree t){
		super(t);
		tbl_name = t.getChild(0).getText();
	}
	
	public void execute(){
		TreeFormat.println("INSERT_SUBQUERY");
		TreeFormat.up();
		TreeFormat.println(tbl_name);
		
		Tree child = tree.getChild(1);
		Subquery subquery = new Subquery(child);
		subquery.execute();
		TreeFormat.down();
	}
}
