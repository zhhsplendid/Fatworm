package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class InsertValues extends Token{
	String tbl_name;
	
	public InsertValues(Tree t){
		super(t);
		tbl_name = tree.getChild(0).getText();
	}
	
	public void execute(){
		
		TreeFormat.println("INSERT_VALUES");
		
		TreeFormat.up();
		TreeFormat.println(tbl_name);
		
		ValuesClause values = new ValuesClause(tree.getChild(1));
		values.execute();
		TreeFormat.down();
		
	}
}
