package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class InsertColumns extends Token{
	
	String tbl_name; 
	
	public InsertColumns(Tree t){
		super(t);
		tbl_name = tree.getChild(0).getText();
	}
	
	public void execute(){
		int count = tree.getChildCount();
		Tree child;
		
		TreeFormat.println("InsertColumns");
		TreeFormat.up();
		TreeFormat.println(tbl_name);
		for(int i = 1; i < count - 1; ++i){
			child = tree.getChild(i);
			ColName col_name = new ColName(child);
			col_name.execute();
		}
		child = tree.getChild(count - 1);
		ValuesClause values = new ValuesClause(child);
		values.execute();
		TreeFormat.down();
	}
}
