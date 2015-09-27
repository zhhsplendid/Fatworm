package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

public class Update extends Token{
	String tbl_name;
	public Update(Tree t){
		super(t);
		tbl_name = t.getChild(0).getText();
	}
	
	public void execute(){
		int count = tree.getChildCount();
		
		TreeFormat.println("UPDATE");
		TreeFormat.up();
		TreeFormat.println(tbl_name);
		
		UpdatePair update_pair;
		for(int i = 1; i < count - 1; ++i){
			update_pair = new UpdatePair(tree.getChild(i));
			update_pair.execute();
		}
		WhereClause where = new WhereClause(tree.getChild(count - 1));
		where.execute();
		TreeFormat.down();
	}
}
