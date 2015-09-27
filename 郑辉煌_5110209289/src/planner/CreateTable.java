package planner;

import org.antlr.runtime.tree.Tree;

import parser.FatwormParser;

import output.TreeFormat;

public class CreateTable extends Token{
	String tbl_name;
	
	public CreateTable(Tree t){
		super(t);
		tbl_name = tree.getChild(0).getText();
	}
	
	public void execute(){
		int count = tree.getChildCount();
		
		TreeFormat.println("CREATE_TABLE");
		TreeFormat.up();
		TreeFormat.println(tbl_name);
		
		for(int i = 1; i < count; ++i){
			Tree child = tree.getChild(i);
			int type = child.getType();
			
			
			switch(type){
			
			case FatwormParser.CREATE_DEFINITION:
				TreeFormat.up();
				CreateDefinition create_definition = new CreateDefinition(child);
				create_definition.execute();
				TreeFormat.down();
				break;
				
			case FatwormParser.PRIMARY_KEY:
				TreeFormat.up();
				PrimaryKey primary_key = new PrimaryKey(child);
				primary_key.execute();
				TreeFormat.down();
				break;
			}
		}
		TreeFormat.down();
	}
}
