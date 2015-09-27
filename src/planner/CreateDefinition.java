package planner;

import org.antlr.runtime.tree.Tree;

import output.TreeFormat;

import parser.FatwormParser;

public class CreateDefinition extends Token{
	ColName col_name;
	DataType data_type;
	public CreateDefinition(Tree t){
		super(t);
		col_name = new ColName(t.getChild(0));
		data_type = new DataType(t.getChild(1));
	}
	
	public void execute(){
		int count = tree.getChildCount();
		
		
		TreeFormat.println("CreateDefinition");
		TreeFormat.up();
		col_name.execute();
		data_type.execute();
		TreeFormat.down();
		
		//TreeFormat.up();
		//column_definition_suffix*
		for(int i = 2; i < count; ++i){
			Tree child = tree.getChild(i);
			int type = child.getType();
			
			switch(type){
			case FatwormParser.NULL:
				TreeFormat.up();
				handleNull(child);
				TreeFormat.down();
				break;
			case FatwormParser.DEFAULT:
				TreeFormat.up();
				handleDefault(child);
				TreeFormat.down();
				break;
			case FatwormParser.AUTO_INCREMENT:
				TreeFormat.up();
				handleAutoIncrement(child);
				TreeFormat.down();
				break;
			}
		}
	}
	
	private void handleNull(Tree child){
		
		if(child.getChildCount() != 0){
			TreeFormat.println("NOT_NULL");
		}
		else{
			TreeFormat.println("NULL");
		}
	}
	private void handleDefault(Tree child){
		TreeFormat.println("DEFAULT");
		TreeFormat.up();
		ConstValue const_value = new ConstValue(child.getChild(0));
		const_value.execute();
		TreeFormat.down();
	}
	private void handleAutoIncrement(Tree child){
		TreeFormat.println("AutoIncrement");
	}
	
}
