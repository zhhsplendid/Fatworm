package planner;

import org.antlr.runtime.tree.*;  

import parser.*;

/**
 * Class response to grammar statement 
 * @author bd
 */

public class Statement {
	
	Tree tree;
	public Statement(Tree t){
		tree = t;
	}
	
	/**
	 * choose the AST node to continue planning
	 */
	public void execute(){
		Integer type = tree.getType();
		//TODO
		switch(type){
		//database_statement
		case FatwormParser.CREATE_DATABASE:
			CreateDatabase create_database = new CreateDatabase(tree);
			create_database.execute();
			break;
			
		case FatwormParser.USE_DATABASE:
			UseDatabase use_database = new UseDatabase(tree);
			use_database.execute();
			break;
			
		case FatwormParser.DROP_DATABASE:
			DropDatabase drop_database= new DropDatabase(tree);
			drop_database.execute();
			break;
			
		//table_statement
		case FatwormParser.CREATE_TABLE:
			CreateTable create_table = new CreateTable(tree);
			create_table.execute();
			break;
		case FatwormParser.DROP_TABLE:
			DropTable drop_table = new DropTable(tree);
			drop_table.execute();
			break;
			
		//insert_statement
		case FatwormParser.INSERT_VALUES:
			InsertValues insert_values = new InsertValues(tree);
			insert_values.execute();
			break;
		case FatwormParser.INSERT_COLUMNS:
			InsertColumns insert_columns = new InsertColumns(tree);
			insert_columns.execute();
			break;
		case FatwormParser.INSERT_SUBQUERY:
			InsertSubquery insert_subquery = new InsertSubquery(tree);
			insert_subquery.execute();
			break;
			
		//delete_statement
		case FatwormParser.DELETE:
			Delete delete = new Delete(tree);
			delete.execute();
			break;
			
		//update_statement
		case FatwormParser.UPDATE:
			Update update = new Update(tree);
			update.execute();
			break;
			
		//index_statement
		case FatwormParser.CREATE_INDEX:
			CreateIndex create_index = new CreateIndex(tree);
			create_index.execute();
			break;
			
		case FatwormParser.CREATE_UNIQUE_INDEX:
			CreateUniqueIndex create_unique_index = new CreateUniqueIndex(tree);
			create_unique_index.execute();
			break;
			
		case FatwormParser.DROP_INDEX:
			DropIndex drop_index = new DropIndex(tree);
			drop_index.execute();
		//select_statement
		case FatwormParser.SELECT:
			Select select = new Select(tree);
			select.execute();
			break;
		case FatwormParser.SELECT_DISTINCT:
			SelectDistinct select_distinct = new SelectDistinct(tree);
			select_distinct.execute();
			break;
		}
	}
}
