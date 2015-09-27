package fatworm.driver;

import java.sql.SQLException;

public class ResultSetMetaData implements java.sql.ResultSetMetaData{
	public long time;
	Schema schema;
	public ResultSetMetaData(Schema s){
		schema = s;
	}
	

	@Override
	public int getColumnCount() throws SQLException {
		return schema.columnName.size();
	}
	@Override
	public int getColumnType(int arg0) throws SQLException {
		return schema.getColumn(schema.columnName.get(arg0 - 1)).type;
	}

}
