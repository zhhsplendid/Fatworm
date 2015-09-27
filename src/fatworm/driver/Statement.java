package fatworm.driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

public class Statement implements java.sql.Statement{
	public long time;
	ResultSet resultSet;
	
	@Override
	public boolean execute(String arg0) throws SQLException {
		
		try {
			resultSet = DatabaseEngine.getInstance().execute(arg0);
		} catch (Throwable e) {
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
	
	@Override
	public ResultSet getResultSet() throws SQLException {
		return resultSet;
	}

	@Override
	public ResultSet executeQuery(String arg0) throws SQLException {
		try {
			resultSet = DatabaseEngine.getInstance().execute(arg0);
		} catch (Throwable e) {
			return null;
		}
		return resultSet;
	}
	
	@Override
	public void close() throws SQLException {
		try{
			DatabaseEngine.getInstance().save();
		} catch (Exception e){
			return;
		}
	}

	

}
