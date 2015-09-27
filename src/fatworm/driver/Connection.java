package fatworm.driver;

import java.io.File;
import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import output.Debug;


public class Connection implements java.sql.Connection{
	public long time;
	//private boolean autoCommit;
	public Connection(String url){
		if(! url.startsWith("jdbc:fatworm:/")){
			Debug.err("url err! url = " + url);
		}
		String filePath = url.substring("jdbc:fatworm:/".length()) + File.separator + "fatworm";
		if(filePath.startsWith("/")){
			filePath = filePath.substring(1);
		}
		try{
			
			DatabaseEngine.getInstance().openFile(filePath);
		}catch(Exception e){
			try {
				System.out.println("open file failuer, using default file");
				DatabaseEngine.getInstance().openFile("./db/fatworm");
			} catch (Exception ee) {
				ee.printStackTrace();
			}
		}
	}
	
	@Override
	public Statement createStatement() throws SQLException {
		return new fatworm.driver.Statement();
	}
	
	@Override
	public void close() throws SQLException {
		try {
			DatabaseEngine.getInstance().close();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	/*
	public Connection(String url) {
		String file = url.substring("jdbc:fatworm://".length()) + File.separator + "fatworm";
		
		try {
			DatabaseEngine.getInstance().openFile(file);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			try {
				Debug.warn("File not accessible, falling back to default file /db/fatworm");		
				DatabaseEngine.getInstance().openFile("/db/fatworm");
			} catch (ClassNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}*/
	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		 DatabaseEngine.autoCommit =  autoCommit;
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return DatabaseEngine.autoCommit;
	}

	@Override
	public void commit() throws SQLException {
		
	}
}
