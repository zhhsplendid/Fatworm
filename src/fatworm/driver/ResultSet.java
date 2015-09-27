package fatworm.driver;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

import datatype.DataRecord;
import output.Debug;
import scan.EmptyScan;
import scan.Scan;



public class ResultSet implements java.sql.ResultSet{
	public long time;
	public Scan scan;
	public Record current;
	public ResultSet() {
		current = null;
	}

	public ResultSet(Scan s) {
		if(s == null)s = EmptyScan.getInstance();
		if(!s.hasComputed) Debug.err("not yet executed!");
		scan = s;
	}

	@Override
	public boolean next() throws SQLException {
		boolean has = scan.hasNext();
		if(has){
			current = scan.next();
		}
		return has;
	}

	@Override
	public void beforeFirst() throws SQLException {
		scan.beforeFirst();
		current = null;
	}

	@Override
	public void close() throws SQLException {
		scan.close();
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return DataRecord.getObject(current.cols.get(columnIndex-1));
	}
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return new ResultSetMetaData(scan.getSchema());
	}
	

}
