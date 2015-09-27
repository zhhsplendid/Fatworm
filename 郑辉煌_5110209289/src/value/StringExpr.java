package value;

import java.util.LinkedList;
import java.util.List;

import util.Env;
import util.Lib;
import datatype.DataRecord;
import datatype.Varchar;

public class StringExpr extends Expr {
	
	public String str;
	
	public StringExpr(String s){
		super();
		str = s;
		isConst = true;
		size = 1;
		value = new Varchar(str);
		type = java.sql.Types.VARCHAR;
	}
	
	@Override
	public String toString(){
		return "\"" + str + "\"";
	}
	
	@Override
	public boolean valuePredicate(Env env) {
		return Lib.toBoolean(str);
	}

	@Override
	public DataRecord valueExpr(Env env) {
		return value;
	}

	@Override
	public List<String> requestCol() {
		return new LinkedList<String>();
	}

	@Override
	public void rename(String oldName, String newName) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasSubquery() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Expr clone() {
		return new StringExpr(str);
	}

}
