package value;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import util.Env;
import datatype.DataRecord;

public class LinearExpr extends Expr {
	String name;
	public ArrayList<Expr> ids;
	public ArrayList<Integer> times;
	LinkedList<String> request;
	public LinearExpr(ArrayList<Expr> id, ArrayList<Integer> time) {
		ids = id;
		times = time;
		size = ids.size() + 1;
	}

	@Override
	public boolean valuePredicate(Env env) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public DataRecord valueExpr(Env env) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> requestCol() {
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		return null;
	}

}
