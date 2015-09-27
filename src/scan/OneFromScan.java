package scan;

import java.util.List;

import util.Env;
import fatworm.driver.Record;
import fatworm.driver.Schema;

public abstract class OneFromScan extends Scan {

	public Scan fromScan;
	
	@Override
	public void setFrom(Scan oldFrom, Scan newFrom){
		newFrom.toScan = this;
		fromScan = newFrom;
	}
	
	public void setFrom(Scan newFrom){
		newFrom.toScan = this;
		fromScan = newFrom;
	}

}
