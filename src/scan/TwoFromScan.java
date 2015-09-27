package scan;

public abstract class TwoFromScan extends Scan{
	public Scan left;
	public Scan right;
	
	@Override
	public void setFrom(Scan oldFrom, Scan newFrom){
		newFrom.toScan = this;
		
		if(left == oldFrom){
			left = newFrom;
		}
		else if(right == oldFrom){
			right = newFrom;
		}
	}
}
