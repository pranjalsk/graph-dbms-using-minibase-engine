package global;

public class EID extends RID {

	/**
	 * default constructor of class
	 */
	public EID() {
	}

	/**
	 * constructor of class
	 */
	public EID(PageId pageno, int slotno) {
		super(pageno, slotno);
	}

	/**
	 * make a copy of the given EID
	 */
	public void copyEID(EID EID) {
		pageNo = EID.pageNo;
		slotNo = EID.slotNo;
	}

	/**
	 * Compares two EID object, i.e, this to the EID
	 * 
	 * @param EID
	 *            EID object to be compared to
	 * @return true if they are equal false if not.
	 */
	public boolean equals(EID EID) {

		if ((this.pageNo.pid == EID.pageNo.pid) && (this.slotNo == EID.slotNo))
			return true;
		else
			return false;
	}
}
