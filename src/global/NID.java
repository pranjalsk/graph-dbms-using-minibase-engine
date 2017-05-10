package global;

public class NID extends RID {

	/**
	 * default constructor of class
	 */
	public NID() {
	}

	/**
	 * constructor of class
	 */
	public NID(PageId pageno, int slotno) {
		super(pageno, slotno);
	}

	/**
	 * make a copy of the given nid
	 */
	public void copyNid(NID nid) {
		pageNo = nid.pageNo;
		slotNo = nid.slotNo;
	}

	/**
	 * Compares two NID object, i.e, this to the nid
	 * 
	 * @param nid
	 *            NID object to be compared to
	 * @return true if they are equal false if not.
	 */
	public boolean equals(NID nid) {

		if ((this.pageNo.pid == nid.pageNo.pid) && (this.slotNo == nid.slotNo))
			return true;
		else
			return false;
	}
}
