package diskmgr;

import global.Convert;
import global.Descriptor;
import heap.Tuple;
import nodeheap.*;

class DummyNodeRecord {

	// content of the record
	public String iLabel;
	public Descriptor iDesc;
	
	// length under control
	private int reclen;

	private byte[] data;

	/**
	 * Default constructor
	 */
	public DummyNodeRecord() {
	}

	/**
	 * another constructor
	 */
	public DummyNodeRecord(int _reclen) {
		setRecLen(_reclen);
		data = new byte[_reclen];
	}

	/**
	 * constructor: convert a byte array to DummyRecord object.
	 * 
	 * @param arecord
	 *            a byte array which represents the DummyRecord object
	 */
	public DummyNodeRecord(byte[] arecord) throws java.io.IOException {

		setStrRec(arecord);
		setDescRec(arecord);
		data = arecord;
		setRecLen(iLabel.length());
	}

	/**
	 * constructor: translate a tuple to a DummyRecord object it will make a
	 * copy of the data in the tuple
	 * 
	 * @param atuple
	 *            : the input tuple
	 */
	public DummyNodeRecord(Node _aNode) throws java.io.IOException {
		data = new byte[_aNode.getLength()];
		data = _aNode.getNodeByteArray();
		setRecLen(_aNode.getLength());

		setStrRec(data);
		setDescRec(data);

	}

	/**
	 * convert this class objcet to a byte array this is used when you want to
	 * write this object to a byte array
	 */
	public byte[] toByteArray() throws java.io.IOException {
		// data = new byte[reclen];
		Convert.setStrValue(iLabel, 0, data);
		Convert.setDescValue(iDesc, reclen - 20, data);
		return data;
	}



	/**
	 * get the String value out of the byte array and set it to the float value
	 * of the HTDummyRecorHT object
	 */
	public void setStrRec(byte[] _data) throws java.io.IOException {
		// System.out.println("reclne= "+reclen);
		// System.out.println("data size "+_data.size());
		iLabel = Convert.getStrValue(0, _data, reclen - 20);
	}
	public void setDescRec(byte[] _data) throws java.io.IOException {
		// System.out.println("reclne= "+reclen);
		// System.out.println("data size "+_data.size());
		iDesc = Convert.getDescValue(reclen - 20, _data);
	}

	// Other access methods to the size of the String field and
	// the size of the record
	public void setRecLen(int size) {
		reclen = size;
	}

	public int getRecLength() {
		return reclen;
	}
}

