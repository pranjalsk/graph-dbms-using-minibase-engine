package zindex;

import global.Descriptor;
import btree.KeyClass;

/**  StringKey: It extends the KeyClass.
 *   It defines the string Key.
 *   the KeyClass objects are used in the index files for passing key arguments
 */ 
public class DescriptorKey extends KeyClass {

	private Descriptor key;

	/**
	 * DescriptorKey copy constructor
	 * @param desc
	 */
	public DescriptorKey(Descriptor desc) {
		this.key = desc;
	}

	/**
	 * returns the descriptor key
	 * @return key
	 */
	public Descriptor getKey() {
		return new Descriptor(key);
	}

	/**
	 * sets the descriptor key
	 * @param newKey
	 */
	public void setKey(Descriptor newKey) {
		key = new Descriptor(newKey);
	}

	/**
	 * Printable format of Descriptor Key
	 */
	public String toString() {
		return key.toString();
	}

}
