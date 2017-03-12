package zindex;

import global.Descriptor;
import btree.KeyClass;

public class DescriptorKey extends KeyClass {

	private Descriptor key;

	public DescriptorKey(Descriptor desc) {
		this.key = desc;
	}

	public Descriptor getKey() {
		return new Descriptor(key);
	}

	public void setKey(Descriptor newKey) {
		key = new Descriptor(newKey);
	}

	public String toString() {
		return key.toString();
	}

}
