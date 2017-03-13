package zindex;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.sun.xml.internal.ws.api.ha.StickyFeature;

import global.Descriptor;
import global.GlobalConst;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.IndexFileScan;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.PinPageException;
import btree.ScanDeleteException;
import btree.ScanIteratorException;
import btree.StringKey;
import btree.UnpinPageException;

public class ZTFileScan extends IndexFileScan implements GlobalConst {
	BTreeFile zBTFile;
	BTFileScan bScan;
	boolean rangeScan;
	KeyClass target;
	int distance;

	public ZTFileScan(KeyClass lowKey,KeyClass highKey) throws GetFileEntryException, PinPageException,
			ConstructPageException, KeyNotMatchException, IteratorException,
			UnpinPageException, IOException {
		zBTFile = new BTreeFile("zBTFile");
		rangeScan = false;
		if(lowKey instanceof DescriptorKey && highKey instanceof DescriptorKey){
			String zLow = ZValue.getZValue(((DescriptorKey) lowKey).getKey());
			KeyClass key_low = new StringKey(zLow);
			String zHigh = ZValue.getZValue(((DescriptorKey) highKey).getKey());
			KeyClass key_high = new StringKey(zHigh);
			bScan = zBTFile.new_scan(key_low, key_high);
		}else{
			bScan = zBTFile.new_scan(lowKey, highKey);
		}
		
	}

	public ZTFileScan(KeyClass target, int distance)
			throws GetFileEntryException, PinPageException,
			ConstructPageException {
		zBTFile = new BTreeFile("zBTFile");
		rangeScan = true;
		this.target = target;
		this.distance = distance;
		try {
			bScan = zBTFile.new_scan(getLowKey(), getHighKey());
		} catch (KeyNotMatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IteratorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnpinPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public KeyDataEntry get_next() throws ScanIteratorException {
		if (!rangeScan) {
			return bScan.get_next();
		} else {
			Descriptor keyDesc;
			KeyDataEntry next;
			do {
				next = bScan.get_next();
				if(next == null) break;
				KeyClass zVal = next.key;
				String stringKey = zVal.toString();
				keyDesc = ZValue.getDescriptor(stringKey);
			} while (!checkIfInRange(keyDesc));
			return next;
		}
	}

	private boolean checkIfInRange(Descriptor desc) {
		if (desc.distance(((DescriptorKey) target).getKey()) <= distance) {
			return true;
		}
		return false;
	}
	
	private KeyClass getLowKey() throws KeyNotMatchException, UnsupportedEncodingException{
		Descriptor desc = new Descriptor();
		Descriptor tar = ((DescriptorKey)target).getKey();
		desc.set(tar.get(0)-distance, tar.get(1)-distance, tar.get(2)-distance, tar.get(3)-distance, tar.get(4)-distance);
		KeyClass lowKey = new StringKey(getZValue(desc));
		if(desc.get(0) < 0 || desc.get(1) < 0 || desc.get(2) < 0 || desc.get(3) < 0 || desc.get(4) < 0){
			return null;
		}
		return lowKey;
	}
	
	private KeyClass getHighKey() throws KeyNotMatchException, UnsupportedEncodingException{
		Descriptor desc = new Descriptor();
		Descriptor tar = ((DescriptorKey)target).getKey();
		desc.set(tar.get(0)+distance, tar.get(1)+distance, tar.get(2)+distance, tar.get(3)+distance, tar.get(4)+distance);
		KeyClass highKey = new StringKey(getZValue(desc));
		return highKey;
	}
	
	private String getZValue(Descriptor desc) throws KeyNotMatchException, UnsupportedEncodingException{
		return ZValue.getZValue(desc);
	}
	public void delete_current() throws ScanDeleteException {
		bScan.delete_current();

	}

	public int keysize() {

		return bScan.keysize();
	}

}
