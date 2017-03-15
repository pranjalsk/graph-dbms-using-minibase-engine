package zindex;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

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
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

/**
 * ZTFileScan implements a search/iterate interface to Z tree index files (class
 * ZTreefile). It derives from abstract base class IndexFileScan.
 */
public class ZTFileScan extends IndexFileScan implements GlobalConst {
	BTreeFile zBTFile;
	BTFileScan bScan;
	boolean rangeScan;
	KeyClass target;
	int distance;

	/**
	 * create a scan with given keys Cases: (1) lo_key = null, hi_key = null
	 * scan the whole index 
	 * (2) lo_key = null, hi_key!= null range scan from min to the hi_key 
	 * (3) lo_key!= null, hi_key = null range scan from the lo_key to max 
	 * (4) lo_key!= null, hi_key!= null, lo_key = hi_key exact match (might not unique) 
	 * (5) lo_key!= null, hi_key!= null, lo_key < hi_key range scan from lo_key to hi_key
	 * 
	 * @param lowKey
	 * @param highKey
	 * @throws GetFileEntryException
	 * @throws PinPageException
	 * @throws ConstructPageException
	 * @throws KeyNotMatchException
	 * @throws IteratorException
	 * @throws UnpinPageException
	 * @throws IOException
	 */
	public ZTFileScan(KeyClass lowKey, KeyClass highKey)
			throws GetFileEntryException, PinPageException,
			ConstructPageException, KeyNotMatchException, IteratorException,
			UnpinPageException, IOException {
		zBTFile = new BTreeFile("zBTFile");
		rangeScan = false;
		if (lowKey instanceof DescriptorKey && highKey instanceof DescriptorKey) {
			String zLow = ZValue.getZValue(((DescriptorKey) lowKey).getKey());
			KeyClass key_low = new StringKey(zLow);
			String zHigh = ZValue.getZValue(((DescriptorKey) highKey).getKey());
			KeyClass key_high = new StringKey(zHigh);
			bScan = zBTFile.new_scan(key_low, key_high);
		} else {
			bScan = zBTFile.new_scan(lowKey, highKey);
		}

	}

	/**
	 * this method creates a scan that returns the nodes with descriptor distance 
	 * from the target node is less than the distance.
	 * @param target
	 * @param distance
	 * @throws GetFileEntryException
	 * @throws PinPageException
	 * @throws ConstructPageException
	 */
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

	/**
	 * Iterate once (during a scan).
	 * 
	 * @return null if done; otherwise next KeyDataEntry
	 * @exception ScanIteratorException
	 *                iterator error
	 */
	public KeyDataEntry get_next() throws ScanIteratorException {
		if (!rangeScan) {
			return bScan.get_next();
		} else {
			Descriptor keyDesc;
			KeyDataEntry next;
			do {
				next = bScan.get_next();
				if (next == null)
					break;
				KeyClass zVal = next.key;
				String stringKey = zVal.toString();
				keyDesc = ZValue.getDescriptor(stringKey);
			} while (!checkIfInRange(keyDesc));
			return next;
		}
	}

	/**
	 * returns true if the given descriptor is in range from the target descriptor
	 * @param desc
	 * @return
	 */
	private boolean checkIfInRange(Descriptor desc) {
		if (desc.distance(((DescriptorKey) target).getKey()) <= distance) {
			return true;
		}
		return false;
	}

	/**
	 * returns the descriptor low key in KeyClass
	 * @return descriptor key as KeyClass
	 * @throws KeyNotMatchException
	 * @throws UnsupportedEncodingException
	 */
	private KeyClass getLowKey() throws KeyNotMatchException,
			UnsupportedEncodingException {
		Descriptor desc = new Descriptor();
		Descriptor tar = ((DescriptorKey) target).getKey();
		desc.set(tar.get(0) - distance, tar.get(1) - distance, tar.get(2)
				- distance, tar.get(3) - distance, tar.get(4) - distance);
		KeyClass lowKey = new StringKey(getZValue(desc));
		if (desc.get(0) < 0 || desc.get(1) < 0 || desc.get(2) < 0
				|| desc.get(3) < 0 || desc.get(4) < 0) {
			return null;
		}
		return lowKey;
	}

	/**
	 * returns the descriptor high key in KeyClass
	 * @return descriptor key as KeyClass
	 * @throws KeyNotMatchException
	 * @throws UnsupportedEncodingException
	 */
	private KeyClass getHighKey() throws KeyNotMatchException,
			UnsupportedEncodingException {
		Descriptor desc = new Descriptor();
		Descriptor tar = ((DescriptorKey) target).getKey();
		desc.set(tar.get(0) + distance, tar.get(1) + distance, tar.get(2)
				+ distance, tar.get(3) + distance, tar.get(4) + distance);
		KeyClass highKey = new StringKey(getZValue(desc));
		return highKey;
	}

	/**
	 * returns the zvalue as string
	 * @param desc
	 * @return zvalue string
	 * @throws KeyNotMatchException
	 * @throws UnsupportedEncodingException
	 */
	private String getZValue(Descriptor desc) throws KeyNotMatchException,
			UnsupportedEncodingException {
		return ZValue.getZValue(desc);
	}

	/**
	 * Delete currently-being-scanned(i.e., just scanned) data entry.
	 * 
	 * @exception ScanDeleteException
	 *                delete error when scan
	 */
	public void delete_current() throws ScanDeleteException {
		bScan.delete_current();
	}

	/**
	 * max size of the key
	 * @return the maximum size of the key in BTFile
	 */
	public int keysize() {

		return bScan.keysize();
	}

	/**
	 * destructor. unpin some pages if they are not unpinned already. and do
	 * some clearing work.
	 * @throws InvalidFrameNumberException
	 * @throws ReplacerException
	 * @throws PageUnpinnedException
	 * @throws HashEntryNotFoundException
	 * @throws IOException
	 */
	public void DestroyBTreeFileScan() throws InvalidFrameNumberException,
			ReplacerException, PageUnpinnedException,
			HashEntryNotFoundException, IOException {
		bScan.DestroyBTreeFileScan();

	}

}
