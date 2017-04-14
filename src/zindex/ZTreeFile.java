package zindex;

import java.io.IOException;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import btree.AddFileEntryException;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteFashionException;
import btree.DeleteRecException;
import btree.FreePageException;
import btree.GetFileEntryException;
import btree.IndexFile;
import btree.IndexFullDeleteException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.InsertRecException;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.LeafRedistributeException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.RecordNotFoundException;
import btree.RedistributeException;
import btree.StringKey;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

/**
 * This is the main definition of class ZTreeFile, which derives from abstract
 * base class IndexFile. It provides an insert/delete interface.
 */
public class ZTreeFile extends IndexFile implements GlobalConst {

	BTreeFile zBTFile;

	/**
	 * this constructor intializes the tree file on a string attribute
	 * 
	 * @throws GetFileEntryException
	 * @throws ConstructPageException
	 * @throws AddFileEntryException
	 * @throws IOException
	 * @throws PinPageException
	 */
	public ZTreeFile(String zTBFile) throws GetFileEntryException,
			ConstructPageException, AddFileEntryException, IOException,
			PinPageException {
		zBTFile = new BTreeFile(zTBFile, AttrType.attrString, 32, 0);
	}

	/**
	 * insert record with the given key and nid
	 * 
	 * @param key
	 *            the key of the record. Input parameter.
	 * @param rid
	 *            the rid of the record. Input parameter.
	 * @exception KeyTooLongException
	 *                key size exceeds the max keysize.
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IOException
	 *                error from the lower layer
	 * @exception LeafInsertRecException
	 *                insert error in leaf page
	 * @exception IndexInsertRecException
	 *                insert error in index page
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception NodeNotMatchException
	 *                node not match index page nor leaf page
	 * @exception ConvertException
	 *                error when convert between revord and byte array
	 * @exception DeleteRecException
	 *                eIndexNodeLabelrror when delete in index page
	 * @exception IndexSearchException
	 *                error when search
	 * @exception IteratorException
	 *                iteIndexNodeLabelrator error
	 * @exception LeafDeleteException
	 *                error when delete in leaf page
	 * @exception InsertException
	 *                error when insert in index page
	 */
	public void insert(KeyClass data, RID rid) throws KeyTooLongException,
			KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException,
			UnpinPageException, PinPageException, NodeNotMatchException,
			ConvertException, DeleteRecException, IndexSearchException,
			IteratorException, LeafDeleteException, InsertException,
			IOException {

		if (data instanceof DescriptorKey) {
			String zVal = ZValue.getZValue(((DescriptorKey) data).getKey());
			KeyClass keyString = new StringKey(zVal);

			zBTFile.insert(keyString, rid);
		} else {
			throw new KeyNotMatchException();
		}

	}

	/**
	 * delete leaf entry given its <key, rid> pair. `rid' is IN the data entry;
	 * it is not the id of the data entry)
	 * 
	 * @param key
	 *            the key in pair <key, rid>. Input Parameter.
	 * @param rid
	 *            the rid in pair <key, rid>. Input Parameter.
	 * @return true if deleted. false if no such record.
	 * @exception DeleteFashionException
	 *                neither full delete nor naive delete
	 * @exception LeafRedistributeException
	 *                redistribution error in leaf pages
	 * @exception RedistributeException
	 *                redistribution error in index pages
	 * @exception InsertRecException
	 *                error when insert in index page
	 * @exception KeyNotMatchException
	 *                key is neither integer key nor string key
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception IndexInsertRecException
	 *                error when insert in index page
	 * @exception FreePageException
	 *                error in BT page constructor
	 * @exception RecordNotFoundException
	 *                error delete a record in a BT page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception IndexFullDeleteException
	 *                fill delete error
	 * @exception LeafDeleteException
	 *                delete error in leaf page
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error in search in index pages
	 * @exception IOException
	 *                error from the lower layer
	 * 
	 */
	public boolean Delete(KeyClass data, RID rid)
			throws DeleteFashionException, LeafRedistributeException,
			RedistributeException, InsertRecException, KeyNotMatchException,
			UnpinPageException, IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException,
			IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException,
			IOException {
		if (data instanceof DescriptorKey) {
			String zVal = ZValue.getZValue(((DescriptorKey) data).getKey());
			KeyClass keyString = new StringKey(zVal);

			zBTFile.Delete(keyString, rid);
			return true;

		} else {
			throw new KeyNotMatchException();
		}

	}

	/**
	 * Cases: (1) lo_key = null, hi_key = null scan the whole index (2) lo_key =
	 * null, hi_key!= null range scan from min to the hi_key (3) lo_key!= null,
	 * hi_key = null range scan from the lo_key to max (4) lo_key!= null,
	 * hi_key!= null, lo_key = hi_key exact match ( might not unique) (5)
	 * lo_key!= null, hi_key!= null, lo_key < hi_key range scan from lo_key to
	 * hi_key
	 * 
	 * @param lo_key
	 * @param hi_key
	 * @return
	 * @throws GetFileEntryException
	 * @throws PinPageException
	 * @throws ConstructPageException
	 * @throws KeyNotMatchException
	 * @throws IteratorException
	 * @throws UnpinPageException
	 * @throws IOException
	 */
	public ZTFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
			throws GetFileEntryException, PinPageException,
			ConstructPageException, KeyNotMatchException, IteratorException,
			UnpinPageException, IOException {
		return new ZTFileScan(lo_key, hi_key);

	}

	/**
	 * creates a scan that returns the descriptors that lies within the given
	 * distance range from the target descriptor
	 * 
	 * @param target
	 * @param distance
	 * @return scan iterator
	 * @throws GetFileEntryException
	 * @throws PinPageException
	 * @throws ConstructPageException
	 * @throws KeyNotMatchException
	 * @throws IteratorException
	 * @throws UnpinPageException
	 * @throws IOException
	 */
	public ZTFileScan new_scan(KeyClass target, int distance)
			throws GetFileEntryException, PinPageException,
			ConstructPageException, KeyNotMatchException, IteratorException,
			UnpinPageException, IOException {
		return new ZTFileScan(target, distance);

	}

	public void close() throws PageUnpinnedException,
			InvalidFrameNumberException, HashEntryNotFoundException,
			ReplacerException {
		zBTFile.close();
	}

}
