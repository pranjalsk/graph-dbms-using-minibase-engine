package zindex;

import java.io.IOException;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import btree.AddFileEntryException;
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

public class ZTreeFile extends IndexFile implements GlobalConst {
	
	BTreeFile zBTFile;
	
	public ZTreeFile() throws GetFileEntryException, ConstructPageException, AddFileEntryException, IOException, PinPageException {
		zBTFile = new BTreeFile("zBTFile",AttrType.attrString,21,1);
	}

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

	public ZTFileScan new_scan(KeyClass lo_key, KeyClass hi_key) throws GetFileEntryException, PinPageException, ConstructPageException, KeyNotMatchException, IteratorException, UnpinPageException, IOException {
		return new ZTFileScan(lo_key,hi_key);

	}
	
	public ZTFileScan new_scan(KeyClass target, int distance) throws GetFileEntryException, PinPageException, ConstructPageException, KeyNotMatchException, IteratorException, UnpinPageException, IOException {
		return new ZTFileScan(target,distance);

	}

}
