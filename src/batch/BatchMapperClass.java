package batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import zindex.ZTFileScan;
import zindex.ZTreeFile;

import diskmgr.Page;
import edgeheap.*;
import nodeheap.*;
import global.*;
import heap.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.RelSpec;
import zindex.*;
import zindex.DescriptorKey;
import btree.*;

public class BatchMapperClass {
	/*
	 * Function to find the NID for a given Node label We get the node heap file
	 * from the GraphDB instance; this is passed as argument
	 */
	public NID getNidFromNodeLabel(String nodeLabel, NodeHeapfile nhf,
			BTreeFile btf_node) throws Exception {
		try {
			// NID newNid = new NID();
			// NScan newNscan = nhf.openScan();
			// Node newNode = new Node();
			// boolean done = false;
			//
			// while(!done){
			// newNode = newNscan.getNext(newNid);
			// if (newNode == null) {
			// break;
			// }
			// newNode.setHdr();
			// String label = newNode.getLabel();
			// if(nodeLabel.equalsIgnoreCase(label)){
			// done = true;
			// }
			// }
			// newNscan.closescan();
			// return newNid;
			NID newnid;
			RID newRid = new RID();
			KeyClass key = new StringKey(nodeLabel);
			BTFileScan newScan = btf_node.new_scan(key, key);
			KeyDataEntry newEntry = newScan.get_next();
			if (newEntry != null) {
				LeafData newData = (LeafData) newEntry.data;
				newRid = newData.getData();
				newnid = new NID(newRid.pageNo, newRid.slotNo);
			} else {
				newnid = new NID(new PageId(-1), -1);
			}

			newScan.DestroyBTreeFileScan();
			return newnid;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}// getNidFromNodeLabel

	public EID getEidFromEdgeLabel(NID sourceNID, NID destinationNID,
			String edgeLabel, EdgeHeapFile ehf, BTreeFile btf_edgelabel)
			throws Exception {
		try {
			// EID newEid = new EID();
			// EScan newEscan = ehf.openScan();
			// Edge newEdge = new Edge();
			// boolean done = false;
			//
			// while(!done){
			// newEdge = newEscan.getNext(newEid);
			// if(newEdge == null){
			// done = true;
			// break;
			// }
			// newEdge.setHdr();
			// if(newEdge.getLabel().equalsIgnoreCase(edgeLabel) &&
			// newEdge.getSource().equals(sourceNID) &&
			// newEdge.getDestination().equals(destinationNID)){
			// done = true;
			// }
			// }
			// newEscan.closescan();
			// return newEid;

			// RID newRid = new RID();
			// EID newEid = null;
			// EID currentEID = new EID();
			EScan newEscan = ehf.openScan();

			KeyClass key = new StringKey(edgeLabel);
			BTFileScan newScan = btf_edgelabel.new_scan(null, null);
			// KeyDataEntry newEntry = new KeyDataEntry();
			// boolean done = false;
			KeyDataEntry newEntry = newScan.get_next();
			while (newEntry != null) {
				// KeyDataEntry newEntry = newScan.get_next();
				// if (newEntry == null) {
				// break;
				// }
				LeafData newData = (LeafData) newEntry.data;
				RID newRid = newData.getData();
				EID newEid = new EID(newRid.pageNo, newRid.slotNo);
				Edge newEdge = newEscan.getNext(newEid);
				newEdge.setHdr();
				if (newEdge.getLabel().equalsIgnoreCase(edgeLabel)
						&& newEdge.getSource().equals(sourceNID)
						&& newEdge.getDestination().equals(destinationNID)) {
					// currentEID.copyEID(newEid);
					newScan.DestroyBTreeFileScan();
					// return currentEID;
					return newEid;
				}
				newEntry = newScan.get_next();
			}
			newScan.DestroyBTreeFileScan();
			return new EID(new PageId(-1), -1);

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}// getEidFromEdgeLabel

	

	public Iterator getNidFromDescriptor(String input, NodeHeapfile nhf,
			ZTreeFile ztf_desc) throws Exception {
		
		try {
			Descriptor inputDesc = new Descriptor();
			String[] descInput = input.trim().split(" ");
			int[] values = new int[5];
			for (int ctr = 0; ctr < 5; ctr++) {
				values[ctr] = Integer.parseInt(descInput[ctr]);
			}
			inputDesc.set(values[0], values[1], values[2], values[3],
					values[4]);

			Heapfile newhf = new Heapfile("NIDheapfile");
			boolean flag = false;
			ZTFileScan newScan = ztf_desc.new_scan(null, null);
			KeyDataEntry newEntry = null;

			while ((newEntry = newScan.get_next()) != null) {
				LeafData newData = (LeafData) newEntry.data;				
				RID newRid = newData.getData();
				Tuple t = new Tuple();
				t.setHdr((short)1, new AttrType[] {new AttrType(AttrType.attrId)}, new short[] {});
				t.setIDFld(1, newRid);
				newhf.insertRecord(t.getTupleByteArray());
				flag = true;
			}

			newScan.DestroyBTreeFileScan();
			if(!flag){
				RID newRID = new RID(new PageId(-1), -1);
				Tuple t = new Tuple();
				t.setHdr((short)1, new AttrType[] {new AttrType(AttrType.attrId)}, new short[] {});
				t.setIDFld(1, newRID);
				newhf.insertRecord(t.getTupleByteArray());
			}
			short[] str_sizes = new short[0];
			
			AttrType[] atrType = new AttrType[1];
			atrType[0] = new AttrType(AttrType.attrId);

			FldSpec[] projlist = new FldSpec[1];
			projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

			Iterator iter = new FileScan("NIDheapfile", atrType,
					str_sizes, (short) 1, 1, projlist, null);

			return iter;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}// getNidFromDescriptor

}// BatchInsert

