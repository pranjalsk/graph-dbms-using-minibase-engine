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

	
// *******Don't know author of this method and where it is being used*********************	
//	public NID getNidFromNodeDescriptor(Descriptor sourceDesc,
//			NodeHeapfile nhf, BTreeFile btf_node) {
//		try {
//
//			NID newnid;
//			RID newRid = new RID();
//			KeyClass key = new DescriptorKey(sourceDesc);
//			ZTFileScan newScan = new ZTFileScan(key, key);
//			// btf_node.new_scan(key, key);
//			KeyDataEntry newEntry = newScan.get_next();
//			if (newEntry != null) {
//				LeafData newData = (LeafData) newEntry.data;
//				newRid = newData.getData();
//				newnid = new NID(newRid.pageNo, newRid.slotNo);
//			} else {
//				newnid = new NID(new PageId(-1), -1);
//			}
//
//			newScan.DestroyBTreeFileScan();
//			return newnid;
//		} catch (Exception e) {
//			e.printStackTrace();
//			return null;
//		}
//	}

	public List<NID> getNidFromDescriptor(String input, NodeHeapfile nhf,
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
			
			List<NID> nidlist = new ArrayList<NID>();
			//NScan newNscan = nhf.openScan();
			ZTFileScan newScan = ztf_desc.new_scan(null, null);
			KeyDataEntry newEntry = null;

			while ((newEntry = newScan.get_next()) != null) {
				LeafData newData = (LeafData) newEntry.data;
				
				RID newRid = newData.getData();
				NID newNid = new NID(newRid.pageNo, newRid.slotNo);
				Node newNode = nhf.getRecord(newNid);
				newNode.setHdr();
				
				Descriptor temp = new Descriptor();
				temp = newNode.getDesc();	
				if (temp.equal(inputDesc)==1) {
//					System.out.print(newNid+":");
//					newNode.print();				// to check if we are sending right nodes
					nidlist.add(newNid);
				}
			}

			newScan.DestroyBTreeFileScan();
			if(nidlist.size() == 0)
				nidlist.add(new NID(new PageId(-1), -1));
			
			return nidlist;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}// getNidFromDescriptor

}// BatchInsert

