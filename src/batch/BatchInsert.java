package batch;

import java.io.IOException;

import edgeheap.*;
import nodeheap.*;
import global.*;
import heap.*;
import btree.*;

public class BatchInsert {
	/* Function to find the NID for a given Node label
	 * We get the node heap file from the GraphDB instance; this is passed as argument
	 */
	public NID getNidFromNodeLabel(String nodeLabel, NodeHeapfile nhf, BTreeFile btf_node) throws Exception{
		try{
			NID newNid = new NID();
			NScan newNscan = nhf.openScan();
			Node newNode = new Node();
			boolean done = false;
			
			while(!done){
				newNode = newNscan.getNext(newNid);
				if (newNode == null) {
					break;
				}
				newNode.setHdr();
				String label = newNode.getLabel();
				if(nodeLabel.equalsIgnoreCase(label)){
					done = true;			
				}
			}
			newNscan.closescan();
			return newNid;
			
//			RID newRid = new RID();
//			KeyClass key = new StringKey(nodeLabel);
//			BTFileScan newScan = btf_node.new_scan(key, key);	
//			KeyDataEntry newEntry = newScan.get_next();
//			LeafData newData = (LeafData)newEntry.data;
//			newRid = newData.getData();
//			NID newnid = new NID(newRid.pageNo, newRid.slotNo);
//			newScan.DestroyBTreeFileScan();
//			return newnid;
		}
		catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}// getNidFromNodeLabel
	
	public EID getEidFromEdgeLabel(NID sourceNID, NID destinationNID, String edgeLabel, EdgeHeapFile ehf, BTreeFile btf_edgelabel) throws Exception{
		try{
			EID newEid = new EID();
			EScan newEscan = ehf.openScan();
			Edge newEdge = new Edge();
			boolean done = false;
			
			while(!done){
				newEdge = newEscan.getNext(newEid);	
				if(newEdge == null){
					done = true;
					break;
				}
				newEdge.setHdr();
				if(newEdge.getLabel().equalsIgnoreCase(edgeLabel) &&
						newEdge.getSource().equals(sourceNID) &&
						newEdge.getDestination().equals(destinationNID)){
					done = true;			
				}
			}
			newEscan.closescan();
			return newEid;
			//RID newRid = new RID();	
			//EID newEid = null;
			/*EID currentEID = new EID();
			EScan newEscan = ehf.openScan();
			
			KeyClass key = new StringKey(edgeLabel);
			BTFileScan newScan = btf_edgelabel.new_scan(key, key);	
			//KeyDataEntry newEntry = new KeyDataEntry();
			//boolean done = false;
			KeyDataEntry newEntry = null;		
			while((newEntry = newScan.get_next()) != null){
				/*KeyDataEntry newEntry = newScan.get_next();
				if (newEntry == null) {
					break;
				}
				LeafData newData = (LeafData)newEntry.data;
				RID newRid = newData.getData();
				EID newEid = new EID(newRid.pageNo, newRid.slotNo);
				Edge newEdge = newEscan.getNext(newEid);	
				newEdge.setHdr();
				if(newEdge.getLabel().equalsIgnoreCase(edgeLabel) &&
						newEdge.getSource().equals(sourceNID) &&
						newEdge.getDestination().equals(destinationNID)){
					currentEID.copyEID(newEid);
					//done = true;			
				}
			}
			return currentEID;
			*/
		}
		catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}// getEidFromEdgeLabel
}//BatchInsert
