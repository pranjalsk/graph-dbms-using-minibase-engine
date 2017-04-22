package batch;

import java.io.*;
import java.util.ArrayList;

import zindex.DescriptorKey;
import zindex.ZTreeFile;

import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyClass;
import btree.StringKey;
import edgeheap.*;
import global.*;
import nodeheap.*;
import heap.*;

public class BatchNodeDelete {

	/**
	 * reurns
	 * 
	 * @param nhf
	 * @param ehf
	 * @param btfNodeLbl
	 * @param ztfNodeDesc
	 * @param btfEdgeLabl
	 * @param btfEdgeWt
	 * @param filePath
	 * @throws Exception
	 */
	public void deleteBatchNode(NodeHeapfile nhf, EdgeHeapFile ehf,
			BTreeFile btfNodeLbl, ZTreeFile ztfNodeDesc, BTreeFile btfEdgeLabl,
			BTreeFile btfEdgeWt, BTreeFile btf_edge_src_label, BTreeFile btf_edge_dest_label, String filePath) throws Exception {
		try {
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String newInput = "";
			ArrayList<NID> nidlist = new ArrayList<NID>();
			
			while ((newInput = br.readLine()) != null) {
				String nodeLabel = newInput.trim();
				BatchMapperClass batchinsert = new BatchMapperClass();
				boolean deleteStatus = false;
				NID currentNid = new NID(); // To store a copy of the nid

				// Delete the node
				NID newNid = batchinsert.getNidFromNodeLabel(nodeLabel, nhf, btfNodeLbl);				
				currentNid.copyNid(newNid);
				
				Node deletedNode =  nhf.getRecord(currentNid);	
				deletedNode.setHdr();
				Descriptor desc = deletedNode.getDesc();
				String lbl = deletedNode.getLabel();
				KeyClass descKey = new DescriptorKey(desc);
				KeyClass lblKey = new StringKey(lbl);
				
				deleteStatus = nhf.deleteRecord(newNid);
				NID nidToDel = new NID();
				nidToDel.copyNid(currentNid);
<<<<<<< HEAD
				nidlist.add(nidToDel);
			
=======
				nidlist.add(currentNid);
>>>>>>> 0e615b15da5c813240b0df9931757dbd01278f7b

				// Delete all edges associated with the node
				/*EID newEid = new EID();
				EScan newEscan = ehf.openScan();
				Edge newEdge = new Edge();
				// boolean done = false;
				NID sourceNID = null;
				NID destinationNID = null;
				deleteStatus = false;
				boolean done = false;

				while (!done) {
					newEdge = newEscan.getNext(newEid);
					EID currentEid = new EID();
					currentEid.copyEID(newEid);
					if (newEdge == null) {
						done = true;
						break;
					}
					newEdge.setHdr();
					
					Edge deletedEdge =  ehf.getRecord(currentEid);
					deletedEdge.setHdr();
					String edgelbl = deletedEdge.getLabel();
					int edgeWt = deletedEdge.getWeight();
					KeyClass edgelblKey = new StringKey(edgelbl);
					KeyClass edgeWtKey = new IntegerKey(edgeWt);
					
					sourceNID = newEdge.getSource();
					destinationNID = newEdge.getDestination();
					if (currentNid.equals(sourceNID)
							|| currentNid.equals(destinationNID)) {
						deleteStatus = ehf.deleteRecord(newEid);
						btfEdgeLabl.Delete(edgelblKey, currentEid);
						btfEdgeWt.Delete(edgeWtKey, currentEid);			
					}// end-if
				}// end-while
				newEscan.closescan();*/

				ztfNodeDesc.Delete(descKey, nidToDel);
				btfNodeLbl.Delete(lblKey, nidToDel);
			}//end-while
<<<<<<< HEAD
			//System.out.println(nidlist);
=======
>>>>>>> 0e615b15da5c813240b0df9931757dbd01278f7b
			
			//Deleting the edges of the deleted nodes
			EID newEid = new EID();
			EScan newEscan = ehf.openScan();
			Edge newEdge = new Edge();
			boolean done = false;
			boolean deleteStatus = false;

			while (!done) {
				newEdge = newEscan.getNext(newEid);
				EID currentEid = new EID();
				currentEid.copyEID(newEid);
				if (newEdge == null) {
					done = true;
					break;
				}
				newEdge.setHdr();
				
				Edge deletedEdge =  ehf.getRecord(currentEid);
				deletedEdge.setHdr();
				String edgelbl = deletedEdge.getLabel();
				int edgeWt = deletedEdge.getWeight();
				KeyClass edgelblKey = new StringKey(edgelbl);
				KeyClass edgeWtKey = new IntegerKey(edgeWt);
				
				NID sourceNID = newEdge.getSource();
				NID destinationNID = newEdge.getDestination();
<<<<<<< HEAD
			//	System.out.println(sourceNID+ " "+ destinationNID);
				if(checkIfContains(nidlist, sourceNID) || checkIfContains(nidlist, destinationNID)) {
					//System.out.println("reached inside if ");
=======
				if(nidlist.contains(sourceNID) || nidlist.contains(destinationNID)) {
>>>>>>> 0e615b15da5c813240b0df9931757dbd01278f7b
					deleteStatus = ehf.deleteRecord(newEid);
					btfEdgeLabl.Delete(edgelblKey, currentEid);
					btfEdgeWt.Delete(edgeWtKey, currentEid);
				}			
			}// end-while
			newEscan.closescan();
		}
		
		catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public boolean checkIfContains(ArrayList<NID> nidlist, NID myNID){	
		for(NID n: nidlist)
			if(n.equals(myNID))
				return true;		
		return false;
	}

}