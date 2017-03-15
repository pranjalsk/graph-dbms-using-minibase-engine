package batch;

import java.io.*;

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
			BTreeFile btfEdgeWt, String filePath) throws Exception {
		try {
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String newInput = "";
			while ((newInput = br.readLine()) != null) {
				String nodeLabel = newInput.trim();
				BatchInsert batchinsert = new BatchInsert();
				boolean deleteStatus = false;
				NID currentNid = new NID(); // To store a copy of the nid

				// Delete the node
				NID newNid = batchinsert.getNidFromNodeLabel(nodeLabel, nhf);
				currentNid.copyNid(newNid);
				
				Node deletedNode =  nhf.getRecord(currentNid);	
				deletedNode.setHdr();
				Descriptor desc = deletedNode.getDesc();
				String lbl = deletedNode.getLabel();
				KeyClass descKey = new DescriptorKey(desc);
				KeyClass lblKey = new StringKey(lbl);
				
				deleteStatus = nhf.deleteRecord(newNid);
				
				ztfNodeDesc.Delete(descKey, currentNid);
				btfNodeLbl.Delete(lblKey, currentNid);

				// Delete all edges associated with the node
				EID newEid = new EID();
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

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
