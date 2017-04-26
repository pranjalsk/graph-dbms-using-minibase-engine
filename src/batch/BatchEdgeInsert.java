package batch;

import java.io.*;

import btree.BTreeFile;
import nodeheap.*;
import edgeheap.*;
import global.*;
import heap.*;
import diskmgr.*;

public class BatchEdgeInsert {
	/**
	 * @param ehf
	 * @param nhf
	 * @param btf_node
	 * @param filePath
	 * @throws Exception
	 */
	public void insertBatchEdge(EdgeHeapFile ehf, NodeHeapfile nhf, BTreeFile btf_node, String filePath) throws Exception{
		try{
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String newInput = "";
			while((newInput = br.readLine()) != null){
				String inputAttributes[] = newInput.trim().split(" ");
				String sourceLabel = inputAttributes[0];
				String destinationLabel = inputAttributes[1];
				String edgeLabel = inputAttributes[2];
				int edgeWeight = Integer.parseInt(inputAttributes[3]);
				
				BatchMapperClass batchinsert = new BatchMapperClass();
				NID sourceNID = batchinsert.getNidFromNodeLabel(sourceLabel, nhf, btf_node);
				NID destinationNID = batchinsert.getNidFromNodeLabel(destinationLabel, nhf,btf_node);
				if(sourceNID.pageNo.pid != -1 && sourceNID.slotNo != -1
						&& destinationNID.pageNo.pid != -1 && destinationNID.slotNo != -1) {
					Edge newEdge = new Edge();
					newEdge.setHdr();
					newEdge.setSource(sourceNID);
					newEdge.setDestination(destinationNID);
					newEdge.setLabel(edgeLabel);
					newEdge.setWeight(edgeWeight);
					newEdge.setSourceLabel(sourceLabel);
					newEdge.setDestLabel(destinationLabel);
					EID newEid = new EID();
					newEid = ehf.insertEdge(newEdge.getEdgeByteArray());
				}
			}
		}
		
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
