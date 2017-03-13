package batch;

import java.io.*;
import edgeheap.*;
import global.*;
import nodeheap.*;
import heap.*;

public class BatchNodeDelete {

	public void deleteBatchNode(NodeHeapfile nhf, EdgeHeapFile ehf, String filePath) throws Exception{
		try{
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String newInput = "";
			while((newInput = br.readLine()) != null){				
				String nodeLabel = newInput.trim();
				BatchInsert batchinsert = new BatchInsert();
				boolean deleteStatus = false;
				NID currentNid = new NID(); //To store a copy of the nid
				
				//Delete the node				
				NID newNid = batchinsert.getNidFromNodeLabel(nodeLabel, nhf);
				currentNid.copyNid(newNid);				
				deleteStatus = nhf.deleteRecord(newNid);
				
				//Delete all edges associated with the node
				EID newEid = new EID();
				EScan newEscan = ehf.openScan();
				Edge newEdge = new Edge();
				//boolean done = false;
				NID sourceNID = null;
				NID destinationNID = null;
				deleteStatus = false;
				boolean done = false;
				
				while(!done){
					newEdge = newEscan.getNext(newEid);
					if (newEdge == null) {
						done = true;
						break;
					}
					newEdge.setHdr();
					sourceNID = newEdge.getSource();
					destinationNID = newEdge.getDestination();
					if(currentNid.equals(sourceNID) || currentNid.equals(destinationNID)){
						deleteStatus = ehf.deleteRecord(newEid);
					}//end-if					
				}//end-while			
								
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
