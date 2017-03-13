package batch;

import java.io.*;
import nodeheap.*;
import edgeheap.*;
import global.*;
import heap.*;
import diskmgr.*;

public class BatchEdgeInsert {
	
	public void insertBatchEdge(EdgeHeapFile ehf, NodeHeapfile nhf, String filePath) throws Exception{
		try{
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String newInput = "";
			while((newInput = br.readLine()) != null){
				String inputAttributes[] = newInput.trim().split(" ");
				String sourceLabel = inputAttributes[0];
				String destinationLabel = inputAttributes[1];
				String edgeLabel = inputAttributes[3];
				int edgeWeight = Integer.parseInt(inputAttributes[3]);
				
				BatchInsert batchinsert = new BatchInsert();
				NID sourceNID = batchinsert.getNidFromNodeLabel(sourceLabel, nhf);
				NID destinationNID = batchinsert.getNidFromNodeLabel(destinationLabel, nhf);
				
				Edge newEdge = new Edge();
				newEdge.setSource(sourceNID);
				newEdge.setDestination(destinationNID);
				newEdge.setLabel(edgeLabel);
				newEdge.setWeight(edgeWeight);
				
				EID newEid = new EID();
				newEid = ehf.insertEdge(newEdge.getEdgeByteArray());
			}
		}
		
		catch(Exception e){
			e.printStackTrace();
		}
	}
	/*At the end of the batch insertion process, the program should also output the relevant database statistics (node and
			edge counts) and the number of disk pages that were read and written (separately) during the operation.
			To be figured out*/
}
