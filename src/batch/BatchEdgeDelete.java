package batch;
import java.io.*;

import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyClass;
import btree.StringKey;
import heap.*;
import edgeheap.*;
import nodeheap.*;
import global.*;

public class BatchEdgeDelete {

	public void deleteBatchEdge(EdgeHeapFile ehf, NodeHeapfile nhf, BTreeFile btf_node, BTreeFile btfEdgeLabl,
			BTreeFile btfEdgeWt, String filePath) throws Exception{
		try{
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String newInput = "";
			while((newInput = br.readLine()) != null){
				String inputAttributes[] = newInput.trim().split(" ");
				String sourceLabel = inputAttributes[0];
				String destinationLabel = inputAttributes[1];
				String edgeLabel = inputAttributes[2];
				
				
				
				BatchInsert batchinsert = new BatchInsert();
				NID sourceNID = batchinsert.getNidFromNodeLabel(sourceLabel, nhf, btf_node);
				NID destinationNID = batchinsert.getNidFromNodeLabel(destinationLabel, nhf,btf_node);
				EID newEid = batchinsert.getEidFromEdgeLabel(sourceNID, destinationNID, edgeLabel, ehf, btfEdgeLabl);
				
				EID currentEid = new EID();
				currentEid.copyEID(newEid);
				
				Edge deletedEdge =  ehf.getRecord(currentEid);
				deletedEdge.setHdr();
				String edgelbl = deletedEdge.getLabel();
				int edgeWt = deletedEdge.getWeight();
				
				KeyClass edgelblKey = new StringKey(edgelbl);
				KeyClass edgeWtKey = new IntegerKey(edgeWt);			
					
				
				try {
					boolean deleteStatus = ehf.deleteRecord(newEid);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				btfEdgeLabl.Delete(edgelblKey, currentEid);
				btfEdgeWt.Delete(edgeWtKey, currentEid);	
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
}
