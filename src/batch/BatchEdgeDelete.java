package batch;
import java.io.*;
import heap.*;
import edgeheap.*;
import nodeheap.*;
import global.*;

public class BatchEdgeDelete {

	public void deleteBatchEdge(EdgeHeapFile ehf, NodeHeapfile nhf, String filePath) throws Exception{
		try{
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String newInput = "";
			while((newInput = br.readLine()) != null){
				String inputAttributes[] = newInput.trim().split(" ");
				String sourceLabel = inputAttributes[0];
				String destinationLabel = inputAttributes[1];
				String edgeLabel = inputAttributes[2];
				
				BatchInsert batchinsert = new BatchInsert();
				NID sourceNID = batchinsert.getNidFromNodeLabel(sourceLabel, nhf);
				NID destinationNID = batchinsert.getNidFromNodeLabel(destinationLabel, nhf);
				EID newEid = batchinsert.getEidFromEdgeLabel(sourceNID, destinationNID, edgeLabel, ehf);
				boolean deleteStatus = ehf.deleteRecord(newEid);				
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
}
