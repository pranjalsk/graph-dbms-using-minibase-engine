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
	/**
	 * @param ehf
	 * @param nhf
	 * @param btf_node
	 * @param btfEdgeLabl
	 * @param btfEdgeWt
	 * @param btf_edge_src_label
	 * @param btf_edge_dest_label
	 * @param filePath
	 * @throws Exception
	 */
	public void deleteBatchEdge(EdgeHeapFile ehf, NodeHeapfile nhf,
			BTreeFile btf_node, BTreeFile btfEdgeLabl, BTreeFile btfEdgeWt,BTreeFile btf_edge_src_label ,BTreeFile btf_edge_dest_label, 
			String filePath) throws Exception {
		try {
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String newInput = "";
			while ((newInput = br.readLine()) != null) {
				String inputAttributes[] = newInput.trim().split(" ");
				String sourceLabel = inputAttributes[0];
				String destinationLabel = inputAttributes[1];
				String edgeLabel = inputAttributes[2];

				BatchMapperClass batchinsert = new BatchMapperClass();
				NID sourceNID = batchinsert.getNidFromNodeLabel(sourceLabel,
						nhf, btf_node);
				NID destinationNID = batchinsert.getNidFromNodeLabel(
						destinationLabel, nhf, btf_node);
				if (sourceNID.pageNo.pid != -1 && sourceNID.slotNo != -1
						&& destinationNID.pageNo.pid != -1
						&& destinationNID.slotNo != -1) {

					EID newEid = batchinsert.getEidFromEdgeLabel(sourceNID,
							destinationNID, edgeLabel, ehf, btfEdgeLabl);
					if (newEid.pageNo.pid != -1 && newEid.slotNo != -1) {

						EID currentEid = new EID();
						currentEid.copyEID(newEid);
						Edge deletedEdge = ehf.getRecord(currentEid);
						deletedEdge.setHdr();
						String edgelbl = deletedEdge.getLabel();
						int edgeWt = deletedEdge.getWeight();
						String edgeSrcLbl = deletedEdge.getSourceLabel();
						String edgeDestLbl = deletedEdge.getDestLabel();
						KeyClass edgelblKey = new StringKey(edgelbl);
						KeyClass edgeWtKey = new IntegerKey(edgeWt);
						KeyClass edgSrcKey = new StringKey(edgeSrcLbl);
						KeyClass edgeDestKey = new StringKey(edgeDestLbl);

						try {
							boolean deleteStatus = ehf.deleteRecord(newEid);
						} catch (Exception e) {
							e.printStackTrace();
						}

						btfEdgeLabl.Delete(edgelblKey, currentEid);
						btfEdgeWt.Delete(edgeWtKey, currentEid);
						btf_edge_src_label.Delete(edgSrcKey, currentEid);
						btf_edge_dest_label.Delete(edgeDestKey, currentEid);
					}

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
