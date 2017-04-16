package batch;

import global.Descriptor;
import global.EID;
import global.NID;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import nodeheap.NodeHeapfile;
import btree.BTreeFile;
import diskmgr.GraphDB;
import edgeheap.EScan;
import edgeheap.Edge;

public class PathExpressionUtil {

	static NID lookupAndIterateOverPath(String nodeLabel, NodeHeapfile nhf,BTreeFile btf_node) throws Exception {
		BatchInsert batchinsert= new BatchInsert();
		// Get NID of head node
		NID newNid = batchinsert.getNidFromNodeLabel(nodeLabel, nhf, btf_node);
		System.out.println("HeadnodeLabel:"+nodeLabel);
		System.out.println("HeadNID:"+newNid);
       //see if any edges from this node points to next node in the path expression
		return newNid;
	}
	public static NID lookupAndIterateOverPath(Descriptor sourceDesc,
			NodeHeapfile nhf, BTreeFile btf_node) {
		// TODO Auto-generated method stub
		BatchInsert batchinsert= new BatchInsert();
		NID newNid = batchinsert.getNidFromNodeDescriptor( sourceDesc, nhf, btf_node);
		//System.out.println("HeadnodeLabel:"+nodeLabel);
		System.out.println("HeadNID:"+newNid);
       //see if any edges from this node points to next node in the path expression
		return newNid;
	}
	public static void iterateOverPath(NID sourceNID, String sourceLabel, String sourceDescriptor, HashMap<String, String> hm, GraphDB gdb) throws Exception {
		// TODO Auto-generated method stub
		//get next Node(s) in path, see if a path exists to the next Node in path expression
			 
		Set<String> nodeLabels=	hm.keySet();
		System.out.println("nodeLabels length"+nodeLabels.size());
		java.util.Iterator<String> it=nodeLabels.iterator();
		while(it.hasNext())
		{
			String nextNodeLabel=it.next();
			NID nextnodeID=PathExpressionUtil.lookupAndIterateOverPath(nextNodeLabel,gdb.nhf,gdb.btf_node);
			boolean found=scanEdgeHeapFile(gdb,sourceNID,nextnodeID);
			if(found==false)
			{
				System.out.println("sourceLabel"+sourceLabel);
				System.out.println("tailLabel"+nextNodeLabel);
			}
		}
	}
	
	public static boolean scanEdgeHeapFile(GraphDB gdb, NID sourceNID, NID nextnodeID) throws InvalidTupleSizeException,
	IOException, InvalidTypeException, FieldNumberOutOfBoundException {
		// scanning of records
		EID newEid = new EID();
		EScan newEscan = gdb.ehf.openScan();
		Edge newEdge = new Edge();
		boolean done = false;
		boolean found= false;
		while (!done) {
			newEdge  = newEscan.getNext(newEid);
	    if (newEdge  == null) {
		 done = true;
		 break;
	    }
	    newEdge.setHdr();
		NID source = newEdge.getSource();
		 if(sourceNID.equals(source))
		 {
			System.out.println("Found path"); 
			found=true;
		 }

	  }
		newEscan.closescan();
		return found;
		 
	}



}