package batch;

import java.io.IOException;

import edgeheap.*;
import nodeheap.*;
import global.*;
import heap.*;

public class BatchInsert {
	/* Function to find the NID for a given Node label
	 * We get the node heap file from the GraphDB instance; this is passed as argument
	 */
	public NID getNidFromNodeLabel(String label, NodeHeapfile nhf) throws Exception{
		try{
			NID newNid = new NID();
			NScan newNscan = nhf.openScan();
			Node newNode = new Node();
			boolean done = false;
			
			while(!done){
				newNode = newNscan.getNext(newNid);
				String nodeLabel = newNode.getLabel();
				if(nodeLabel.equalsIgnoreCase(label)){
					done = true;			
				}
			}
			return newNid;
		}
		catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}// getNidFromNodeLabel
	
	public EID getEidFromEdgeLabel(String label, EdgeHeapFile ehf) throws Exception{
		try{
			EID newEid = new EID();
			EScan newEscan = ehf.openScan();
			Edge newEdge = new Edge();
			boolean done = false;
			
			while(!done){
				newEdge = newEscan.getNext(newEid);
				String edgeLabel = newEdge.getLabel();
				if(edgeLabel.equalsIgnoreCase(label)){
					done = true;			
				}
			}
			return newEid;
		}
		catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}// getEidFromEdgeLabel
}//BatchInsert
