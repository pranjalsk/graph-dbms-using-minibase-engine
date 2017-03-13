package Test_Phase2;

import java.io.IOException;

import batch.BatchEdgeInsert;
import batch.BatchInsert;
import batch.BatchNodeDelete;
import batch.BatchNodeInsert;
import btree.AddFileEntryException;
import btree.BTFileScan;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteRecException;
import btree.GetFileEntryException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.IteratorException;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.StringKey;
import btree.UnpinPageException;

import diskmgr.GraphDB;
import edgeheap.EScan;
import edgeheap.Edge;

import global.Descriptor;
import global.EID;
import global.NID;
import global.PageId;
import global.RID;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import nodeheap.NScan;
import nodeheap.Node;

public class UnitTestingPhase2 {

	static GraphDB gdb;
	
	public static void main(String[] args) throws Exception {
	
//		nodeTesting();
//		edgeTesting();
			
		GraphDB.initGraphDB("MyDB");
		System.out.println("Graph DB 1 creatsed");
		gdb = new GraphDB(0);
		
		BatchNodeInsert b = new BatchNodeInsert();
		b.insertBatchNode(gdb.nhf, "B 1 2 3 4 5");
		b.insertBatchNode(gdb.nhf, "D 6 2 3 4 5");
		b.insertBatchNode(gdb.nhf, "A 1 5 3 4 5");
		b.insertBatchNode(gdb.nhf, "C 1 2 2 4 5");
		System.out.println("Nodecnt-->"+gdb.nhf.getNodeCnt());	
		scanNodeHeapFile();
		gdb.createBTNodeLabel();
		scanNodeIndexFile();
		
		
//		edgeInsertTest("A", "B", 445);
//		edgeInsertTest("B", "D", 829);
//		edgeInsertTest("C", "D", 747);
//		edgeInsertTest("A", "C", 478);
//		edgeInsertTest("B", "C", 329);
//		System.out.println("EdgeCount-->"+gdb.ehf.getEdgeCnt());
//		scanEdgeHeapFile();
//		
//		deleteNodeFromHF("A");
//		//scanNodeHeapFile();
//		scanEdgeHeapFile();
		

	}
	
	public static void edgeInsertTest(String srcLbl, String destLbl, int edgeWeight) throws Exception{
		BatchInsert batchinsert = new BatchInsert();
		NID src  = batchinsert.getNidFromNodeLabel(srcLbl, gdb.nhf);
		NID dest = batchinsert.getNidFromNodeLabel(destLbl, gdb.nhf);	
				
		Edge newEdge = new Edge();
		newEdge.setHdr();
		newEdge.setSource(src);
		newEdge.setDestination(dest);
		newEdge.setLabel("edge"+srcLbl+destLbl);
		newEdge.setWeight(edgeWeight);	
		EID newEid = new EID();
		newEid = gdb.ehf.insertEdge(newEdge.getEdgeByteArray());	
	
	}
	
	
	public static void deleteNodeFromHF(String Label) throws Exception{
		String nodeLabel = Label;
		BatchInsert batchinsert = new BatchInsert();
		boolean deleteStatus = false;
		NID currentNid = new NID(); //To store a copy of the nid
		
		//Delete the node				
		NID newNid = batchinsert.getNidFromNodeLabel(nodeLabel, gdb.nhf);
		currentNid.copyNid(newNid);				
		deleteStatus = gdb.nhf.deleteRecord(newNid);
		
		//Delete all edges associated with the node
		EID newEid = new EID();
		EScan newEscan = gdb.ehf.openScan();
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
				deleteStatus = gdb.ehf.deleteRecord(newEid);
			}//end-if					
		}//end-while				
		scanNodeHeapFile();
	}
	
	public static void scanNodeHeapFile() throws InvalidTupleSizeException, IOException, InvalidTypeException, FieldNumberOutOfBoundException{
		//scanning of records
				NID newNid = new NID();
				NScan newNscan = gdb.nhf.openScan();
				Node newNode = new Node();
				boolean done = false;
				
				while(!done){
					newNode = newNscan.getNext(newNid);
					if (newNode == null) {
						done = true;
						break;
					}
					newNode.setHdr();
					String nodeLabel = newNode.getLabel();
					System.out.println(nodeLabel);
					for (int j = 0; j < 5; j++) {
						System.out.print(newNode.getDesc().get(j));
							
					}
				}
				newNscan.closescan();
				System.out.println("test done");
	} 
	
	public static void scanNodeIndexFile() throws InvalidTupleSizeException, IOException, InvalidTypeException, FieldNumberOutOfBoundException, ScanIteratorException, KeyNotMatchException, IteratorException, ConstructPageException, PinPageException, UnpinPageException{
		//scanning of records
				NID newNid = new NID();
				BTFileScan newNscan = gdb.btf_node.new_scan(null, null);
				KeyDataEntry newKeyDataEntry = null;
				boolean done = false;
				
				while(!done){
					newKeyDataEntry = newNscan.get_next();
					if (newKeyDataEntry == null) {
						done = true;
						break;
					}
					//newKeyDataEntry.setHdr();
					String nodeLabel = newKeyDataEntry.data.toString();
					System.out.println(nodeLabel);
					
				}
				
				System.out.println("test done");
	}
	
	
	public static void scanEdgeHeapFile() throws InvalidTupleSizeException, IOException, InvalidTypeException, FieldNumberOutOfBoundException{
		//scanning of records
				EID newEid = new EID();
				EScan newEscan = gdb.ehf.openScan();
				Edge newEdge = new Edge();
				boolean done = false;
				
				while(!done){
					newEdge = newEscan.getNext(newEid);
					if (newEdge == null) {
						done = true;
						break;
					}
					newEdge.setHdr();
					String edgeLabel = newEdge.getLabel();
					int edgeWeight = newEdge.getWeight();
					NID src = newEdge.getSource();
					NID dest = newEdge.getDestination();
					System.out.println(edgeLabel + edgeWeight);
					
				}
				newEscan.closescan();
				System.out.println("test done");
	} 
	
	
	
	//Node creation working fine
	public static void nodeTesting() throws InvalidTypeException, InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException{
		Descriptor desc = new Descriptor();
		desc.set(1, 2, 3, 4, 5);
		
		Node node = new Node();
		node.setHdr();
		
		node.setLabel("A");
		node.setDesc(desc);
		
		System.out.println(node.getLabel());
		
		for (int j = 0; j < 5; j++) {
			System.out.println(node.getDesc().get(j));
				
		}
	}
	
	//Edge creation working fine
	public static void edgeTesting() throws InvalidTypeException, InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException{
		PageId pageno = new PageId(2);
		NID src = new NID(pageno , 1);
		//NID src = new NID();
		//src.pageNo.pid = 10;
		//src.slotNo = 1;
		PageId pageno1 = new PageId(3);
		NID dest = new NID(pageno1 , 2);
//		NID dest = new NID();
//		dest.pageNo.pid = 11;
//		dest.slotNo = 2;
		
		Edge edge = new Edge();
		edge.setHdr();
		
		
		edge.setLabel("XYZ");
		edge.setWeight(10);
		edge.setSource(src);
		edge.setDestination(dest);
		
		System.out.println(edge.getLabel());
		System.out.println(edge.getWeight());
		System.out.println(edge.getSource().pageNo.pid+ " " + edge.getSource().slotNo);
		System.out.println(edge.getDestination().pageNo.pid + " " + edge.getDestination().slotNo);
	}
		
}
