package Test_Phase2;

import java.io.IOException;

import zindex.DescriptorKey;
import zindex.ZTFileScan;

import batch.BatchEdgeInsert;
import batch.BatchInsert;
import batch.BatchNodeDelete;
import batch.BatchNodeInsert;
import batch.EdgeQuery;
import batch.EdgeQueryWithIndex;
import batch.NodeQuery;
import batch.NodeQueryWithIndex;
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
import btree.KeyClass;
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
import nodeheap.NodeHeapfile;

public class QueryTest {

	static GraphDB gdb;
	
	public static void main(String[] args) throws Exception {
	
//		nodeTesting();
//		edgeTesting();
			
		GraphDB.initGraphDB("MyDB");
		System.out.println("Graph DB 1 creatsed");
		gdb = new GraphDB(0);
		
		BatchNodeInsert b = new BatchNodeInsert();
		b.insertBatchNode(gdb.nhf, "B 10000 10000 10000 10000 10000");
		b.insertBatchNode(gdb.nhf, "D 500 500 500 500 400");
		b.insertBatchNode(gdb.nhf, "A 500 500 500 500 500");
		b.insertBatchNode(gdb.nhf, "C 500 500 500 500 500");
		b.insertBatchNode(gdb.nhf, "E 550 512 529 515 503");
		b.insertBatchNode(gdb.nhf, "F 550 512 529 515 503");
		b.insertBatchNode(gdb.nhf, "G 550 512 529 515 503");
		b.insertBatchNode(gdb.nhf, "H 550 512 529 515 503");
		b.insertBatchNode(gdb.nhf, "I 550 512 529 515 503");
		b.insertBatchNode(gdb.nhf, "J 550 512 529 515 503");
		b.insertBatchNode(gdb.nhf, "K 550 512 529 43 503");
		b.insertBatchNode(gdb.nhf, "L 550 512 529 515 503");
		b.insertBatchNode(gdb.nhf, "M 550 512 529 515 503");
		b.insertBatchNode(gdb.nhf, "N 550 512 529 515 503");
		b.insertBatchNode(gdb.nhf, "O 550 512 529 515 503");
		b.insertBatchNode(gdb.nhf, "P 550 512 529 515 503");
		b.insertBatchNode(gdb.nhf, "Q 550 512 529 515 503");
		b.insertBatchNode(gdb.nhf, "R 550 512 529 515 503");
		System.out.println("Nodecnt-->"+gdb.nhf.getNodeCnt());	
//		scanNodeHeapFile();
		

		NodeQuery nq = new NodeQuery();
		NodeHeapfile nhf = gdb.nhf;
//		nq.query0(nhf);
//		System.out.println("query 0 without index completed.");
		short nodeLabelLength = 32, edgeLabelLength = 32, numBuf = 12;
//		nq.query1(nhf, nodeLabelLength, numBuf);
//		
		Descriptor targetDescriptor = new Descriptor();
		targetDescriptor.set(500, 500, 500, 500,500);
		//nq.query2(nhf, nodeLabelLength, numBuf, targetDescriptor);
		
		double distance = 100;
		
//		nq.query3(nhf, nodeLabelLength, numBuf, targetDescriptor, distance);
//		System.out.println("query 3 without index completed.");

		
//		gdb.createZTFNodeDesc();
//		scanNode_ZIndexFile();
//		gdb.createBTNodeLabel();
//		scanNodeIndexFile();
//		GraphDB.initGraphDB("MyDB");
//		System.out.println("Graph DB 1 creatsed");
//		GraphDB gdb = new GraphDB(0);
//		b.insertBatchNode(gdb.nhf, "E 1 5 9 4 5");
//		scanNodeHeapFile();
//		edgeInsertTest("A", "B", 445);
//		System.out.println("Edge AB created");
//		edgeInsertTest("B", "D", 829);
//		System.out.println("Edge BD created");
//		edgeInsertTest("C", "D", 747);
//		System.out.println("Edge CD created");
//		edgeInsertTest("A", "C", 478);
//		edgeInsertTest("B", "C", 329);
//		System.out.println("EdgeCount-->"+gdb.ehf.getEdgeCnt());
//		gdb.createBTEdgeLabel();

//		scanEdgeHeapFile();
//		

//		deleteNodeFromHF("A");
//		scanNodeHeapFile();
//		scanEdgeHeapFile();

	
		EdgeQuery eq = new EdgeQuery();
		EdgeQueryWithIndex eqi = new EdgeQueryWithIndex();
		
//		eq.query1(gdb.ehf, gdb.nhf);
//		System.out.println("query 1 without index completed");
		eq.query2(gdb.ehf, gdb.nhf);
		System.out.println("query 2 without index completed");
//		eqi.query3(gdb.ehf, gdb.btf_edge_label, edgeLabelLength, numBuf);
//        System.out.println("query 3 with index completed.");
        
//        eqi.query4(gdb.ehf, gdb.btf_edge_weight, edgeLabelLength, numBuf);
//        System.out.println("query 4 with index completed");
		
//		eqi.query0(gdb.ehf, gdb.btf_edge_label, edgeLabelLength, numBuf);
//		System.out.println("query 0 with index completed");
//		eqi.query1(gdb.ehf, gdb.btf_edge_label, gdb.nhf,edgeLabelLength, numBuf);
//		System.out.println("query 1 with index completed");
		eqi.query2(gdb.ehf, gdb.btf_edge_label, gdb.nhf,edgeLabelLength, numBuf);
		System.out.println("query 2 with index completed");
//		eqi.query5(gdb.ehf, gdb.btf_edge_weight, edgeLabelLength, numBuf, 800, 445);
//		System.out.println("query 5 with index completed");
		

		NodeQueryWithIndex nqi = new NodeQueryWithIndex();
//		short nodeLabelLength = 32, numBuf = 12;
//		nqi.query0(gdb.nhf, gdb.btf_node, nodeLabelLength, numBuf);
//		System.out.println("query 0 with index completed.");
//		nqi.query1(gdb.nhf, gdb.btf_node, nodeLabelLength, numBuf);
//		System.out.println("query 1 with index completed.");
//		nqi.query2(gdb.nhf, gdb.ztf_node_desc, nodeLabelLength, numBuf, targetDescriptor, distance);
//		System.out.println("query 2 with index completed.");
//		nq.query2(gdb.nhf, nodeLabelLength, numBuf, targetDescriptor, distance);
//		System.out.println("query 2 without index completed.");
//		nqi.query3(gdb.nhf, gdb.ztf_node_desc, nodeLabelLength, numBuf, targetDescriptor, distance);
//		System.out.println("query 3 with index completed.");
//		String targetLabel = new String("B");
//		nqi.query4(gdb.nhf, gdb.btf_node, gdb.ehf, nodeLabelLength, numBuf, targetLabel);
//		System.out.println("query 4 with index completed.");
//		nqi.query5(gdb.nhf, gdb.ztf_node_desc ,gdb.ehf,nodeLabelLength, numBuf, targetDescriptor, distance);
//		System.out.println("query 5 with index completed.");
//		nq.query5(gdb.nhf, gdb.ehf,nodeLabelLength, numBuf, targetDescriptor, distance);
//		System.out.println("query 5 without index completed.");
	}
	
	public static void scanNode_ZIndexFile() throws GetFileEntryException, PinPageException, ConstructPageException, KeyNotMatchException, IteratorException, UnpinPageException, IOException {
		//scanning of records
		NID newNid = new NID();

		Descriptor desc_low = new Descriptor();
		desc_low.set(500,500,500,500,500);

		Descriptor desc_high =new Descriptor();
		desc_high.set(6,2,3,4,5);
		
		KeyClass low_key = /*null;*/new DescriptorKey(desc_low);
		KeyClass high_key = /*null;*/new DescriptorKey(desc_high);
		int distance = 100;
		ZTFileScan newDscan = gdb.ztf_node_desc.new_scan(low_key, distance);
		KeyDataEntry newKeyDataEntry = null;
		boolean done = false;
		
		
		while(!done){
			try {
				newKeyDataEntry = newDscan.get_next();
			} catch (ScanIteratorException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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

//	public static void edgeInsertTest(String srcLbl, String destLbl, int edgeWeight) throws Exception{
//		BatchInsert batchinsert = new BatchInsert();
//		NID src  = batchinsert.getNidFromNodeLabel(srcLbl, gdb.nhf);
//		NID dest = batchinsert.getNidFromNodeLabel(destLbl, gdb.nhf);	
//				
//		Edge newEdge = new Edge();
//		newEdge.setHdr();
//		newEdge.setSource(src);
//		newEdge.setDestination(dest);
//		newEdge.setLabel("edge"+srcLbl+destLbl);
//		newEdge.setWeight(edgeWeight);	
//		EID newEid = new EID();
//		newEid = gdb.ehf.insertEdge(newEdge.getEdgeByteArray());	
//	
//	}
	
	
//	public static void deleteNodeFromHF(String Label) throws Exception{
//		String nodeLabel = Label;
//		BatchInsert batchinsert = new BatchInsert();
//		boolean deleteStatus = false;
//		NID currentNid = new NID(); //To store a copy of the nid
//		
//		//Delete the node				
//		NID newNid = batchinsert.getNidFromNodeLabel(nodeLabel, gdb.nhf);
//		currentNid.copyNid(newNid);				
//		deleteStatus = gdb.nhf.deleteRecord(newNid);
//		
//		//Delete all edges associated with the node
//		EID newEid = new EID();
//		EScan newEscan = gdb.ehf.openScan();
//		Edge newEdge = new Edge();
//		//boolean done = false;
//		NID sourceNID = null;
//		NID destinationNID = null;
//		deleteStatus = false;
//		boolean done = false;
//		
//		
//		while(!done){
//			newEdge = newEscan.getNext(newEid);
//			if (newEdge == null) {
//				done = true;
//				break;
//			}
//			newEdge.setHdr();
//			sourceNID = newEdge.getSource();
//			destinationNID = newEdge.getDestination();
//			if(currentNid.equals(sourceNID) || currentNid.equals(destinationNID)){
//				deleteStatus = gdb.ehf.deleteRecord(newEid);
//			}//end-if					
//		}//end-while				
//		scanNodeHeapFile();
//	}
	
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
