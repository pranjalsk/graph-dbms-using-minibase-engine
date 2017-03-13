package Test_Phase2;

import java.io.IOException;

import batch.BatchInsert;
import batch.BatchNodeInsert;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteRecException;
import btree.GetFileEntryException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.IteratorException;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.NodeNotMatchException;
import btree.PinPageException;
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

	public static void main(String[] args) throws Exception {
	
//		nodeTesting();
//		edgeTesting();
			
		GraphDB.initGraphDB("MyDB");
		System.out.println("Graph DB 1 creatsed");
		GraphDB gdb = new GraphDB(0);
		
		BatchNodeInsert b = new BatchNodeInsert();
		b.insertBatchNode(gdb.nhf, "A 1 2 3 4 5");
		b.insertBatchNode(gdb.nhf, "B 6 2 3 4 5");
		b.insertBatchNode(gdb.nhf, "C 1 5 3 4 5");
		b.insertBatchNode(gdb.nhf, "D 1 2 2 4 5");
		System.out.println("Nodecnt-->"+gdb.nhf.getNodeCnt());	
		
		NID newNid = new NID();
		NScan newNscan = gdb.nhf.openScan();
		Node newNode = new Node();
		boolean done = false;
		
		while(!done){
			newNode = newNscan.getNext(newNid);
			if (newNode == null) {
				done = true;
			}
			newNode.setHdr();
			String nodeLabel = newNode.getLabel();
			System.out.println(nodeLabel);
		}
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
