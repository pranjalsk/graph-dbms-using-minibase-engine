package Test_Phase2;

import java.io.IOException;

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
import btree.UnpinPageException;

import diskmgr.GraphDB;
import edgeheap.Edge;

import global.Descriptor;
import global.NID;
import global.PageId;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import nodeheap.Node;

public class UnitTestingPhase2 {

	public static void main(String[] args) throws Exception {
	
		GraphDB.initGraphDB("MyDB");
		System.out.println("Graph DB created");
		GraphDB gdb = new GraphDB(0);
		BatchNodeInsert b = new BatchNodeInsert();
		b.insertBatchNode(gdb.nhf, "A 1 2 3 4 5");
		System.out.println(gdb.nhf.getNodeCnt());	
		
	}
	
	//Node creation working fine
	public void nodeTesting() throws InvalidTypeException, InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException{
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
	public void edgeTesting() throws InvalidTypeException, InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException{
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
