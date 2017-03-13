package diskmgr;

import edgeheap.*;
import edgeheap.FieldNumberOutOfBoundException;
import global.*;
import nodeheap.*;
import heap.*;

import java.io.IOException;
import java.util.HashSet;

import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteRecException;
import btree.GetFileEntryException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.IntegerKey;
import btree.IteratorException;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.StringKey;
import btree.UnpinPageException;

public class GraphDB extends DB {

	public NodeHeapfile nhf;
	public EdgeHeapFile ehf;
	public BTreeFile btf_node;
	public BTreeFile btf_edge_label;
	public BTreeFile btf_edge_weight;
	public int type;
	public static String dbpath;
	public static String logpath;

	public GraphDB(int type) throws HFException, HFBufMgrException,
			HFDiskMgrException, IOException, GetFileEntryException,
			ConstructPageException, AddFileEntryException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, InvalidTupleSizeException, heap.FieldNumberOutOfBoundException {

		this.type = type;
		
		int keyTypeString = AttrType.attrString;
		int keyTypeInt = AttrType.attrInteger;
		int KeyTypeDesc = AttrType.attrDesc;
		
		nhf = new NodeHeapfile("NodeHeapFile"+dbpath);
		System.out.println("heap file created");
		ehf = new EdgeHeapFile("EdgeHeapFile"+dbpath);
		System.out.println("edge heap file cretaed");
		btf_node = new BTreeFile("IndexNodeLabel", keyTypeString, 32, 1);
		btf_edge_label = new BTreeFile("IndexEdgeLabel", keyTypeString, 32, 1);
		btf_edge_weight = new BTreeFile("IndexEdgeWeight", keyTypeInt, 4, 1);
		System.out.println("BTree intitalization is ok");
//		createBTNodeLabel();
//		createBTEdgeLabel();
//		createBTEdgeWeight();
			

	}

	public static void initGraphDB(String db_name) {

		dbpath = "/tmp/" + db_name + System.getProperty("user.name")

				+ ".minibase-db";
		logpath = "/tmp/" + db_name + System.getProperty("user.name")
				+ ".minibase-log";
		SystemDefs sysdef = new SystemDefs(dbpath, 100, 100, "Clock");
		
		PCounter.initialize();
		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent. We assume
		// user are on UNIX system here
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("IO error: " + e);
		}

		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("IO error: " + e);
		}

	}

	public void createBTNodeLabel() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException, InvalidTupleSizeException, heap.FieldNumberOutOfBoundException{
		try {
			NID nid = new NID();
			String key = null;
			Node newNode = null;
			NScan newNscan = nhf.openScan();
			boolean done = false;
			
			while(!done){
				newNode = newNscan.getNext(nid);
				if (newNode==null) {
					done = true;
					break;
				}
				newNode.setHdr();
				key = newNode.getLabel();
				btf_node.insert(new StringKey(key), (RID) nid);			
			}
			newNscan.closescan();
		} catch (Exception e) {
			System.err.println("Empty node heap file");
			e.printStackTrace();
		}
	}
	
	public void createBTEdgeLabel() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException, InvalidTupleSizeException, heap.FieldNumberOutOfBoundException{
		try {
			EID eid = new EID();
			String key = null;
			Edge newEdge = null;
			EScan newEscan = ehf.openScan();
			boolean done = false;
			
			while(!done){
				newEdge = newEscan.getNext(eid);
				if (newEdge == null) {
					done = true;
					break;
				}
				newEdge.setHdr();
				key = newEdge.getLabel();
				btf_edge_label.insert(new StringKey(key), (RID) eid);			
			}
			newEscan.closescan();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void createBTEdgeWeight() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException, InvalidTupleSizeException, heap.FieldNumberOutOfBoundException{
		try {
			EID eid = new EID();
			int key;
			Edge newEdge = null;
			EScan newEscan = ehf.openScan();
			boolean done = false;
			
			while(!done){
				newEdge = newEscan.getNext(eid);
				if (newEdge == null) {
					done = true;
					break;
				}
				newEdge.setHdr();
				key = newEdge.getWeight();
				btf_edge_weight.insert(new IntegerKey(key), (RID) eid);			
			}
			newEscan.closescan();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public int getNodeCnt() throws InvalidSlotNumberException,
			InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException,
			IOException {
		return nhf.getNodeCnt();
	}

	public int getEdgeCnt() throws InvalidSlotNumberException,
			InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException,
			IOException {
		return ehf.getEdgeCnt();
	}

	public int getSourceCnt() {
		try {
			HashSet<NID> hashSet = new HashSet<NID>();
			EID newEid = new EID();
			EScan newEscan = ehf.openScan();
			Edge newEdge = new Edge();
			boolean done = false;

			while (!done) {
				newEdge = newEscan.getNext(newEid);
				NID srcNID = newEdge.getSource();
								
				if (!hashSet.contains(srcNID)) {
					hashSet.add(srcNID);
					done = true;
				}
			}
			return hashSet.size();
		} catch (Exception e) {
			e.printStackTrace();
			return (Integer) null;
		}

	}

	public int getDestinationCnt(){
		try {
			HashSet<NID> hashSet = new HashSet<NID>();
			EID newEid = new EID();
			EScan newEscan = ehf.openScan();
			Edge newEdge = new Edge();
			boolean done = false;

			while (!done) {
				newEdge = newEscan.getNext(newEid);
				NID destNID = newEdge.getDestination();
				if (!hashSet.contains(destNID)) {
					hashSet.add(destNID);
					done = true;
				}
			}
			return hashSet.size();
		} catch (Exception e) {
			e.printStackTrace();
			return (Integer) null;
		}
	}

	public int getLabelCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException, IOException{
		
		int edgeLabelCnt = 0;
		try {
			HashSet<String> hashSet = new HashSet<String>();
			EID newEid = new EID();
			EScan newEscan = ehf.openScan();
			Edge newEdge = new Edge();
			boolean done = false;

			while (!done) {
				newEdge = newEscan.getNext(newEid);
				String edgeLbl = newEdge.getLabel();
								
				if (!hashSet.contains(edgeLbl)) {
					hashSet.add(edgeLbl);
					done = true;
				}
			}
			edgeLabelCnt = hashSet.size();
		} catch (Exception e) {
			e.printStackTrace();
			return (Integer) null;
		}
		
		int nodeLabelCnt = nhf.getNodeCnt();
		
		return (edgeLabelCnt + nodeLabelCnt);		
	}

}
