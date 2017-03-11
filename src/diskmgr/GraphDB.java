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
import btree.GetFileEntryException;
import btree.StringKey;

public class GraphDB extends DB {

	private final static boolean OK = true;
	private final static boolean FAIL = false;

	public NodeHeapfile nhf;
	public EdgeHeapFile ehf;
	public BTreeFile btf_node;
	public BTreeFile btf_edge_label;
	public BTreeFile btf_edge_weight;
	public int type;

	public GraphDB(int type) throws HFException, HFBufMgrException,
			HFDiskMgrException, IOException, GetFileEntryException,
			ConstructPageException, AddFileEntryException {

		int keyTypeString = AttrType.attrString;
		int keyTypeInt = AttrType.attrInteger;
		// int KeyTypeDesc = AttrType.attrDesc;

		btf_node = new BTreeFile("IndexNodeLabel", keyTypeString, 34, 1);
		btf_edge_label = new BTreeFile("IndexEdgeLabel", keyTypeString, 34, 1);
		btf_edge_weight = new BTreeFile("IndexEdgeWeight", keyTypeInt, 4, 1);
		nhf = new NodeHeapfile("NodeHeapFile");
		ehf = new EdgeHeapFile("EdgeHeapFile");

		if (type == 1) {
			// index files
		} else {
			// heap files
		}
	}

<<<<<<< HEAD
//	
//	public void createBTNodeLabel(){
//		NID nid = new NID();
//		String key = null;
//		Node newNode = null;
//		NScan newNscan = nhf.openScan();
//		boolean done = false;
//		
//		while(!done){
//			newNode = newNscan.getNext(nid);
//			key = newNode.getLabel();
//			btf_node.insert(key, nid);
//			
//			
//		}
//		// close the file scan
//		newNscan.closescan();
//	}
	
	
	public void initGraphDB(String db_name) {
=======
	public static void initGraphDB(String db_name) {
>>>>>>> 90cda1e0f35d63d82c9ef841fe8d09539ba70b0d
		String dbpath = "/tmp/" + db_name + System.getProperty("user.name")
				+ ".minibase-db";
		String logpath = "/tmp/" + db_name + System.getProperty("user.name")
				+ ".minibase-log";
		SystemDefs sysdef = new SystemDefs(dbpath, 100, 100, "Clock");

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

	
	/*
	 * public static void main(String[] args) throws HFException,
	 * HFBufMgrException, HFDiskMgrException, GetFileEntryException,
	 * ConstructPageException, AddFileEntryException, IOException,
	 * heap.FieldNumberOutOfBoundException, InvalidSlotNumberException,
	 * InvalidTupleSizeException, SpaceNotAvailableException,
	 * FieldNumberOutOfBoundException { initGraphDB("MyDB"); GraphDB gdb = new
	 * GraphDB(0); //gdb.initGraphDB("GraphDBTest");
	 * 
	 * Descriptor desc = new Descriptor(); desc.set(1, 2, 3, 4, 5);
	 * 
	 * Node node = new Node(); node.setLabel("A"); node.setDesc(desc);
	 * 
	 * gdb.insertNode(node.getLabel(), node.getDesc());
	 * 
	 * System.out.println(gdb.nhf.getNodeCnt()); //System.out.println(gdb.nhf.);
	 * 
	 * }
	 */

}
