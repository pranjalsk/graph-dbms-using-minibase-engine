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

	public void initGraphDB(String db_name) {
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

	/*
	 * public NID insertNode(String Label, Descriptor Desc) throws
	 * InvalidSlotNumberException, InvalidTupleSizeException,
	 * SpaceNotAvailableException, HFException, HFBufMgrException,
	 * HFDiskMgrException, IOException, FieldNumberOutOfBoundException,
	 * heap.FieldNumberOutOfBoundException { Node nd = new Node();
	 * nd.setLabel(Label); nd.setDesc(Desc); return
	 * nhf.insertNode(nd.getNodeByteArray()); }
	 * 
	 * public EID insertEdge(NID src, NID dest, String Label, int Weight) throws
	 * InvalidSlotNumberException, InvalidTupleSizeException,
	 * SpaceNotAvailableException, HFException, HFBufMgrException,
	 * HFDiskMgrException, IOException, FieldNumberOutOfBoundException,
	 * heap.FieldNumberOutOfBoundException {
	 * 
	 * Edge ed = new Edge(); ed.setSource(src); ed.setDestination(dest);
	 * ed.setLabel(Label); ed.setWeight(Weight);
	 * 
	 * return ehf.insertEdge(ed.getEdgeByteArray()); }
	 * 
	 * public boolean deleteNodeRecord(String Label) throws
	 * InvalidSlotNumberException, HFException, HFBufMgrException,
	 * HFDiskMgrException, Exception { boolean status = OK; NScan scan = null;
	 * NID nid = new NID();
	 * 
	 * if (status == OK) { System.out.println("- Delete  the record\n"); try {
	 * scan = nhf.openScan(); } catch (Exception e) { status = FAIL;
	 * System.err.println("*** Error opening scan\n"); e.printStackTrace(); } }
	 * 
	 * if (status == OK) { int i = 0; DummyNodeRecord rec = null; Node node =
	 * new Node(); boolean done = false;
	 * 
	 * while (!done) { try { node = scan.getNext(nid); if (node == null) { done
	 * = true; } } catch (Exception e) { status = FAIL; e.printStackTrace(); }
	 * 
	 * if (!done && status == OK) { try { rec = new DummyNodeRecord(node); }
	 * catch (Exception e) { System.err.println("" + e); e.printStackTrace(); }
	 * 
	 * if (rec.iLabel.equals(Label)) { try { status = nhf.deleteRecord(nid); }
	 * catch (Exception e) { status = FAIL;
	 * System.err.println("*** Error deleting record " + i + "\n");
	 * e.printStackTrace(); break; } } } ++i; } } scan.closescan(); scan = null;
	 * return status; }
	 */

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

/*	public int getSourceCnt() throws heap.FieldNumberOutOfBoundException,
			InvalidSlotNumberException, InvalidTupleSizeException, HFException,
			HFDiskMgrException, HFBufMgrException, IOException, Exception {

		HashSet<NID> hs = new HashSet<NID>();

		// create hashset to store source NIDs
		// scan the edge heap file and fetch each source NID and add it(if not
		// already in hashset)in hashset
		//

		boolean status = OK;
		EScan scan = null;
		EID eid = new EID();

		if (status == OK) {
			System.out.println("- Delete  the record\n");
			try {
				scan = ehf.openScan();
			} catch (Exception e) {
				status = FAIL;
				System.err.println("*** Error opening scan\n");
				e.printStackTrace();
			}
		}

		if (status == OK) {
			int i = 0;
			DummyEdgeRecord rec = null;
			Edge edge = new Edge();
			boolean done = false;

			while (!done) {
				try {
					edge = scan.getNext(eid);
					if (edge == null) {
						done = true;
					}
				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}

				if (!done && status == OK) {
					try {
						rec = new DummyEdgeRecord(edge);
					} catch (Exception e) {
						System.err.println("" + e);
						e.printStackTrace();
					}

					try {

						NID nid = rec.getSource();
						// NID nid = ehf.getRecord(eid).getSource();
						// scan over file
						// create edge dummy record
						//
						if (!hs.contains(nid)) {
							hs.add(nid);
						}
						status = OK;
					} catch (Exception e) {
						status = FAIL;
						System.err.println("*** Error deleting record " + i
								+ "\n");
						e.printStackTrace();
						break;
					}

				}
				++i;
			}
		}
		scan.closescan();
		scan = null;
		return hs.size();

	}*/

	

}
