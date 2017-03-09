package diskmgr;

import edgeheap.*;
import edgeheap.FieldNumberOutOfBoundException;
import global.*;
import nodeheap.*;
import heap.*;

import java.io.IOException;

public class GraphDB extends DB {

	private final static boolean OK = true;
	private final static boolean FAIL = false;

	NodeHeapfile nhf;
	EdgeHeapFile ehf;

	public GraphDB() throws HFException, HFBufMgrException, HFDiskMgrException,
			IOException {
		super();
		nhf = new NodeHeapfile("NodeHeapFile");
		ehf = new EdgeHeapFile("EdgeHeapFile");
	}

	public GraphDB(int type) throws HFException, HFBufMgrException,
			HFDiskMgrException, IOException {

		if (type == 1) {
			// index file
		} else {
			// heap file
		}
	}

	public NID insertNode(String Label, Descriptor Desc)
			throws InvalidSlotNumberException, InvalidTupleSizeException,
			SpaceNotAvailableException, HFException, HFBufMgrException,
			HFDiskMgrException, IOException, FieldNumberOutOfBoundException, heap.FieldNumberOutOfBoundException {
		Node nd = new Node();
		nd.setLabel(Label);
		nd.setDesc(Desc);
		return nhf.insertNode(nd.getNodeByteArray());
	}
	
	public EID insertEdge(NID src, NID dest, String Label, int Weight)
			throws InvalidSlotNumberException, InvalidTupleSizeException,
			SpaceNotAvailableException, HFException, HFBufMgrException,
			HFDiskMgrException, IOException, FieldNumberOutOfBoundException, heap.FieldNumberOutOfBoundException {
		
		Edge ed = new Edge();
		ed.setSource(src);
		ed.setDestination(dest);
		ed.setLabel(Label);
		ed.setWeight(Weight);
		
		return ehf.insertEdge(ed.getEdgeByteArray());
	}

	public boolean delete(String Label) throws InvalidSlotNumberException,
			HFException, HFBufMgrException, HFDiskMgrException, Exception {
		boolean status = OK;
		NScan scan = null;
		NID nid = new NID();

		if (status == OK) {
			System.out.println("- Delete  the record\n");
			try {
				scan = nhf.openScan();
			} catch (Exception e) {
				status = FAIL;
				System.err.println("*** Error opening scan\n");
				e.printStackTrace();
			}
		}

		if (status == OK) {
			int i = 0;
			DummyNodeRecord rec = null;
			Node node = new Node();
			boolean done = false;

			while (!done) {
				try {
					node = scan.getNext(nid);
					if (node == null) {
						done = true;
					}
				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}

				if (!done && status == OK) {
					try {
						rec = new DummyNodeRecord(node);
					} catch (Exception e) {
						System.err.println("" + e);
						e.printStackTrace();
					}
					
					if (rec.iLabel.equals(Label)) { 
						try {
							status = nhf.deleteRecord(nid);
						} catch (Exception e) {
							status = FAIL;
							System.err.println("*** Error deleting record " + i
									+ "\n");
							e.printStackTrace();
							break;
						}
					}
				}
				++i;
			}
		}
		scan.closescan(); 
		scan = null;
		return status;
	}

	public int getNodeCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException, IOException{
		return nhf.getNodeCnt();
	}
	
	public int getEdgeCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException, IOException{
		return ehf.getEdgeCnt();
	}
}
