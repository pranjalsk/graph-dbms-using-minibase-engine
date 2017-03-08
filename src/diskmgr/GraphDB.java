package diskmgr;

import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.IOException;

import nodeheap.NodeHeapfile;


public class GraphDB extends DB {
	NodeHeapfile nhf;
	public String filename="NODEHEAPFILE";

	public GraphDB(int type) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {

		switch (type) {
		case 1:
			 nhf = new NodeHeapfile(filename);
			break;
		
		default:
			break;
		}

	}
	
	public GraphDB() {
		// TODO Auto-generated constructor stub
		super();
	}

	public int getNodeCount()
	{
		return 0;
	}

	public NodeHeapfile createHeapFile(String nodeHeapFileName) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		
		nhf = new NodeHeapfile(nodeHeapFileName);
		return nhf;
		
	}	

}