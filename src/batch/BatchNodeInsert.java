package batch;

import global.Descriptor;
import global.NID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.IOException;

import nodeheap.Node;
import nodeheap.NodeHeapfile;

public class BatchNodeInsert {

	 public void insertNode(String filename,String label,Descriptor desc)
	    {
		 NodeHeapfile nodeHeapfile = null ;
		try {
			nodeHeapfile = new NodeHeapfile(filename);
			Node node=make_node(label,  desc);
			NID nodeID=nodeHeapfile.insertNode(node.getNodeByteArray());
		} catch (HFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFBufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFDiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  
		}

	   protected Node make_node(String label, Descriptor desc) throws Exception {
		// TODO Auto-generated method stub
		Node node =new Node();
		node.setLabel(label);
		node.setDesc(desc);
		return node;
	  }

}
