package batch;

import global.Descriptor;
import nodeheap.Node;
import nodeheap.NodeHeapfile;

public class BatchNodeInsert {
	/**
	 * @param nhf
	 * @param sCurrentLine
	 * @throws Exception
	 */
	public void insertBatchNode(NodeHeapfile nhf, String sCurrentLine) throws Exception{
		try{
		String[] tokens = sCurrentLine.split(" ");
		String label=tokens[0];
		int value1=Integer.parseInt(tokens[1]);
		int value2=Integer.parseInt(tokens[2]);
		int value3=Integer.parseInt(tokens[3]);
		int value4=Integer.parseInt(tokens[4]);
		int value5=Integer.parseInt(tokens[5]);
		Descriptor desc= new Descriptor();
		desc.set(value1, value2, value3, value4, value5);
		Node node=makeNode(label, desc);
		nhf.insertNode(node.getNodeByteArray());
 		}
		catch(Exception ex)
		{
			throw new Exception(ex);
		}
	}

	/**
	 * @param label
	 * @param desc
	 * @return
	 * @throws Exception
	 */
	private Node makeNode(String label, Descriptor desc) throws Exception {
		Node node =new Node();
		node.setHdr();
		node.setLabel(label);
		node.setDesc(desc);
		return node;
	}
}