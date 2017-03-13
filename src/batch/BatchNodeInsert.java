package batch;

import java.io.*;


import global.*;
import nodeheap.*;
import heap.*;
import diskmgr.*;

public class BatchNodeInsert {

	public void insertBatchNode(NodeHeapfile nhf, String sCurrentLine) {
		
		// TODO Auto-generated method stub
		
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
		Node node=make_node(label,  desc);
		nhf.insertNode(node.getNodeByteArray()); 
 		}
		catch(Exception ex)
		{}
	}

	private Node make_node(String label, Descriptor desc) {
		// TODO Auto-generated method stub
		Node node =new Node();
		try{
		
		node.setLabel(label);
		node.setDesc(desc);
		}
		catch(Exception ex)
		{}
		return node;
	}


}
