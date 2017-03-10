package batch;

import java.io.*;
import global.*;
import nodeheap.*;
import heap.*;
import diskmgr.*;

public class BatchNodeInsert {

	/* Function to read the input file and insert all Nodes
	 * Input format: <Label Descriptors>
	 */
	public void insertBatchNode(NodeHeapfile nhf, FileReader fr) throws Exception{
		try{
			BufferedReader br = new BufferedReader(fr);
			String newInput = "";
			while((newInput = br.readLine()) != null){
				String inputAttributes[] = newInput.trim().split(" ");
				String label = inputAttributes[0];
				
				Descriptor desc = new Descriptor();
				int value0 = Integer.parseInt(inputAttributes[1]);
				int value1 = Integer.parseInt(inputAttributes[2]);
				int value2 = Integer.parseInt(inputAttributes[3]);
				int value3 = Integer.parseInt(inputAttributes[4]);
				int value4 = Integer.parseInt(inputAttributes[5]);			
				desc.set(value0, value1, value2, value3, value4);
				
				Node newNode = new Node();
				newNode.setLabel(label);
				newNode.setDesc(desc);
				
				NID newNid = new NID();
				newNid = nhf.insertNode(newNode.getNodeByteArray()); 
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
