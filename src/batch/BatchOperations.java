package batch;

import global.Descriptor;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import nodeheap.NodeHeapfile;
import diskmgr.DB;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.GraphDB;
import diskmgr.InvalidPageNumberException;

public class BatchOperations {


	 
	private static String taskName ="";

	private static String nodeHeapFileName ="";
	
	private static String graphDBName ="";
 
	public static void main(String[] args) {

		BufferedReader br = null;
		FileReader fr = null;
		if (args.length < 3)
	    {
	        System.out.println("Error: missing argument\n");
	       // return 1;
	    }
		
			
			taskName=args[0];
			nodeHeapFileName=args[1];
			graphDBName=args[2];
			//fr = new FileReader(nodeHeapFileName);
			//br = new BufferedReader(fr);
			int taskNumber = 0;
			if(taskName.equalsIgnoreCase("batchnodeinsert"))
				taskNumber = 10;
			else if(taskName.equalsIgnoreCase("batchedgeinsert"))
				taskNumber = 11;
			else if(taskName.equalsIgnoreCase("batchnodedelete"))
				taskNumber = 12;
			else if(taskName.equalsIgnoreCase("batchedgedelete"))
				taskNumber = 13;
			
            switch(taskNumber){
            //batch node insert case
            case 10:
            try {                        	
            	String sCurrentLine;
            	BatchNodeInsert bi=new BatchNodeInsert();
				br = new BufferedReader(new FileReader(nodeHeapFileName));       
				while ((sCurrentLine = br.readLine()) != null) {
					System.out.println(sCurrentLine);
					String[] tokens = sCurrentLine.split(" ");
 					
					String label=tokens[0];
					int value1=Integer.valueOf(tokens[1]);
					int value2=Integer.valueOf(tokens[2]);
					int value3=Integer.valueOf(tokens[3]);
					int value4=Integer.valueOf(tokens[4]);
					int value5=Integer.valueOf(tokens[5]);
					Descriptor desc= new Descriptor();
					desc.set(value1, value2, value3, value4, value5);
	            
		            GraphDB dbObject=new GraphDB();
		            File varTmpDir = new File(graphDBName);
		            boolean exists = varTmpDir.exists();
		            if(exists){
			            dbObject.openDB(graphDBName);
			            NodeHeapfile nhf=dbObject.createHeapFile(nodeHeapFileName);
			            bi.insertNode(nhf.get_fileName(), label, desc);
 		              }
		            else
		            	System.out.println("File do not exists");
				     }  
              }
				catch (IOException e) {
					e.printStackTrace();
				} catch (InvalidPageNumberException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (FileIOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (DiskMgrException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (HFException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (HFBufMgrException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (HFDiskMgrException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					try {
						if (br != null)
							br.close();
						//if (fr != null)
						//	fr.close();
					} catch (IOException ex) {
						ex.printStackTrace();
					}
				}
            	break;
            	
            case 11:
            	//TODO task 11
            	break;
            	
            case 12:
            	//TODO task 12
            	break;
            	
            case 13:
            	//TODO task 13
            	break;
            	
            default:
            	System.out.println( "Error: unrecognized task number " + taskName);
                break;
           
            }//switch
		 
		
	}//main


}
