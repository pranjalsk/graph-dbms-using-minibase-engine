package batch;

import global.Descriptor;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import nodeheap.NodeHeapfile;
import diskmgr.DB;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.GraphDB;
import diskmgr.InvalidPageNumberException;

public class BatchOperations {
	 
	private static String taskName ="";	
	
	private static String filePath = "";
	
	private static String graphDBName ="";
 
	public static void main(String[] args) throws Exception{

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		/* Menu Driven Program (CUI for Batch operations)
		 * Enter the task name of your choice
		 * Enter the input file path
		 * Enter the GraphDB name
		 * Call the appropriate class methods according to the task number
		 */
		System.out.println("\n\nList of Batch Operations");
		System.out.println("1) batchnodeinsert");
		System.out.println("2) batchedgeinsert");
		System.out.println("3) batchnodedelete");
		System.out.println("4) batchedgedelete");
		
		System.out.println("Enter the batch operation, the input file path and the name of the Graph Database in the following format");
		System.out.println("<task_name> <file_path> <GraphDB_name>");
		
		String commandLineInvocation = br.readLine().trim();
		String inputArguments[] = commandLineInvocation.split(" ");
		if(inputArguments.length != 3){
			System.out.println("Error: Invalid input");
		}
		
		else{
			taskName=inputArguments[0];
			filePath=inputArguments[1];
			graphDBName=inputArguments[2];

			int taskNumber = 0;
			if(taskName.equalsIgnoreCase("batchnodeinsert"))
				taskNumber = 10;
			else if(taskName.equalsIgnoreCase("batchedgeinsert"))
				taskNumber = 11;
			else if(taskName.equalsIgnoreCase("batchnodedelete"))
				taskNumber = 12;
			else if(taskName.equalsIgnoreCase("batchedgedelete"))
				taskNumber = 13;
			   	 

	    	GraphDB.initGraphDB(graphDBName);
			GraphDB newGDB = new GraphDB(0); //This is to be VERIFIED!!!
 	    	
	        switch(taskNumber){
	        //Task : Batch node insert
	        case 10:
		        try {	  
		        	String sCurrentLine;
		        			        
		        	br = new BufferedReader(new FileReader(filePath));       
					while ((sCurrentLine = br.readLine()) != null) {

		        	BatchNodeInsert newNodeInsert = new BatchNodeInsert();
		        	newNodeInsert.insertBatchNode(newGDB.nhf, sCurrentLine);
		        	 
					}
		        }
				catch (Exception e) {
					e.printStackTrace();
				}	        
	        	break;
	        
	        //Task : Batch Edge Insert
	        case 11:
	        	try {	        	
		        	BatchEdgeInsert newEdgeInsert = new BatchEdgeInsert();
		        	newEdgeInsert.insertBatchEdge(newGDB.ehf, newGDB.nhf, filePath);	        	
		        }
				catch (Exception e) {
					e.printStackTrace();
				}	    
	        	break;
	        	
	        //Task : Batch Node Delete
	        case 12:
	        	try {	        	
		        	BatchNodeDelete newNodeDelete = new BatchNodeDelete();
		        	newNodeDelete.deleteBatchNode(newGDB.nhf, newGDB.ehf, filePath);	        	
		        }
				catch (Exception e) {
					e.printStackTrace();
				}
	        	break;
	        	
	        //Task : Batch Edge Delete
	        case 13:
	        	try {	        	
		        	BatchEdgeDelete newEdgeDelete = new BatchEdgeDelete();
		        	newEdgeDelete.deleteBatchEdge(newGDB.ehf, newGDB.nhf, filePath);	        	
		        }
				catch (Exception e) {
					e.printStackTrace();
				}
	        	break;
	        	
	        default:
	        	System.out.println( "Error: unrecognized task number "+taskName);
	            break;
	       
	        }//switch
		}//else
	}//main

}
