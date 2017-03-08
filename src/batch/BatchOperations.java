package batch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

public class BatchOperations {


	 
	private static String taskName ="";

	private static String nodeHeapFileName ="";
	
	private static String graphDBName ="";
	//= "C:\\Phd_software\\cse510\\test\\Nodefiletest.txt";

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
				br = new BufferedReader(new FileReader(nodeHeapFileName));       
				while ((sCurrentLine = br.readLine()) != null) {
					System.out.println(sCurrentLine);
					StringTokenizer st=new StringTokenizer(sCurrentLine," ");										
				}
              }
				catch (IOException e) {
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
