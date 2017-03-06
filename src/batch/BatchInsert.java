package batch;



import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class BatchInsert {
	

 
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
                switch(taskName){
                case "batchnodeinsert":
                try {

                
                	
                	String sCurrentLine;

    				br = new BufferedReader(new FileReader(nodeHeapFileName));
              

    				while ((sCurrentLine = br.readLine()) != null) {
    					System.out.println(sCurrentLine);
    				}
                  }
    				catch (IOException e) {

    					e.printStackTrace();

    				} finally {

    					try {

    						if (br != null)
    							br.close();

    						if (fr != null)
    							fr.close();

    					} catch (IOException ex) {

    						ex.printStackTrace();

    					}

    				}

 
                	break;
                case "batchedgedelete":
                	//TODO tast12
                default:
                	System.out.println( "Error: unrecognized task number " + taskName);
                    break;
               
                }//switch
			 
			
		}//main

	
}//BatchInsert 
