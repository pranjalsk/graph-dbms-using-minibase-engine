package batch;

import global.NID;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

import diskmgr.GraphDB;

public class PathExpressionOperations {

	private static String graphDBName = "GRAPHDB";
	private static String sourceDescriptor = null;
	private static String sourceLabel;
	static GraphDB gdb;
	public static String dbpath;
	public static String logpath;
	static HashSet<String> hs;
	static HashMap<String,String> hm=new HashMap<String,String>();
	public static String nodeLabel;
	public static String nodeDescriptor;
    public static PathExpressionOperatorTypeOne pathExprOpTypeOne;
    public static NID sourceNID;
    public static String insideArguments[];
	public static void parsePathExpression(String commandLineInvocation) throws Exception {
		
		hs = new HashSet<String>();
		
		/*
		 * Menu Driven Program (CUI for Batch operations) Enter the task name of
		 * your choice Enter the input file path Enter the GraphDB name Call the
		 * appropriate class methods according to the task number
		 */
		    System.out.println("commandLineInvocation1"+commandLineInvocation);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));

	//		System.out
	//			.println("Enter the path query expression in the following format");
	//	System.out.println("(Node Label|Node Descriptor)/(Node Label|Node Descriptor)/(Node Label|Node Descriptor)");


			String inputArguments[] = commandLineInvocation.split("/");
			int length=inputArguments.length;
			//System.out.println("Length"+length);
			if(inputArguments[0].equalsIgnoreCase("exit"))
				System.exit(0);
			//for(int i=0; i<length; i++)
			//{
				System.out.println("Input argument:"+inputArguments[0]);
				boolean present=inputArguments[0].contains("|");
			 
				if(present){
					String inputArg=inputArguments[0];
					System.out.println("inputArg:"+inputArg);
			
					insideArguments = inputArg.split("\\|");
					 int length1=insideArguments.length;
					  for(int j=0; j<length1; j++){
						  //System.out.println(insideArguments[j]);
						  boolean present1=insideArguments[0].contains("(");
						  if(present1)
						  {
							nodeLabel=insideArguments[0].substring(1);
						  }
						  boolean present2=insideArguments[1].contains(")");
						  if(present2)
						  {
							nodeDescriptor=insideArguments[1].substring(0, insideArguments.length-1);
						  }	
						  if(j==0)
						  {
							  sourceLabel=nodeLabel;
							  sourceDescriptor=nodeDescriptor;
						  }
						  if(j > 0)
						  hm.put(nodeLabel, nodeDescriptor);
							if (!hs.contains(graphDBName)) {
								hs.add(graphDBName);
								GraphDB.initGraphDB(graphDBName);	
								gdb = new GraphDB(0);
							}
							else{
								gdb.openDB(graphDBName);
							}
							sourceNID= PathExpressionOperatorTypeOne.lookupAndIterateOverPath(nodeLabel,gdb.nhf,gdb.btf_node);
						  }//for
					}//if present
			//}
		//TODO-task 2.3		
		//	PathExpressionOperatorTypeOne.iterateOverPath(sourceNID,sourceLabel,sourceDescriptor,hm,gdb);


	}// method



}

