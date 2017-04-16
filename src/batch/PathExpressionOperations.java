package batch;

import global.AttrType;
import global.Descriptor;
import global.NID;
import global.TupleOrder;
import heap.Tuple;
import iterator.Iterator;
import iterator.Sort;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import nodeheap.Node;
import diskmgr.GraphDB;

public class PathExpressionOperations {

	static boolean nodeLabelQuery = false;
	static boolean nodeDescriptorQuery = false;
	private static String sourceDescriptor = null;
	private static String sourceLabel;
	static GraphDB gdb;
	public static String dbpath;
	public static String logpath;
	static HashSet<String> hs;
	static HashMap<String, String> hm = new HashMap<String, String>();
	public static String nodeLabel;
	public static String nodeDescriptor;
	public static Descriptor sourceDesc;
	static AttrType[] attr;
	public static NID sourceNID;
	public static String insideArguments[];
	public static Object args[];
	static short numbuf = 50;

	public static void parsePathExpression(String commandLineInvocation,
			String graphDBName, GraphDB gdb, String pathExpressionOperationType)
			throws Exception {

		//hs = new HashSet<String>();

		/*
		 * Menu Driven Program (CUI for Batch operations) Enter the task name of
		 * your choice Enter the input file path Enter the GraphDB name Call the
		 * appropriate class methods according to the task number
		 */
<<<<<<< HEAD
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
							//	GraphDB.initGraphDB(graphDBName);	
							//	gdb = new GraphDB(0);
							}
							else{
								gdb.openDB(graphDBName);
							}
	//						sourceNID= PathExpressionOperatorTypeOne.lookupAndIterateOverPath(nodeLabel,gdb.nhf,gdb.btf_node);
						  }//for
					}//if present
			//}
		//TODO-task 2.3		
		//	PathExpressionOperatorTypeOne.iterateOverPath(sourceNID,sourceLabel,sourceDescriptor,hm,gdb);
=======
		System.out.println("commandLineInvocationVal" + commandLineInvocation);
>>>>>>> f9b842e69a9cf0b26b6ecfec9451ea8a0ade710b


		String inputArguments[] = commandLineInvocation.split("/");
		int length = inputArguments.length;
		System.out.println("Length" + length);
		attr = new AttrType[length];
		args = new Object[length];
		String inputArg = inputArguments[0];
		System.out.println("inputArg0:" + inputArg);
		String inputArg1[] = inputArg.split(" ");
		System.out.println("inputArg1.length" + inputArg1.length);
		if (inputArg1.length == 1) {
			nodeLabelQuery = true;
			nodeLabel = inputArg;
			attr[0] = new AttrType(0);
			sourceNID = PathExpressionUtil.lookupAndIterateOverPath(nodeLabel,
				gdb.nhf, gdb.btf_edge_label);
			args[0] = sourceNID;
		} else {
			nodeDescriptorQuery = true;
			nodeDescriptor = inputArg;
			sourceDesc = new Descriptor();
			insideArguments = nodeDescriptor.split(" ");
			System.out.println("insideArguments.length"
					+ insideArguments.length);
			int value1 = Integer.parseInt(insideArguments[0]);
			int value2 = Integer.parseInt(insideArguments[1]);
			int value3 = Integer.parseInt(insideArguments[2]);
			int value4 = Integer.parseInt(insideArguments[3]);
			int value5 = Integer.parseInt(insideArguments[4]);
			Descriptor desc = new Descriptor();
			sourceDesc.set(value1, value2, value3, value4, value5);

			 sourceNID=PathExpressionUtil.lookupAndIterateOverPath(sourceDesc,
			 gdb.nhf,gdb.btf_edge_label);

			attr[0] = new AttrType(5);
			args[0] = sourceNID;
		}
		if ((pathExpressionOperationType.equalsIgnoreCase("PQ1a"))
				|| (pathExpressionOperationType.equalsIgnoreCase("PQ2a"))
				|| (pathExpressionOperationType.equalsIgnoreCase("PQ3a"))) {
			for (int i = 1; i < length; i++) {
				//System.out.println("Input argument1:" + inputArguments[i]);
				String inputArgs = inputArguments[i];

				System.out.println("inputArgs:" + inputArgs);
				String inputArgs1[] = inputArgs.split(" ");
				//System.out.println("inputArgs1.length"+inputArgs1.length);
				
				//for(int i1=0; i1 <inputArgs1.length; i1++)
				//System.out.println("inputArgs" + inputArgs1[i1]);
				
				if( (inputArgs1.length == 1)&&(nodeLabelQuery)) {
					//nodeLabelQuery = true;
					nodeLabel = inputArgs;
					attr[i] = new AttrType(0);
					args[i] = inputArgs;
				} else {
					nodeDescriptorQuery = true;
					nodeDescriptor = inputArgs;
					attr[i] = new AttrType(5);
					insideArguments = nodeDescriptor.split(" ");
				//	System.out.println("insideArguments.length"
					//		+ insideArguments.length);
					int value1 = Integer.parseInt(insideArguments[0]);
					int value2 = Integer.parseInt(insideArguments[1]);
					int value3 = Integer.parseInt(insideArguments[2]);
					int value4 = Integer.parseInt(insideArguments[3]);
					int value5 = Integer.parseInt(insideArguments[4]);
					Descriptor desc = new Descriptor();
					desc.set(value1, value2, value3, value4, value5);

					args[i] = desc;
				}

			}// for
			 Tuple nodesInPath;
			 Tuple nodesInPathSorted;
			 Node nextNodeInPath;
			 String nextNodeInPathLabel;			 
		    
		     Set<String> set=new LinkedHashSet<String>();
			Iterator it=PathExpression.pathExpress1(args, attr,
					 gdb.nhf.get_fileName(), gdb.ehf.get_fileName(),
					 gdb.btf_edge_label.get_fileName(), nodeLabel, numbuf,
					 (short)32);
			if (pathExpressionOperationType.equalsIgnoreCase("PQ1a")) {
				// need to make this method static?
				 
				while((nodesInPath=it.get_next())!=null)
				{
					nodesInPath.setHdr((short) 1, new AttrType[] { new AttrType(
							AttrType.attrId) }, new short[] {});
					
					NID nextNID=(NID) nodesInPath.getIDFld(1);
					nextNodeInPath=gdb.nhf.getRecord(nextNID);
					
					if(nextNodeInPath != null){
					nextNodeInPathLabel=nextNodeInPath.getLabel();
					System.out.println("HeadNodeLabel"+nodeLabel);
					System.out.println("TailNodeLabel"+nextNodeInPathLabel);
					}
					
				}
				
			}
			if (pathExpressionOperationType.equalsIgnoreCase("PQ2a")) {

				
				AttrType[] type = new AttrType[1];
				type[0] = new AttrType(AttrType.attrString);
				short[] str_sizes=new short[1];
				str_sizes[0] = (short) 32;
				Iterator nodesInPathSortedIt = new Sort(type, (short) 2, str_sizes,
						it, 2, new TupleOrder(0), 32, numbuf);
				while((nodesInPathSorted=nodesInPathSortedIt.get_next())!=null)
				{
					
					nodesInPathSorted.setHdr((short) 1, new AttrType[] { new AttrType(
							AttrType.attrId) }, new short[] {});
					NID nextNID=(NID) nodesInPathSorted.getIDFld(1);
					nextNodeInPath=gdb.nhf.getRecord(nextNID);
					
					if(nextNodeInPath != null){
					nextNodeInPathLabel=nextNodeInPath.getLabel();
					System.out.println("HeadNodeLabel"+nodeLabel);
					System.out.println("TailNodeLabel"+nextNodeInPathLabel);
					}
				}

			}
			if (pathExpressionOperationType.equalsIgnoreCase("PQ3a")) {
				AttrType[] type = new AttrType[1];
				type[0] = new AttrType(AttrType.attrString);
				short[] str_sizes=new short[1];
				str_sizes[0] = (short) 32;
				Iterator nodesInPathSortedIt = new Sort(type, (short) 2, str_sizes,
						it, 2, new TupleOrder(0), 32, numbuf);
				while((nodesInPathSorted=nodesInPathSortedIt.get_next())!=null)
				{
					
					nodesInPathSorted.setHdr((short) 1, new AttrType[] { new AttrType(
							AttrType.attrId) }, new short[] {});
					NID nextNID=(NID) nodesInPathSorted.getIDFld(1);
					nextNodeInPath=gdb.nhf.getRecord(nextNID);
					if(nextNodeInPath != null){
					nextNodeInPathLabel=nextNodeInPath.getLabel();
				    set.add(nextNodeInPathLabel);
					}
				}
				while(set.iterator()!=null)
				{
					System.out.println("HeadNodeLabel"+nodeLabel);
					System.out.println("TailNodeLabel"+set.iterator().next());
				}
			}
		}

	}// method

}
