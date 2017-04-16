package batch;

import global.AttrType;
import global.Descriptor;
import global.NID;
import iterator.Iterator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

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
			String graphDBName, String pathExpressionOperationType)
			throws Exception {

		hs = new HashSet<String>();

		/*
		 * Menu Driven Program (CUI for Batch operations) Enter the task name of
		 * your choice Enter the input file path Enter the GraphDB name Call the
		 * appropriate class methods according to the task number
		 */
		System.out.println("commandLineInvocation" + commandLineInvocation);
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		if (!hs.contains(graphDBName)) {
			hs.add(graphDBName);
			//GraphDB.initGraphDB(graphDBName);
			//gdb = new GraphDB(0);
		} else {
			//gdb.openDB(graphDBName);
		}

		String inputArguments[] = commandLineInvocation.split("/");
		int length = inputArguments.length;
		//System.out.println("Length" + length);
		attr = new AttrType[length];
		args = new Object[length];
		String inputArg = inputArguments[0];
		//System.out.println("inputArg:" + inputArg);
		String inputArg1[] = inputArg.split(" ");
		//System.out.println("inputArg1.length" + inputArg1.length);
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
				//System.out.println("Input argument:" + inputArguments[i]);
				String inputArgs = inputArguments[i];

				//System.out.println("inputArgs:" + inputArgs);
				String inputArgs1[] = inputArgs.split(" ");
				//System.out.println("inputArgs1.length" + inputArgs1.length);
				if (inputArgs1.length == 1) {
					nodeLabelQuery = true;
					nodeLabel = inputArgs;
					attr[i] = new AttrType(0);
					args[i] = inputArgs;
				} else {
					nodeDescriptorQuery = true;
					nodeDescriptor = inputArgs;
					attr[i] = new AttrType(5);
					insideArguments = nodeDescriptor.split(" ");
					//System.out.println("insideArguments.length"
						//	+ insideArguments.length);
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

			if (pathExpressionOperationType.equalsIgnoreCase("PQ1a")) {
				// need to make this method static?
				 Iterator it=PathExpression.pathExpress1(args, attr,
				 gdb.nhf.get_fileName(), gdb.ehf.get_fileName(),
				 gdb.btf_edge_label.get_fileName(), nodeLabel, numbuf,
				 (short)32);
			}
			if (pathExpressionOperationType.equalsIgnoreCase("PQ2a")) {

			}
			if (pathExpressionOperationType.equalsIgnoreCase("PQ3a")) {

			}
		}

	}// method

}
