package batch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Scanner;

import tests.IndexNestedJoinTest;
import zindex.ZTreeFile;
import btree.BTreeFile;
import diskmgr.DB;
import diskmgr.GraphDB;
import diskmgr.PCounter;
import edgeheap.EdgeHeapFile;
import global.AttrType;
import global.Descriptor;
import global.NID;
import global.SystemDefs;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.NFileScan;
import iterator.RelSpec;
import iterator.Sort;
import iterator.TupleUtils;
import nodeheap.NScan;
import nodeheap.Node;
import nodeheap.NodeHeapfile;

public class BatchOperations {

	private static String taskName = "";
	private static String filePath = "";
	private static String graphDBName = "";
	private static int numBuf = 0;
	private static int qtype = 0;
	private static int index = 0;
	private static Descriptor targetDescriptor = null;
	private static String targetLabel;
	private static double distance = 0;
	private static int edgeWtBound1 = 0;
	private static int edgeWtBound2 = 0;
	private static short nodeLabelLength = 32;
	private static short edgeLabelLength = 32;
	static GraphDB gdb;
	public static String dbpath;
	public static String logpath;
	static HashSet<String> hs;

	public static void main(String[] args) throws Exception {

		Scanner sc= new Scanner(System.in);
		System.out.println("Enter Graph DB name:");
		graphDBName = sc.next();
		initGraphDB(graphDBName);
		GraphDB gdb = new GraphDB(0, graphDBName);

		/*
		 * Menu Driven Program (CUI for Batch operations) Enter the task name of
		 * your choice Enter the input file path Enter the GraphDB name Call the
		 * appropriate class methods according to the task number
		 */
		do {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			System.out.println("\n\nList of Operations");

			System.out.println("1) batchnodeinsert");
			System.out.println("2) batchedgeinsert");
			System.out.println("3) batchnodedelete");
			System.out.println("4) batchedgedelete");
			System.out.println("5) nodequery");
			System.out.println("6) edgequery");

			System.out
					.println("For Phase2:Enter the batch operation, the input file path and the name of the Graph Database in the following format");
			System.out.println("<task_name> <file_path> <GraphDB_name>");
			System.out.println("For Phase 3: Enter <task_name> as 'PathExpressionQuery'");

			String commandLineInvocation = br.readLine().trim();
			String inputArguments[] = commandLineInvocation.split(" ");

			taskName = inputArguments[0];
			int taskNumber = 0;

			if (taskName.equalsIgnoreCase("exit")) {
				System.exit(0);
			} else {

				System.out.println("Task Name: " + taskName);

				if (taskName.equalsIgnoreCase("batchnodeinsert"))
					taskNumber = 10;
				else if (taskName.equalsIgnoreCase("batchedgeinsert"))
					taskNumber = 11;
				else if (taskName.equalsIgnoreCase("batchnodedelete"))
					taskNumber = 12;
				else if (taskName.equalsIgnoreCase("batchedgedelete"))
					taskNumber = 13;
				else if (taskName.equalsIgnoreCase("nodequery"))
					taskNumber = 14;
				else if (taskName.equalsIgnoreCase("edgequery"))
					taskNumber = 15;
				else if (taskName.equalsIgnoreCase("PathExpressionQuery"))
					taskNumber = 23;

				if (taskNumber == 10 || taskNumber == 11 || taskNumber == 12
						|| taskNumber == 13) {

					// ---------------------------------
					filePath = inputArguments[1];
					String inputGraphDBName = inputArguments[2];
					while(!graphDBName.equals(inputGraphDBName)){
						System.out.println("Wrong graph db name enter graph db name again");
						System.out.println("Graph DB name: ");
						Scanner sc1 = new Scanner(System.in);
						inputGraphDBName = sc1.next();
					}
					
				} 
				else if (taskNumber == 14) {
					String inputGraphDBName = inputArguments[1];
					while(!graphDBName.equals(inputGraphDBName)){
						System.out.println("Wrong graph db name; enter graph db name again");
						System.out.println("Graph DB name: ");
						Scanner sc1 = new Scanner(System.in);
						inputGraphDBName = sc1.next();
					}	
					
					numBuf = Integer.parseInt(inputArguments[2]);
					qtype = Integer.parseInt(inputArguments[3]);
					index = Integer.parseInt(inputArguments[4]);

					System.out.println("GraphDB Name: " + graphDBName);
					System.out.println("Number of Buffers: " + numBuf);
					System.out.println("QType: " + qtype);
					System.out.println("Index: " + index);
					int d1, d2, d3, d4, d5;
					double dist;
					if (qtype == 2) {
						d1 = Integer.parseInt(inputArguments[5]);
						d2 = Integer.parseInt(inputArguments[6]);
						d3 = Integer.parseInt(inputArguments[7]);
						d4 = Integer.parseInt(inputArguments[8]);
						d5 = Integer.parseInt(inputArguments[9]);

						targetDescriptor = new Descriptor();
						targetDescriptor.set(d1, d2, d3, d4, d5);
					} else if (qtype == 3 || qtype == 5) {
						d1 = Integer.parseInt(inputArguments[5]);
						d2 = Integer.parseInt(inputArguments[6]);
						d3 = Integer.parseInt(inputArguments[7]);
						d4 = Integer.parseInt(inputArguments[8]);
						d5 = Integer.parseInt(inputArguments[9]);
						dist = Double.parseDouble(inputArguments[10]);
						targetDescriptor = new Descriptor();
						targetDescriptor.set(d1, d2, d3, d4, d5);
						distance = dist;
					} else if (qtype == 4) {
						targetLabel = inputArguments[5];
					}
				} 
				else if (taskNumber == 15) {
					String inputGraphDBName = inputArguments[1];
					while(!graphDBName.equals(inputGraphDBName)){
						System.out.println("Wrong graph db name; enter graph db name again");
						System.out.println("Graph DB name: ");
						Scanner sc1 = new Scanner(System.in);
						inputGraphDBName = sc1.next();
					}	
					numBuf = Integer.parseInt(inputArguments[2]);
					qtype = Integer.parseInt(inputArguments[3]);
					index = Integer.parseInt(inputArguments[4]);
					if (qtype == 5) {
						edgeWtBound1 = Integer.parseInt(inputArguments[5]);
						edgeWtBound2 = Integer.parseInt(inputArguments[6]);
					}
				}
				System.out.println("tasknumber" + taskNumber);

				
				switch (taskNumber) {

				// Task : Batch node insert
				case 10:
					try {
						String sCurrentLine;
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"+ graphDBName);
						EdgeHeapFile ehf =  new EdgeHeapFile("EdgeHeapFile_"+graphDBName);
						BTreeFile btf_node_label = new BTreeFile("IndNodeLabel_"+graphDBName);
						ZTreeFile ztf_node_desc = new ZTreeFile("zBTFile");
						
						br = new BufferedReader(new FileReader(filePath));
						while ((sCurrentLine = br.readLine()) != null) {

							BatchNodeInsert newNodeInsert = new BatchNodeInsert();		
							newNodeInsert.insertBatchNode(nhf, sCurrentLine);
						}
						System.out.println("Batch Nodes insertion done");
						
						// Insert records in Btree file
						gdb.createBTNodeLabel(nhf,btf_node_label);
						
						printStatistics(gdb,nhf,ehf);
						
						System.out.println("Node label BT craeted");
						btf_node_label.close();
												
						gdb.createZTFNodeDesc(nhf,ztf_node_desc);
						System.out.println("Node descriptor BT created");
						
						//ztf_node_desc.close();  //No close method for Z tree file
						
					
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				// Task : Batch Edge Insert
				case 11:
					try {
						BatchEdgeInsert newEdgeInsert = new BatchEdgeInsert();
						EdgeHeapFile ehf =  new EdgeHeapFile("EdgeHeapFile_"+graphDBName);
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"+ graphDBName);
						BTreeFile btf_node_label = new BTreeFile("IndNodeLabel_"+graphDBName);
						BTreeFile btf_edge_label = new BTreeFile("IndEdgeLabel_"+graphDBName);
						BTreeFile btf_edge_weight = new BTreeFile("IndEdgeWeight_"+graphDBName);
						
						newEdgeInsert.insertBatchEdge(ehf, nhf,btf_node_label, filePath);
						System.out.println("Batch edge insertion done");
						
						printStatistics(gdb,nhf,ehf);
						
						btf_node_label.close();
						
						gdb.createBTEdgeLabel(ehf,btf_edge_label);
						System.out.println("BTree on Edge Label Created");
						btf_edge_label.close();
						
						gdb.createBTEdgeWeight(ehf,btf_edge_weight);
						System.out.println("BTree on Edge Weight Created");
						btf_edge_weight.close();
						
						} 
					catch (Exception e) {
						e.printStackTrace();
					}
					break;

				// Task : Batch Node Delete
				case 12:
					try {
						BatchNodeDelete newNodeDelete = new BatchNodeDelete();
						
						EdgeHeapFile ehf =  new EdgeHeapFile("EdgeHeapFile_"+graphDBName);
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"+ graphDBName);
						BTreeFile btf_node_label = new BTreeFile("IndNodeLabel_"+graphDBName);
						BTreeFile btf_edge_label = new BTreeFile("IndEdgeLabel_"+graphDBName);
						BTreeFile btf_edge_weight = new BTreeFile("IndEdgeWeight_"+graphDBName);
						ZTreeFile ztf_node_desc = new ZTreeFile("zBTFile");
						
						newNodeDelete.deleteBatchNode(nhf, ehf,
								btf_node_label, ztf_node_desc,
								btf_edge_label, btf_edge_weight,
								filePath);
						
						printStatistics(gdb,nhf,ehf);
						
						//close all files
						btf_node_label.close();
						btf_edge_label.close();
						btf_edge_weight.close();
						//ztf_node_desc.close();     //No close method for Z tree fiel
						
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				// Task : Batch Edge Delete
				case 13:
					try {
						BatchEdgeDelete newEdgeDelete = new BatchEdgeDelete();	

						EdgeHeapFile ehf =  new EdgeHeapFile("EdgeHeapFile_"+graphDBName);
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"+ graphDBName);
						BTreeFile btf_node_label = new BTreeFile("IndNodeLabel_"+graphDBName);
						BTreeFile btf_edge_label = new BTreeFile("IndEdgeLabel_"+graphDBName);
						BTreeFile btf_edge_weight = new BTreeFile("IndEdgeWeight_"+graphDBName);
						
						newEdgeDelete.deleteBatchEdge(ehf, nhf,
								btf_node_label, btf_edge_label,
								btf_edge_weight, filePath);
						
						printStatistics(gdb,nhf,ehf);
						
						//close all files
						btf_node_label.close();
						btf_edge_label.close();
						btf_edge_weight.close();	
						
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				// Task : Node Query
				case 14:
					try {
						System.out.println("task 14");
						NodeQuery nq = new NodeQuery();
						NodeQueryWithIndex nqi = new NodeQueryWithIndex();
						
						EdgeHeapFile ehf = new EdgeHeapFile("EdgeHeapFile_"+graphDBName);
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"+ graphDBName);
						BTreeFile btf_node_label = new BTreeFile("IndNodeLabel_"+graphDBName);
						BTreeFile btf_edge_label = new BTreeFile("IndEdgeLabel_"+graphDBName);
						BTreeFile btf_edge_weight = new BTreeFile("IndEdgeWeight_"+graphDBName);
						ZTreeFile ztf_node_desc = new ZTreeFile("zBTFile");
						
						try {
							// heapfile scan
							if (index == 0) {

								if (qtype == 0) {
									nq.query0(nhf);
								} else if (qtype == 1) {
									nq.query1(nhf, nodeLabelLength,
											(short) numBuf);
								} else if (qtype == 2) {
									nq.query2(nhf, nodeLabelLength,
											(short) numBuf, targetDescriptor,
											distance);
								} else if (qtype == 3) {
									nq.query3(nhf, nodeLabelLength,
											(short) numBuf, targetDescriptor,
											distance);
								} else if (qtype == 4) {
									nq.query4(nhf, ehf, btf_node_label,
											nodeLabelLength, (short) numBuf,
											targetLabel);
								} else if (qtype == 5) {
									nq.query5(nhf, ehf, btf_node_label,
											nodeLabelLength, (short) numBuf,
											targetDescriptor, distance);
								}

							}
							// index scan
							else if (index == 1) {
								if (qtype == 0) {
									nqi.query0(nhf, btf_node_label,
											nodeLabelLength, (short) numBuf);
								} else if (qtype == 1) {
									nqi.query1(nhf, btf_node_label,
											nodeLabelLength, (short) numBuf);
								} else if (qtype == 2) {
									nqi.query2(nhf, ztf_node_desc,
											nodeLabelLength, (short) numBuf,
											targetDescriptor, distance);
								} else if (qtype == 3) {
									nqi.query3(nhf, ztf_node_desc,
											nodeLabelLength, (short) numBuf,
											targetDescriptor, distance);
								} else if (qtype == 4) {
									nqi.query4(nhf, btf_node_label, ehf,
											nodeLabelLength, (short) numBuf,
											targetLabel);
								} else if (qtype == 5) {
									nqi.query5(nhf, ztf_node_desc,
											btf_node_label, ehf, nodeLabelLength,
											(short) numBuf, targetDescriptor,
											distance);
								}
							}
							
							printStatistics(gdb,nhf,ehf);
							
							//close all files
							btf_node_label.close();
							btf_edge_label.close();
							btf_edge_weight.close();
							//ztf_node_desc.close();     //No close method for Z tree fiel
							
						} catch (Exception e) {
							e.printStackTrace();
						}
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					break;

				// Task : Edge Query
				case 15:
					try {
						System.out.println("Task 15");
						EdgeQuery eq = new EdgeQuery();
						EdgeQueryWithIndex eqi = new EdgeQueryWithIndex();
											
						EdgeHeapFile ehf =  new EdgeHeapFile("EdgeHeapFile_"+graphDBName);
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"+ graphDBName);
						BTreeFile btf_node_label = new BTreeFile("IndNodeLabel_"+graphDBName);
						BTreeFile btf_edge_label = new BTreeFile("IndEdgeLabel_"+graphDBName);
						BTreeFile btf_edge_weight = new BTreeFile("IndEdgeWeight_"+graphDBName);
						//ZTreeFile ztf_node_desc = new ZTreeFile("zBTFile");     //never used in this case
						
						try {
							// heapfile scan
							if (index == 0) {

								if (qtype == 0) {
									eq.query0(ehf, nhf);
								} else if (qtype == 1) {
									eq.query1(ehf, nhf, btf_node_label,
											nodeLabelLength, (short) numBuf);
								} else if (qtype == 2) {
									eq.query2(ehf, nhf, btf_node_label,
											nodeLabelLength, (short) numBuf);
								} else if (qtype == 3) {
									eq.query3(ehf, edgeLabelLength,
											(short) numBuf);
								} else if (qtype == 4) {
									eq.query4(ehf, edgeLabelLength,
											(short) numBuf);
								} else if (qtype == 5) {
									eq.query5(ehf, edgeLabelLength,
											(short) numBuf, edgeWtBound1,
											edgeWtBound2);
								} else if (qtype == 6) {
									eq.query6(ehf);
								} else if (qtype == 7) {
									eq.query7(ehf, (short) 32);
								}

							}
							// index scan
							else if (index == 1) {
								if (qtype == 0) {
									eqi.query0(ehf, btf_edge_label,
											nhf, edgeLabelLength,
											(short) numBuf);
								} else if (qtype == 1) {
									eqi.query1(ehf, btf_node_label, nhf,
											nodeLabelLength, (short) numBuf);
								} else if (qtype == 2) {
									eqi.query2(ehf, btf_node_label, nhf,
											nodeLabelLength, (short) numBuf);
								} else if (qtype == 3) {
									eqi.query3(ehf, btf_edge_label,
											edgeLabelLength, (short) numBuf);
								} else if (qtype == 4) {
									eqi.query4(ehf, btf_edge_weight,
											edgeLabelLength, (short) numBuf);
								} else if (qtype == 5) {
									eqi.query5(ehf, btf_edge_weight,
											edgeLabelLength, (short) numBuf,
											edgeWtBound1, edgeWtBound2);
								} else if (qtype == 6) {
									eqi.query6(ehf, btf_edge_label,
											nhf, edgeLabelLength,
											(short) numBuf);
								}
							} else if (index == 2) {
								IndexNestedJoinTest intest = new IndexNestedJoinTest();
								if (qtype == 0) {
									eqi.query0(ehf, btf_edge_label,
											nhf, edgeLabelLength,
											(short) numBuf);
								} else if (qtype == 1) {
									System.out.println("node_edge_source");
									intest.node_edge_source(ehf, nhf,
											nodeLabelLength, (short) numBuf);
								} else if (qtype == 2) {
									System.out.println("node_edge_destination");
									intest.node_edge_dest(ehf, nhf,
											nodeLabelLength, (short) numBuf);
								} else if (qtype == 3) {
									System.out.println("edge_node_source");
									intest.edge_node_source(ehf, nhf,
											btf_node_label, edgeLabelLength,
											(short) numBuf);
								} else if (qtype == 4) {
									System.out.println("edge_node_source");
									intest.edge_node_dest(ehf, nhf,
											btf_node_label, edgeLabelLength,
											(short) numBuf);
								} else if (qtype == 5) {
									eqi.query5(ehf, btf_edge_weight,
											edgeLabelLength, (short) numBuf,
											edgeWtBound1, edgeWtBound2);
								} else if (qtype == 6) {
									eqi.query6(ehf, btf_edge_label,
											nhf, edgeLabelLength,
											(short) numBuf);
								}
							}
							printStatistics(gdb,nhf,ehf);
							
							//close all files
							btf_node_label.close();
							btf_edge_label.close();
							btf_edge_weight.close();
							//ztf_node_desc.close();     //No close method for Z tree file
							
						} catch (Exception e) {
							e.printStackTrace();
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					break;
				case 23:
					System.out
							.println("Enter the path query expression in the following format");
					System.out
							.println("(Node Label|Node Descriptor)/(Node Label|Node Descriptor)/(Node Label|Node Descriptor)");

					String commandLineInvocation1 = br.readLine().trim();
					PathExpressionOperations
							.parsePathExpression(commandLineInvocation1);
					break;
				default:
					System.out.println("Error: unrecognized task number "
							+ taskName);
					break;

				}// switch
			} // else

			PCounter.initialize();

		} while (true);
	}// main

	public static void printStatistics(GraphDB newGDB, NodeHeapfile nhf, EdgeHeapFile ehf) throws Exception {
		int n = newGDB.getNodeCnt(nhf);
		System.out.println("NodeCount " + n);
		int n1 = newGDB.getEdgeCnt(ehf);
		System.out.println("EdgeCount " + n1);
		System.out.println("Number of pages read :" + PCounter.getRCounter());
		System.out.println("Number of pages written :" + PCounter.getWCounter());

	}

	public static void initGraphDB(String db_name) {
		
		dbpath = "/tmp/" + db_name + System.getProperty("user.name")
				+ ".minibase-db";
		logpath = "/tmp/" + db_name + System.getProperty("user.name")
				+ ".minibase-log";

		SystemDefs sysdef = new SystemDefs(dbpath, 10000, 500, "Clock");
		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent. We assume
		// user are on UNIX system here
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("IO error: " + e);
		}
		
//		
//		remove_logcmd = remove_cmd + newlogpath;
//		remove_dbcmd = remove_cmd + newdbpath;
//
//		try {
//			Runtime.getRuntime().exec(remove_logcmd);
//			Runtime.getRuntime().exec(remove_dbcmd);
//		} catch (IOException e) {
//			System.err.println("IO error: " + e);
//		}

	}
}