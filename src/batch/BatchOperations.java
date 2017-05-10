package batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import global.PageId;
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
	private static String pathexp = "";
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

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		/*
		 * Menu Driven Program (CUI for Batch operations)
		 * Enter the task name of your choice
		 * Enter the input file path
		 * Enter the GraphDB name
		 * Call the appropriate class methods according to the task number
		 */
		do {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			System.out.println("\n\t\tList of Operations");
			System.out.println("--------------------------------------------------------------------");
			System.out.println("1) batchnodeinsert <file_path> <GraphDB_name>");
			System.out.println("2) batchedgeinsert <file_path> <GraphDB_name>");
			System.out.println("3) batchnodedelete <file_path> <GraphDB_name>");
			System.out.println("4) batchedgedelete <file_path> <GraphDB_name>");
			System.out.println("5) nodequery <GraphDB_name> <buffersize> <qtype> <index>");
			System.out.println("6) edgequery <GraphDB_name> <buffersize> <qtype> <index>");
			System.out.println("7) pathquery <GraphDB_name> <buffersize> <PathExpressionString>");
			System.out.println("--------------------------------------------------------------------");
			
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
				else if (taskName.equalsIgnoreCase("pathquery"))
					taskNumber = 32;

				if (taskNumber == 10 || taskNumber == 11 || taskNumber == 12
						|| taskNumber == 13) {

					// ---------------------------------
					filePath = inputArguments[1];
					graphDBName = inputArguments[2];					

				} 
				else if (taskNumber == 14) {				
					graphDBName = inputArguments[1];
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
					} 
					else if (qtype == 3 || qtype == 5) {
						d1 = Integer.parseInt(inputArguments[5]);
						d2 = Integer.parseInt(inputArguments[6]);
						d3 = Integer.parseInt(inputArguments[7]);
						d4 = Integer.parseInt(inputArguments[8]);
						d5 = Integer.parseInt(inputArguments[9]);
						dist = Double.parseDouble(inputArguments[10]);
						targetDescriptor = new Descriptor();
						targetDescriptor.set(d1, d2, d3, d4, d5);
						distance = dist;
					} 
					else if (qtype == 4) {
						targetLabel = inputArguments[5];
					}
				} 
				
				else if (taskNumber == 15) {
					graphDBName = inputArguments[1];
					numBuf = Integer.parseInt(inputArguments[2]);
					qtype = Integer.parseInt(inputArguments[3]);
					index = Integer.parseInt(inputArguments[4]);
					if (qtype == 5) {
						edgeWtBound1 = Integer.parseInt(inputArguments[5]);
						edgeWtBound2 = Integer.parseInt(inputArguments[6]);
					}
				} 
				
				else if (taskNumber == 32) {
					
					List<String> list = new ArrayList<String>();
					Matcher m1 = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(commandLineInvocation);
					while (m1.find())
					    list.add(m1.group(1)); 			

					graphDBName = list.get(1);
					numBuf = Integer.parseInt(list.get(2));
					pathexp = list.get(3).substring(1, list.get(3).length()-1); // to remove surrounding quotes.

					Pattern p = Pattern.compile("([PT]Q)(\\d)?(\\w)\\s?[:>]\\s?(.*)");
					Matcher m = p.matcher(pathexp);

					while (m.find()) {
						// capture "1" "2" "3" info
						if (m.group(1).equals("TQ")) {
							qtype = 4;
						} else {
							qtype = m.group(2).equals("1") ? 1 : m.group(2)
									.equals("2") ? 2 : 3;
						}
					}

				}
				gdb = new GraphDB(0, graphDBName);
				//System.out.println(SystemDefs.MINIBASE_RESTART_FLAG);
				System.out.println("tasknumber" + taskNumber);

				switch (taskNumber) {

				// Task : Batch node insert
				case 10:
					try {
						String sCurrentLine;
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"
								+ graphDBName);
						EdgeHeapFile ehf = new EdgeHeapFile("EdgeHeapFile_"
								+ graphDBName);
						BTreeFile btf_node_label = new BTreeFile(
								"IndNodeLabel_" + graphDBName);
						ZTreeFile ztf_node_desc = new ZTreeFile("zBTFile");

						br = new BufferedReader(new FileReader(filePath));
						while ((sCurrentLine = br.readLine()) != null) {

							BatchNodeInsert newNodeInsert = new BatchNodeInsert();
							newNodeInsert.insertBatchNode(nhf, sCurrentLine);
						}
						System.out.println("Batch Nodes insertion done");

						// Insert records in Btree file
						gdb.createBTNodeLabel(nhf, btf_node_label);

						System.out.println("Node label BT craeted");
						btf_node_label.close();

						gdb.createZTFNodeDesc(nhf, ztf_node_desc);
						System.out.println("Node descriptor BT created");

						ztf_node_desc.close();

						printStatistics(gdb, nhf, ehf);
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				// Task : Batch Edge Insert
				case 11:
					try {
						BatchEdgeInsert newEdgeInsert = new BatchEdgeInsert();
						EdgeHeapFile ehf = new EdgeHeapFile("EdgeHeapFile_"
								+ graphDBName);
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"
								+ graphDBName);
						BTreeFile btf_node_label = new BTreeFile(
								"IndNodeLabel_" + graphDBName);
						BTreeFile btf_edge_label = new BTreeFile(
								"IndEdgeLabel_" + graphDBName);
						BTreeFile btf_edge_weight = new BTreeFile(
								"IndEdgeWeight_" + graphDBName);
						BTreeFile btf_edge_src_label = new BTreeFile(
								"IndEdgeSrcLabel_" + graphDBName);
						BTreeFile btf_edge_dest_label = new BTreeFile(
								"IndEdgeDestLabel_" + graphDBName);

						newEdgeInsert.insertBatchEdge(ehf, nhf, btf_node_label,
								filePath);
						System.out.println("Batch edge insertion done");


						btf_node_label.close();

						gdb.createBTEdgeLabel(ehf, btf_edge_label);
						System.out.println("BTree on Edge Label Created");
						btf_edge_label.close();

						gdb.createBTEdgeWeight(ehf, btf_edge_weight);
						System.out.println("BTree on Edge Weight Created");
						btf_edge_weight.close();

						gdb.createBTEdgeSrcLabel(ehf, btf_edge_src_label);
						btf_edge_src_label.close();

						gdb.createBTEdgeDestLabel(ehf, btf_edge_dest_label);
						btf_edge_dest_label.close();

						printStatistics(gdb, nhf, ehf);
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				// Task : Batch Node Delete
				case 12:
					try {
						BatchNodeDelete newNodeDelete = new BatchNodeDelete();

						EdgeHeapFile ehf = new EdgeHeapFile("EdgeHeapFile_"
								+ graphDBName);
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"
								+ graphDBName);
						BTreeFile btf_node_label = new BTreeFile(
								"IndNodeLabel_" + graphDBName);
						BTreeFile btf_edge_label = new BTreeFile(
								"IndEdgeLabel_" + graphDBName);
						BTreeFile btf_edge_weight = new BTreeFile(
								"IndEdgeWeight_" + graphDBName);
						ZTreeFile ztf_node_desc = new ZTreeFile("zBTFile");
						BTreeFile btf_edge_src_label = new BTreeFile(
								"IndEdgeSrcLabel_" + graphDBName);
						BTreeFile btf_edge_dest_label = new BTreeFile(
								"IndEdgeDestLabel_" + graphDBName);

						newNodeDelete.deleteBatchNode(nhf, ehf, btf_node_label,
								ztf_node_desc, btf_edge_label, btf_edge_weight,
								btf_edge_src_label, btf_edge_dest_label,
								filePath);


						// close all files
						btf_node_label.close();
						btf_edge_label.close();
						btf_edge_weight.close();
						ztf_node_desc.close();
						btf_edge_src_label.close();
						btf_edge_dest_label.close();
						printStatistics(gdb, nhf, ehf);
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				// Task : Batch Edge Delete
				case 13:
					try {
						BatchEdgeDelete newEdgeDelete = new BatchEdgeDelete();

						EdgeHeapFile ehf = new EdgeHeapFile("EdgeHeapFile_"
								+ graphDBName);
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"
								+ graphDBName);
						BTreeFile btf_node_label = new BTreeFile(
								"IndNodeLabel_" + graphDBName);
						BTreeFile btf_edge_label = new BTreeFile(
								"IndEdgeLabel_" + graphDBName);
						BTreeFile btf_edge_weight = new BTreeFile(
								"IndEdgeWeight_" + graphDBName);
						BTreeFile btf_edge_src_label = new BTreeFile(
								"IndEdgeSrcLabel_" + graphDBName);
						BTreeFile btf_edge_dest_label = new BTreeFile(
								"IndEdgeDestLabel_" + graphDBName);

						newEdgeDelete.deleteBatchEdge(ehf, nhf, btf_node_label,
								btf_edge_label, btf_edge_weight,btf_edge_src_label, btf_edge_dest_label, filePath);

						// close all files
						btf_node_label.close();
						btf_edge_label.close();
						btf_edge_weight.close();
						btf_edge_src_label.close();
						btf_edge_dest_label.close();

						printStatistics(gdb, nhf, ehf);
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

						EdgeHeapFile ehf = new EdgeHeapFile("EdgeHeapFile_"
								+ graphDBName);
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"
								+ graphDBName);
						BTreeFile btf_node_label = new BTreeFile(
								"IndNodeLabel_" + graphDBName);
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
											btf_node_label, ehf,
											nodeLabelLength, (short) numBuf,
											targetDescriptor, distance);
								}
							}

							

							
						} catch (Exception e) {
							e.printStackTrace();
						}
						// close all files
						btf_node_label.close();
						ztf_node_desc.close();
						printStatistics(gdb, nhf, ehf);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
					break;

				// Task : Edge Query
				case 15:
					try {
						System.out.println("Task 15");
						EdgeQuery eq = new EdgeQuery();
						EdgeQueryWithIndex eqi = new EdgeQueryWithIndex();

						EdgeHeapFile ehf = new EdgeHeapFile("EdgeHeapFile_"
								+ graphDBName);
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"
								+ graphDBName);
						BTreeFile btf_node_label = new BTreeFile(
								"IndNodeLabel_" + graphDBName);
						BTreeFile btf_edge_label = new BTreeFile(
								"IndEdgeLabel_" + graphDBName);
						BTreeFile btf_edge_weight = new BTreeFile(
								"IndEdgeWeight_" + graphDBName);
						BTreeFile btf_edge_dest_label = new BTreeFile(
								"IndEdgeDestLabel_" + graphDBName);
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
									eq.query6(ehf,(short) numBuf);
								} else if(qtype == 7){
									eq.query7(gdb.ehf, (short) 32,(short) numBuf);	//Sort Merge Join
	  							}

							}
							// index scan
							else if (index == 1) {
								if (qtype == 0) {
									eqi.query0(ehf, btf_edge_label, nhf,
											edgeLabelLength, (short) numBuf);
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
									eqi.query6(ehf, btf_edge_dest_label, nhf,
											edgeLabelLength, (short) numBuf);
								}
							}

							// close all files
							btf_node_label.close();
							btf_edge_label.close();
							btf_edge_weight.close();
							btf_edge_dest_label.close();

							printStatistics(gdb, nhf, ehf);
						} catch (Exception e) {
							e.printStackTrace();
						}

					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				case 32:
					try {
						System.out.println("Task Path Exp Queries");
						PathExpressionQuery pq = new PathExpressionQuery();

						EdgeHeapFile ehf = new EdgeHeapFile("EdgeHeapFile_"
								+ graphDBName);
						NodeHeapfile nhf = new NodeHeapfile("NodeHeapFile_"
								+ graphDBName);
						BTreeFile btf_node_label = new BTreeFile(
								"IndNodeLabel_" + graphDBName);
						ZTreeFile ztf_node_desc = new ZTreeFile("zBTFile");
						BTreeFile btf_edge_src_label = new BTreeFile(
								"IndEdgeSrcLabel_" + graphDBName);

						if (qtype == 1) {

							pq.pathExpressQuery1(pathexp, nhf, ehf,
									btf_edge_src_label, btf_node_label,
									ztf_node_desc, (short) numBuf,
									nodeLabelLength);
						} else if (qtype == 2) {

							pq.pathExpressQuery2(pathexp, nhf, ehf,
									btf_edge_src_label, btf_node_label,
									ztf_node_desc, (short) numBuf,
									nodeLabelLength);

						} else if (qtype == 3) {
							
							pq.pathExpressQuery3(pathexp, nhf, ehf,
									btf_edge_src_label, btf_node_label,
									ztf_node_desc, (short) numBuf,
									nodeLabelLength);							

						} else if (qtype == 4) {

							pq.triangleQuery(pathexp, nhf.get_fileName(),
									ehf.get_fileName(),
									btf_edge_src_label.get_fileName(),
									btf_node_label.get_fileName(),
									(short) numBuf, nodeLabelLength);
						}


						// close all files
						btf_node_label.close();
						btf_edge_src_label.close();
						ztf_node_desc.close();

						printStatistics(gdb, nhf, ehf);
					} catch (Exception e) {
						e.printStackTrace();
					}

					break;

				default:
					System.out.println("Error: unrecognized task number "
							+ taskName);
					break;

				}// switch
			} // else

			PCounter.initialize();
			gdb.sysdef.JavabaseBM.flushAllPages();
		} while (true);
	}// main

	/**
	 * @param newGDB
	 * @param nhf
	 * @param ehf
	 * @throws Exception
	 */
	//Prints Nodecount and Edgecount
	public static void printStatistics(GraphDB newGDB, NodeHeapfile nhf,
			EdgeHeapFile ehf) throws Exception {
		int n = newGDB.getNodeCnt(nhf);
		System.out.println("NodeCount " + n);
		int n1 = newGDB.getEdgeCnt(ehf);
		System.out.println("EdgeCount " + n1);
		System.out.println("Number of pages read :" + PCounter.getRCounter());
		System.out
				.println("Number of pages written :" + PCounter.getWCounter());

	}
}