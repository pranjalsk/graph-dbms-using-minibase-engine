package batch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import zindex.ZTreeFile;
import btree.BTreeFile;

import Test_Phase2.QueryTest;

import diskmgr.DB;
import diskmgr.GraphDB;
import diskmgr.PCounter;
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
	private static double distance = 0;
	private static short nodeLabelLength = 32;
	static GraphDB gdb;
	public static String dbpath;
	public static String logpath;
	static HashSet<String> hs;
	

	
	public static void main(String[] args) throws Exception {
		
		hs = new HashSet<String>();
		
		/*
		 * Menu Driven Program (CUI for Batch operations) Enter the task name of
		 * your choice Enter the input file path Enter the GraphDB name Call the
		 * appropriate class methods according to the task number
		 */
		do {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			System.out.println("\n\nList of Batch Operations");
			System.out.println("1) batchnodeinsert");
			System.out.println("2) batchedgeinsert");
			System.out.println("3) batchnodedelete");
			System.out.println("4) batchedgedelete");
			System.out.println("5) nodequery");
			System.out.println("6) edgequery");

			System.out
					.println("Enter the batch operation, the input file path and the name of the Graph Database in the following format");
			System.out.println("<task_name> <file_path> <GraphDB_name>");

			String commandLineInvocation = br.readLine().trim();
			String inputArguments[] = commandLineInvocation.split(" ");

			taskName = inputArguments[0];
			int taskNumber = 0;

			if (inputArguments.length != 3) {
				if (taskName.equalsIgnoreCase("exit")) {
					System.out.println("Exiting out of the program");
					System.exit(0);

				} else
					System.out
							.println("Error: Invalid input,please add all the input parameters");
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


				//---------------------------------
				filePath = inputArguments[1];
				graphDBName = inputArguments[2];
				if (!hs.contains(graphDBName)) {
					hs.add(graphDBName);
					GraphDB.initGraphDB(graphDBName);	
					gdb = new GraphDB(0);
				}
				else{
					gdb.openDB(graphDBName);
				}
				
				
				if (taskNumber == 10 || taskNumber == 11 || taskNumber == 12
						|| taskNumber == 13) {
					
				} else if (taskNumber == 14) {
					NodeQuery nq = new NodeQuery();
					NodeQueryWithIndex nqi = new NodeQueryWithIndex();
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
					String label;
					if (qtype == 1){
						
						if(index == 0){

						}
						else if(index == 1){
							nqi.query0(newGDB.nhf, newGDB.btf_node, nodeLabelLength, (short)numBuf);
						}
						
					}
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
						label = inputArguments[5];
					}
				}

				switch (taskNumber) {

				// Task : Batch node insert
				case 10:
					try {
						String sCurrentLine;

						br = new BufferedReader(new FileReader(filePath));
						while ((sCurrentLine = br.readLine()) != null) {

							BatchNodeInsert newNodeInsert = new BatchNodeInsert();
							newNodeInsert
									.insertBatchNode(gdb.nhf, sCurrentLine);
						}
						System.out.println("Batch Nodes insertion done");
						printStatistics(gdb);
						gdb.createBTNodeLabel();
						System.out.println("Node label BT craeted");
						gdb.createZTFNodeDesc();
						System.out.println("Node descriptor BT created");
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				// Task : Batch Edge Insert
				case 11:
					try {
						BatchEdgeInsert newEdgeInsert = new BatchEdgeInsert();
						newEdgeInsert.insertBatchEdge(gdb.ehf, gdb.nhf,
								filePath);
						System.out.println("Batch edge insertion done");
						gdb.createBTEdgeLabel();
						System.out.println("BTree on Edge Label Created");
						gdb.createBTEdgeWeight();
						System.out.println("BTree on Edge Weight Created");
						printStatistics(gdb);
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				// Task : Batch Node Delete
				case 12:
					try {
						BatchNodeDelete newNodeDelete = new BatchNodeDelete();
						newNodeDelete.deleteBatchNode(gdb.nhf, gdb.ehf,
								gdb.btf_node, gdb.ztf_node_desc,
								gdb.btf_edge_label, gdb.btf_edge_weight,
								filePath);
						printStatistics(gdb);
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				// Task : Batch Edge Delete
				case 13:
					try {
						BatchEdgeDelete newEdgeDelete = new BatchEdgeDelete();
						newEdgeDelete.deleteBatchEdge(gdb.ehf, gdb.nhf,
								gdb.btf_edge_label, gdb.btf_edge_weight,
								filePath);
						printStatistics(gdb);
						System.out.println(gdb.getEdgeCnt());
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				// Task : Node Query
				case 14:
					try {
						NodeQuery nq = new NodeQuery();
						// heapfile scan
						if (index == 0) {

							if (qtype == 0) {
								System.out.println("qtype0");
								NodeHeapfile nhf = gdb.nhf;
								nq.query0(nhf);
							} else if (qtype == 1) {

							} else if (qtype == 2) {

							} else if (qtype == 3) {

							} else if (qtype == 4) {

							} else if (qtype == 5) {

							}

						}
						// index scan
						else if (index == 1) {

						}

					} catch (Exception e) {
						e.printStackTrace();
					}
					break;

				// Task : Edge Query
				case 15:
					try {

					} catch (Exception e) {

					}
					break;
				default:
					System.out.println("Error: unrecognized task number "
							+ taskName);
					break;

				}// switch
			} // else

		} while (true);
	}// main

	public static void printStatistics(GraphDB newGDB) throws Exception {
		int n = newGDB.getNodeCnt();
		System.out.println("NodeCount " + n);
		int n1 = newGDB.getEdgeCnt();
		System.out.println("EdgeCount " + n1);
		PCounter pCount = new PCounter();
		System.out.println("Number of pages read :" + pCount.getRCounter());
		System.out.println("Number of pages written :" + pCount.getWCounter());

	}

	public static void scanNodeHeapFile() throws InvalidTupleSizeException,
			IOException, InvalidTypeException, FieldNumberOutOfBoundException {
		// scanning of records
		NID newNid = new NID();
		NScan newNscan = gdb.nhf.openScan();
		Node newNode = new Node();
		boolean done = false;

		while (!done) {
			newNode = newNscan.getNext(newNid);
			if (newNode == null) {
				done = true;
				break;
			}
			newNode.setHdr();
			String nodeLabel = newNode.getLabel();
			System.out.println(nodeLabel);
			for (int j = 0; j < 5; j++) {
				System.out.print(newNode.getDesc().get(j));

			}
		}
		newNscan.closescan();
		System.out.println("test done");
	}

}
