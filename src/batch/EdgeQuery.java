package batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import btree.BTreeFile;
import bufmgr.PageNotReadException;

import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.EdgeHeapFile;
import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.EID;
import global.NID;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import iterator.CondExpr;
import iterator.EFileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NFileScan;
import iterator.NestedLoopException;
import iterator.NestedLoopsJoins;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.SortMerge;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import nodeheap.NScan;
import nodeheap.Node;
import nodeheap.NodeHeapfile;

public class EdgeQuery {
	/**
	 * Prints Edge data on the order of occurrence/storage in Edge Heap File
	 * 
	 * @param ehf
	 *            Edge Heap File
	 * @param nhf
	 *            Node Heap File
	 */
	public void query0(EdgeHeapFile ehf, NodeHeapfile nhf) {
		EID eid = new EID();
		Edge edge;
		try {

			NID sourceNID, destinationNID;
			EScan escan = ehf.openScan();
			edge = escan.getNext(eid);
			String edgeLabel;
			int edgeWeight;

			while (edge != null) {
				edge.setHdr();
				edgeLabel = edge.getLabel();
				edgeWeight = edge.getWeight();
				sourceNID = edge.getSource();
				destinationNID = edge.getDestination();
				Node sourceNode = null, destinationNode = null;
				sourceNode = nhf.getRecord(sourceNID);
				sourceNode.setHdr();
				destinationNode = nhf.getRecord(destinationNID);
				destinationNode.setHdr();

				System.out.print("Label: " + edgeLabel + " , Weight:"
						+ edgeWeight + " , ");
				if (sourceNode != null)
					System.out.print("Source Node: " + sourceNode.getLabel()
							+ " , ");
				if (destinationNode != null)
					System.out.print("Destination Node: "
							+ destinationNode.getLabel());
				System.out.println();

				edge = escan.getNext(eid);
			}
			String queryPlan = "(Pi(edge.label, edge.weight, edge.source, edge.dest) (EdgeHeapFile))\n";
			System.out.println(queryPlan);
			escan.closescan();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Prints Edge data based on increasing alpha-numerical order of label of
	 * Source Node
	 * 
	 * @param ehf
	 *            Edge Heap File
	 * @param nhf
	 *            Node Heap File
	 */
	public void query1(EdgeHeapFile ehf, NodeHeapfile nhf,
			BTreeFile btf_node_label, short nodeLabelLength, short numBuf) {

		String nodeHeapFileName = nhf.get_fileName();
		AttrType[] attrType = new AttrType[2];
		short[] stringSize = new short[1];
		stringSize[0] = nodeLabelLength;
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrDesc);
		FldSpec[] projlist = new FldSpec[2];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);

		TupleOrder[] order = new TupleOrder[2];
		order[0] = new TupleOrder(TupleOrder.Ascending);
		order[1] = new TupleOrder(TupleOrder.Descending);

		Node node = new Node();
		try {
			NFileScan nfscan = new NFileScan(nodeHeapFileName, attrType,
					stringSize, (short) 2, 2, projlist, null);
			Sort sort = new Sort(attrType, (short) 2, stringSize, nfscan, 1,
					order[0], nodeLabelLength, numBuf);

			String nodeLabel;
			Tuple t;
			t = sort.get_next();

			while (t != null) {
				node.nodeInit(t.getTupleByteArray(), t.getOffset());
				node.setHdr();
				nodeLabel = node.getLabel();
				BatchMapperClass bInsert = new BatchMapperClass();
				NID nodeNID = bInsert.getNidFromNodeLabel(nodeLabel, nhf,
						btf_node_label);

				EID eid = new EID();
				Edge edge;
				try {

					NID sourceNID;
					EScan escan = ehf.openScan();
					edge = escan.getNext(eid);
					while (edge != null) {
						edge.setHdr();
						sourceNID = edge.getSource();

						if (nodeNID.equals(sourceNID)) {
							System.out.println("Label: " + edge.getLabel()
									+ " , Weight: " + edge.getWeight()
									+ " , Source Node Label: "
									+ node.getLabel());
						}

						edge = escan.getNext(eid);
					}
					escan.closescan();

				} catch (Exception e) {
					e.printStackTrace();
				}

				t = sort.get_next();
			}
			nfscan.close();
			sort.close();
			String queryPlan = "\n(Pi(edge.label, edge.weight, edge.source)(Sigma(node.label == edge.source)((Sort - node.label(Pi(node.label, node.descriptor) " +
					"(NodeHeapFile)))  |><| EdgeHeapFile)))\n";
			System.out.println(queryPlan);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Prints Edge data based on increasing alpha-numerical order of label of
	 * Destination Node
	 * 
	 * @param ehf
	 *            Edge Heap File
	 * @param nhf
	 *            Node Heap File
	 */
	public void query2(EdgeHeapFile ehf, NodeHeapfile nhf,
			BTreeFile btf_node_label, short nodeLabelLength, short numBuf) {

		String nodeHeapFileName = nhf.get_fileName();
		AttrType[] attrType = new AttrType[2];
		short[] stringSize = new short[1];
		stringSize[0] = nodeLabelLength;
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrDesc);
		FldSpec[] projlist = new FldSpec[2];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);

		TupleOrder[] order = new TupleOrder[2];
		order[0] = new TupleOrder(TupleOrder.Ascending);
		order[1] = new TupleOrder(TupleOrder.Descending);

		Node node = new Node();
		try {
			NFileScan nfscan = new NFileScan(nodeHeapFileName, attrType,
					stringSize, (short) 2, 2, projlist, null);
			Sort sort = new Sort(attrType, (short) 2, stringSize, nfscan, 1,
					order[0], nodeLabelLength, numBuf);

			String nodeLabel;
			Tuple t;
			t = sort.get_next();

			while (t != null) {
				node.nodeInit(t.getTupleByteArray(), t.getOffset());
				node.setHdr();
				nodeLabel = node.getLabel();
				BatchMapperClass bInsert = new BatchMapperClass();
				NID nodeNID = bInsert.getNidFromNodeLabel(nodeLabel, nhf,
						btf_node_label);

				EID eid = new EID();
				Edge edge;
				try {

					NID destNID;
					EScan escan = ehf.openScan();
					edge = escan.getNext(eid);
					while (edge != null) {
						edge.setHdr();
						destNID = edge.getDestination();

						if (nodeNID.equals(destNID)) {
							System.out.println("Label: " + edge.getLabel()
									+ " , Weight: " + edge.getWeight()
									+ " , Destination Node Label: "
									+ node.getLabel());
						}

						edge = escan.getNext(eid);
					}
					escan.closescan();

				} catch (Exception e) {
					e.printStackTrace();
				}

				t = sort.get_next();
			}
			nfscan.close();
			sort.close();
			String queryPlan = "\n(Pi(edge.label, edge.weight, edge.dest)(Sigma(node.label == edge.dest)((Sort - node.label(Pi(node.label, node.descriptor) " +
					"(NodeHeapFile)))  |><| EdgeHeapFile)))\n";
			System.out.println(queryPlan);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Prints Edge data based on increasing alpha-numerical order of Edge Label
	 * 
	 * @param ehf
	 *            Edge Heap File
	 * @param edgeLabelLength
	 *            Length of Edge Label
	 * @param numBuf
	 *            Number of Buffers
	 */
	public void query3(EdgeHeapFile ehf, short edgeLabelLength, short numBuf) {

		String edgeHeapFileName = ehf.get_fileName();
		AttrType[] attrType = new AttrType[8];
		short[] stringSize = new short[3];
		stringSize[0] = edgeLabelLength;
		stringSize[1] = edgeLabelLength;
		stringSize[2] = edgeLabelLength;
		attrType[0] = new AttrType(AttrType.attrInteger);
		attrType[1] = new AttrType(AttrType.attrInteger);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrInteger);
		attrType[4] = new AttrType(AttrType.attrString);
		attrType[5] = new AttrType(AttrType.attrInteger);
		attrType[6] = new AttrType(AttrType.attrString);
		attrType[7] = new AttrType(AttrType.attrString);

		FldSpec[] projlist = new FldSpec[8];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);
		projlist[3] = new FldSpec(rel, 4);
		projlist[4] = new FldSpec(rel, 5);
		projlist[5] = new FldSpec(rel, 6);
		projlist[6] = new FldSpec(rel, 7);
		projlist[7] = new FldSpec(rel, 8);

		TupleOrder[] order = new TupleOrder[2];
		order[0] = new TupleOrder(TupleOrder.Ascending);
		order[1] = new TupleOrder(TupleOrder.Descending);

		Edge edge = new Edge();
		try {
			EFileScan efscan = new EFileScan(edgeHeapFileName, attrType,
					stringSize, (short) 8, 8, projlist, null);
			Sort sort = new Sort(attrType, (short) 8, stringSize, efscan, 5,
					order[0], edgeLabelLength, numBuf);

			String edgeLabel, srcLabel, destLabel;
			int sourceNodePageID, sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;

			Tuple t;
			t = sort.get_next();

			while (t != null) {
				edge.edgeInit(t.getTupleByteArray(), t.getOffset());
				edge.setHdr();
				edgeLabel = "";
				edgeLabel = edge.getLabel();
				sourceNodePageID = edge.getSource().pageNo.pid;
				sourceNodeSlotID = edge.getSource().slotNo;
				destinationNodePageID = edge.getDestination().pageNo.pid;
				destinationNodeSlotID = edge.getDestination().slotNo;
				edgeWeight = edge.getWeight();
				srcLabel = edge.getSourceLabel();
				destLabel = edge.getDestLabel();

				System.out.println("Label: " + edgeLabel + " , Weight : "
						+ edgeWeight + "Source Node PageID: "
						+ sourceNodePageID + " , Source Node SlotID: "
						+ sourceNodeSlotID + " , Destination Node PageID: "
						+ destinationNodePageID
						+ " , Destination Node SlotID: "
						+ destinationNodeSlotID);
				t = sort.get_next();
			}
			efscan.close();
			sort.close();
			String queryPlan = "\nSort - edge.label(Pi(edge.label, edge.weight, edge.source, edge.dest) " +
					"(EdgeHeapFile))\n";
			System.out.println(queryPlan);
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	/**
	 * Prints Edge data based on increasing numerical order of Edge Weights
	 * 
	 * @param ehf
	 *            Edge Heap File
	 * @param edgeLabelLength
	 *            Length of Edge Label
	 * @param numBuf
	 *            Number of Buffers
	 */
	public void query4(EdgeHeapFile ehf, short edgeLabelLength, short numBuf) {

		String edgeHeapFileName = ehf.get_fileName();
		AttrType[] attrType = new AttrType[8];
		short[] stringSize = new short[3];
		stringSize[0] = edgeLabelLength;
		stringSize[1] = edgeLabelLength;
		stringSize[2] = edgeLabelLength;
		attrType[0] = new AttrType(AttrType.attrInteger);
		attrType[1] = new AttrType(AttrType.attrInteger);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrInteger);
		attrType[4] = new AttrType(AttrType.attrString);
		attrType[5] = new AttrType(AttrType.attrInteger);
		attrType[6] = new AttrType(AttrType.attrString);
		attrType[7] = new AttrType(AttrType.attrString);

		FldSpec[] projlist = new FldSpec[8];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);
		projlist[3] = new FldSpec(rel, 4);
		projlist[4] = new FldSpec(rel, 5);
		projlist[5] = new FldSpec(rel, 6);
		projlist[6] = new FldSpec(rel, 7);
		projlist[7] = new FldSpec(rel, 8);

		TupleOrder[] order = new TupleOrder[2];
		order[0] = new TupleOrder(TupleOrder.Ascending);
		order[1] = new TupleOrder(TupleOrder.Descending);

		Edge edge = new Edge();
		try {
			EFileScan efscan = new EFileScan(edgeHeapFileName, attrType,
					stringSize, (short) 8, 8, projlist, null);
			Sort sort = new Sort(attrType, (short) 8, stringSize, efscan, 6,
					order[0], 4, numBuf);

			String edgeLabel, edgeSource, edgeDest;
			int sourceNodePageID, sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;

			Tuple t;
			t = sort.get_next();

			while (t != null) {
				edge.edgeInit(t.getTupleByteArray(), t.getOffset());
				edge.setHdr();
				edgeLabel = edge.getLabel();
				sourceNodePageID = edge.getSource().pageNo.pid;
				sourceNodeSlotID = edge.getSource().slotNo;
				destinationNodePageID = edge.getDestination().pageNo.pid;
				destinationNodeSlotID = edge.getDestination().slotNo;
				edgeWeight = edge.getWeight();
				edgeSource = edge.getSourceLabel();
				edgeDest = edge.getDestLabel();

				System.out.println("Label: " + edgeLabel + " , Weight : "
						+ edgeWeight + "Source Node PageID: "
						+ sourceNodePageID + " , Source Node SlotID: "
						+ sourceNodeSlotID + " , Destination Node PageID: "
						+ destinationNodePageID
						+ " , Destination Node SlotID: "
						+ destinationNodeSlotID);
				t = sort.get_next();
			}
			sort.close();
			efscan.close();
			String queryPlan = "\nSort - edge.weight(Pi(edge.label, edge.weight, edge.source, edge.dest) " +
					"(EdgeHeapFile))\n";
			System.out.println(queryPlan);
		} catch (Exception e) {
			e.printStackTrace();

		}

	}

	/**
	 * Prints Edge data whose Weight value is in the range of values specified
	 * by lowWeight and highWeight
	 * 
	 * @param ehf
	 *            Edge Heap File
	 * @param edgeLabelLength
	 *            Length of Edge Label
	 * @param numBuf
	 *            Number of Buffers
	 * @param lowWeight
	 *            Lower limit of Weight
	 * @param highWeight
	 *            Higher limit of Weight
	 */
	public void query5(EdgeHeapFile ehf, short edgeLabelLength, short numBuf,
			int bound1, int bound2) {

		int lowerBound, upperBound;
		if (bound1 >= bound2) {
			upperBound = bound1;
			lowerBound = bound2;
		} else {
			upperBound = bound2;
			lowerBound = bound1;
		}

		String edgeHeapFileName = ehf.get_fileName();
		AttrType[] attrType = new AttrType[8];
		short[] stringSize = new short[3];
		stringSize[0] = edgeLabelLength;
		stringSize[1] = edgeLabelLength;
		stringSize[2] = edgeLabelLength;

		attrType[0] = new AttrType(AttrType.attrInteger);
		attrType[1] = new AttrType(AttrType.attrInteger);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrInteger);
		attrType[4] = new AttrType(AttrType.attrString);
		attrType[5] = new AttrType(AttrType.attrInteger);
		attrType[6] = new AttrType(AttrType.attrString);
		attrType[7] = new AttrType(AttrType.attrString);

		FldSpec[] projlist = new FldSpec[8];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);
		projlist[3] = new FldSpec(rel, 4);
		projlist[4] = new FldSpec(rel, 5);
		projlist[5] = new FldSpec(rel, 6);
		projlist[6] = new FldSpec(rel, 7);
		projlist[7] = new FldSpec(rel, 8);

		CondExpr[] expr = new CondExpr[3];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopGE);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].type1 = new AttrType(AttrType.attrInteger);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 6);
		expr[0].operand1.integer = upperBound;
		expr[1] = new CondExpr();
		expr[1].op = new AttrOperator(AttrOperator.aopLE);
		expr[1].type2 = new AttrType(AttrType.attrSymbol);
		expr[1].type1 = new AttrType(AttrType.attrInteger);
		expr[1].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 6);
		expr[1].operand1.integer = lowerBound;
		expr[2] = null;

		Edge edge = new Edge();
		try {
			EFileScan efscan = new EFileScan(edgeHeapFileName, attrType,
					stringSize, (short) 8, 8, projlist, expr);

			String edgeLabel, edgeSrc, edgeDest;
			int sourceNodePageID, sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;

			Edge e;
			e = efscan.get_next();
			while (e != null) {
				edge.edgeInit(e.getTupleByteArray(), e.getOffset());
				edge.setHdr();
				edgeLabel = edge.getLabel();
				sourceNodePageID = edge.getSource().pageNo.pid;
				sourceNodeSlotID = edge.getSource().slotNo;
				destinationNodePageID = edge.getDestination().pageNo.pid;
				destinationNodeSlotID = edge.getDestination().slotNo;
				edgeWeight = edge.getWeight();
				edgeSrc = edge.getSourceLabel();
				edgeDest = edge.getDestLabel();

				System.out.println("Label: " + edgeLabel + " , Weight : "
						+ edgeWeight + " Source Node PageID: "
						+ sourceNodePageID + " , Source Node SlotID: "
						+ sourceNodeSlotID + " , Destination Node PageID: "
						+ destinationNodePageID
						+ " , Destination Node SlotID: "
						+ destinationNodeSlotID);
				e = efscan.get_next();
			}
			efscan.close();
			String queryPlan = "\nPi(edge.label, edge.weight, edge.source, edge.dest) " +
					"(Sigma(edge.weight > lowBound && edge.weight < upBound)(EdgeHeapFile))\n";
			System.out.println(queryPlan);
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	/**
	 * printing the edge pairs who have the same destination node
	 * but not with itself using IndexNestedLoopsJoins
	 * @param ehf
	 * Edge Heap File
	 * @throws Exception 
	 * @throws UnknownKeyTypeException 
	 * @throws UnknowAttrType 
	 * @throws LowMemException 
	 * @throws PredEvalException 
	 * @throws PageNotReadException 
	 * @throws InvalidTypeException 
	 * @throws InvalidTupleSizeException 
	 */
	public void query6(EdgeHeapFile ehf, short numBuf) throws InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		
		short[] t2_str_sizes = new short[3];
		t2_str_sizes[0] = 32;
		t2_str_sizes[1] = 32;
		t2_str_sizes[2] = 32;
		AttrType[] in2 = new AttrType[8];
		in2[0] = new AttrType(AttrType.attrInteger);
		in2[1] = new AttrType(AttrType.attrInteger);
		in2[2] = new AttrType(AttrType.attrInteger);
		in2[3] = new AttrType(AttrType.attrInteger);
		in2[4] = new AttrType(AttrType.attrString);
		in2[5] = new AttrType(AttrType.attrInteger);
		in2[6] = new AttrType(AttrType.attrString);
		in2[7] = new AttrType(AttrType.attrString);
		
		FldSpec[] proj_list = new FldSpec[8];
		RelSpec inner_relation = new RelSpec(RelSpec.innerRel);
		RelSpec outer_relation = new RelSpec(RelSpec.outer);
		proj_list[0] = new FldSpec(outer_relation, 1);
		proj_list[1] = new FldSpec(outer_relation, 2);
		proj_list[2] = new FldSpec(outer_relation, 3);
		proj_list[3] = new FldSpec(outer_relation, 4);
		proj_list[4] = new FldSpec(outer_relation, 5);
		proj_list[5] = new FldSpec(outer_relation, 6);
		proj_list[6] = new FldSpec(outer_relation, 7);
		proj_list[7] = new FldSpec(outer_relation, 8);
		
		CondExpr[] out_filter_outer_Iterator = new CondExpr[3];
		out_filter_outer_Iterator[0] = new CondExpr();
		out_filter_outer_Iterator[0].op = new AttrOperator(AttrOperator.aopEQ);
		out_filter_outer_Iterator[0].type2 = new AttrType(AttrType.attrSymbol);
		out_filter_outer_Iterator[0].type1 = new AttrType(AttrType.attrSymbol);
		out_filter_outer_Iterator[0].operand2.symbol = new FldSpec(new RelSpec(
				RelSpec.outer), 8);
		out_filter_outer_Iterator[0].operand1.symbol = new FldSpec(new RelSpec(
				RelSpec.innerRel), 8);
		out_filter_outer_Iterator[1] = new CondExpr();
		out_filter_outer_Iterator[1].op = new AttrOperator(AttrOperator.aopNE);
		out_filter_outer_Iterator[1].type2 = new AttrType(AttrType.attrSymbol);
		out_filter_outer_Iterator[1].type1 = new AttrType(AttrType.attrSymbol);
		out_filter_outer_Iterator[1].operand2.symbol = new FldSpec(new RelSpec(
				RelSpec.outer), 5);
		out_filter_outer_Iterator[1].operand1.symbol = new FldSpec(new RelSpec(
				RelSpec.innerRel), 5);
		out_filter_outer_Iterator[2] = null;
		CondExpr or2 =	new CondExpr();
		or2.op = new AttrOperator(AttrOperator.aopNE);
		or2.type2 = new AttrType(AttrType.attrSymbol);
		or2.type1 = new AttrType(AttrType.attrSymbol);
		or2.operand2.symbol = new FldSpec(new RelSpec(
				RelSpec.outer), 6);
		or2.operand1.symbol = new FldSpec(new RelSpec(
				RelSpec.innerRel), 6);
		CondExpr or3 = new CondExpr();
		or3.op = new AttrOperator(AttrOperator.aopNE);
		or3.type2 = new AttrType(AttrType.attrSymbol);
		or3.type1 = new AttrType(AttrType.attrSymbol);
		or3.operand2.symbol = new FldSpec(new RelSpec(
				RelSpec.outer), 7);
		or3.operand1.symbol = new FldSpec(new RelSpec(
				RelSpec.innerRel), 7);
		or3.next =or2;
		out_filter_outer_Iterator[1].next = or2;
		
		FldSpec[] outer_proj_list = new FldSpec[8];
		outer_proj_list[0] = new FldSpec(inner_relation, 5);
		outer_proj_list[1] = new FldSpec(inner_relation, 6);
		outer_proj_list[2] = new FldSpec(inner_relation, 7);
		outer_proj_list[3] = new FldSpec(inner_relation, 8);
		outer_proj_list[4] = new FldSpec(outer_relation, 5);
		outer_proj_list[5] = new FldSpec(outer_relation, 6);
		outer_proj_list[6] = new FldSpec(outer_relation, 7);
		outer_proj_list[7] = new FldSpec(outer_relation, 8);
		Iterator eScan = new EFileScan(ehf.get_fileName(), in2, t2_str_sizes, (short)8, 8, proj_list, null);
		
		
		Iterator nlj = new NestedLoopsJoins(in2, 8, t2_str_sizes, in2, 8, t2_str_sizes, numBuf, eScan, ehf.get_fileName(), out_filter_outer_Iterator, null, outer_proj_list, 8);
		
		
		short[] output_str_sizes = new short[6];
		output_str_sizes[0] = 32;
		output_str_sizes[1] = 32;
		output_str_sizes[2] = 32;
		output_str_sizes[3] = 32;
		output_str_sizes[4] = 32;
		output_str_sizes[5] = 32;
		AttrType[] output_attr = new AttrType[8];
		output_attr[0] = new AttrType(AttrType.attrString);
		output_attr[1] = new AttrType(AttrType.attrInteger);
		output_attr[2] = new AttrType(AttrType.attrString);
		output_attr[3] = new AttrType(AttrType.attrString);
		output_attr[4] = new AttrType(AttrType.attrString);
		output_attr[5] = new AttrType(AttrType.attrInteger);
		output_attr[6] = new AttrType(AttrType.attrString);
		output_attr[7] = new AttrType(AttrType.attrString);
		Tuple tu;
		while((tu = nlj.get_next()) != null){
			tu.setHdr((short)8, output_attr, output_str_sizes);
			System.out.println("Edges " + tu.getStrFld(1)
					+ " and " + tu.getStrFld(5)
					+ " are incident pairs.");
		}
		
		nlj.close();
		String queryPlan = "\n(Pi(outedge.label, inedge.label)(Sigma((outedge.dest == inedge.dest) && (outedge.label != inedge.label " +
				"|| outedge.weight != inedge.weight || outedge.source != inedge.source))(EdgeHeapFile) " +
				"|><|(nlj) EdgeHeapFile)))\n";
		System.out.println(queryPlan);
	}
	
	//Sort Merge Join
	public void query7(EdgeHeapFile ehf, short labelLength, short numBuf) {
		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		expr[1] = new CondExpr();

		expr[0].next = null;
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 8);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 7);
		expr[1] = null;
		
		
		AttrType[] attrType = new AttrType[8];
		attrType[0] = new AttrType(AttrType.attrInteger); // SrcNID.pageid
		attrType[1] = new AttrType(AttrType.attrInteger); // SrcNID.slotno
		attrType[2] = new AttrType(AttrType.attrInteger); // DestNID.pageid
		attrType[3] = new AttrType(AttrType.attrInteger); // DestNID.slotno
		attrType[4] = new AttrType(AttrType.attrString); // EdgeLabel
		attrType[5] = new AttrType(AttrType.attrInteger); // EdgeWeight
		attrType[6] = new AttrType(AttrType.attrString); // SrcLabel
		attrType[7] = new AttrType(AttrType.attrString); // DestLabel

		AttrType[] jtype = new AttrType[8];
		jtype[0] = new AttrType(AttrType.attrString); // EdgeLabel1
		jtype[1] = new AttrType(AttrType.attrInteger); // EdgeWeight1
		jtype[2] = new AttrType(AttrType.attrString); // SrcLabel1
		jtype[3] = new AttrType(AttrType.attrString); // DestLabel1

		jtype[4] = new AttrType(AttrType.attrString); // EdgeLabel2
		jtype[5] = new AttrType(AttrType.attrInteger); // EdgeWeight2
		jtype[6] = new AttrType(AttrType.attrString); // SrcLabel2
		jtype[7] = new AttrType(AttrType.attrString); // DestLabel2

		FldSpec[] inputProjList = new FldSpec[8];
		RelSpec rel1 = new RelSpec(RelSpec.outer);
		RelSpec rel2 = new RelSpec(RelSpec.innerRel);
		inputProjList[0] = new FldSpec(rel1, 1);
		inputProjList[1] = new FldSpec(rel1, 2);
		inputProjList[2] = new FldSpec(rel1, 3);
		inputProjList[3] = new FldSpec(rel1, 4);
		inputProjList[4] = new FldSpec(rel1, 5);
		inputProjList[5] = new FldSpec(rel1, 6);
		inputProjList[6] = new FldSpec(rel1, 7);
		inputProjList[7] = new FldSpec(rel1, 8);

		FldSpec[] outputProjList = new FldSpec[8];
		outputProjList[0] = new FldSpec(rel1, 5);
		outputProjList[1] = new FldSpec(rel1, 6);
		outputProjList[2] = new FldSpec(rel1, 7);
		outputProjList[3] = new FldSpec(rel1, 8);
		outputProjList[4] = new FldSpec(rel2, 5);
		outputProjList[5] = new FldSpec(rel2, 6);
		outputProjList[6] = new FldSpec(rel2, 7);
		outputProjList[7] = new FldSpec(rel2, 8);

		String edgeHeapFileName = ehf.get_fileName();
		short s1_sizes[] = new short[3];
		s1_sizes[0] = labelLength;
		s1_sizes[1] = labelLength;
		s1_sizes[2] = labelLength;
		
		short s2_sizes[] = new short[6];
		s2_sizes[0] = labelLength;
		s2_sizes[1] = labelLength;
		s2_sizes[2] = labelLength;
		s2_sizes[3] = labelLength;
		s2_sizes[4] = labelLength;
		s2_sizes[5] = labelLength;

		TupleOrder order = new TupleOrder(TupleOrder.Ascending);
		EFileScan efscan1 = null;
		EFileScan efscan2 = null;
		SortMerge sm = null;
		Tuple t;

		try {
			efscan1 = new EFileScan(edgeHeapFileName, attrType, s1_sizes,
					(short) 8, 8, inputProjList, null);
//			Sort sort1 = new Sort(attrType, (short) 8, s1_sizes, efscan1,
//					8, order, 32, numBuf / 2);
			efscan2 = new EFileScan(edgeHeapFileName, attrType, s1_sizes,
					(short) 8, 8, inputProjList, null);
//			Sort sort2 = new Sort(attrType, (short) 8, s1_sizes, efscan2,
//					7, order, 32, numBuf / 2);
			sm = new SortMerge(
					attrType, 8, s1_sizes, attrType, 8, s1_sizes, 8,
					labelLength, 7, labelLength, numBuf, efscan1, efscan2, false,
					false, order, null, outputProjList, outputProjList.length);
					
			while ((t = sm.get_next()) != null) {
				t.print(jtype);
			}
			sm.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
