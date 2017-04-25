package batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import nodeheap.Node;
import nodeheap.NodeHeapfile;
import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.EdgeHeapFile;
import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.EID;
import global.IndexType;
import global.NID;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.EdgeIndexScan;
import index.IndexException;
import index.NodeIndexScan;
import iterator.CondExpr;
import iterator.EFileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.IndexNestedLoopsJoins;
import iterator.InvalidRelation;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NestedLoopException;
import iterator.NestedLoopsJoins;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import btree.BTreeFile;
import bufmgr.PageNotReadException;

public class EdgeQueryWithIndex {

	/**
	 * When {QTYPE = 0,Index = 1} then the query will print the edge data in the
	 * order it occurs in the node heap using edge label index file
	 * 
	 * @param ehf
	 *            EdgeHeapFile
	 * @param btf_edge_label
	 *            B-Tree Index file on edge label
	 * @param edgeLabelLength
	 * @param numBuf
	 */
	public void query0(EdgeHeapFile ehf, BTreeFile btf_edge_label,
			NodeHeapfile nhf, short edgeLabelLength, short numBuf) {
		EID eid = new EID();
		Edge edge;
		String edgeHeapFileName = ehf.get_fileName();
		String edgeIndexFileName = btf_edge_label.get_fileName();

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

		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		IndexType indType = new IndexType(1);
		try {
			EScan escan = ehf.openScan();
			edge = escan.getNext(eid);
			String targetEdgeLabel;

			while (edge != null) {
				edge.setHdr();
				targetEdgeLabel = edge.getLabel();
				expr[0].op = new AttrOperator(AttrOperator.aopEQ);
				expr[0].type2 = new AttrType(AttrType.attrSymbol);
				expr[0].type1 = new AttrType(AttrType.attrString);
				expr[0].operand2.symbol = new FldSpec(
						new RelSpec(RelSpec.outer), 5);
				expr[0].operand1.string = targetEdgeLabel;
				expr[1] = null;
				EdgeIndexScan eIscan = new EdgeIndexScan(indType,
						edgeHeapFileName, edgeIndexFileName, attrType,
						stringSize, 8, 8, projlist, expr, 5, false);
				edge = eIscan.get_next();

				String edgeLabel, edgeSrc, edgeDest;
				int sourceNodePageID, sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;
				Node sourceNode = null, destinationNode = null;
				NID sourceNID, destinationNID;
				if (edge != null) {
					edgeLabel = edge.getLabel();
					edgeSrc = edge.getSourceLabel();
					edgeDest = edge.getDestLabel();
					sourceNID = edge.getSource();
					destinationNID = edge.getDestination();

					sourceNode = nhf.getRecord(sourceNID);
					sourceNode.setHdr();

					destinationNode = nhf.getRecord(destinationNID);
					destinationNode.setHdr();

					edgeWeight = edge.getWeight();
					System.out.print("Label: " + edgeLabel + " , Weight:"
							+ edgeWeight + " , ");
					if (sourceNode != null)
						System.out.print("Source Node: "
								+ sourceNode.getLabel() + " , ");
					if (destinationNode != null)
						System.out.print("Destination Node: "
								+ destinationNode.getLabel());
					System.out.println();
				}
				edge = escan.getNext(eid);
				eIscan.close();
			}
			escan.closescan();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void query1(EdgeHeapFile ehf, BTreeFile btf_node_label,
			NodeHeapfile nhf, short nodeLabelLength, short numBuf) {

		String nodeHeapFileName = nhf.get_fileName();
		String nodeIndexFileName = btf_node_label.get_fileName();
		AttrType[] attrType = new AttrType[2];
		short[] stringSize = new short[1];
		stringSize[0] = nodeLabelLength;
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrDesc);
		FldSpec[] projlist = new FldSpec[2];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);

		CondExpr[] expr = null;
		IndexType indType = new IndexType(1);
		Node node = new Node();
		try {
			NodeIndexScan nIscan = new NodeIndexScan(indType, nodeHeapFileName,
					nodeIndexFileName, attrType, stringSize, 2, 2, projlist,
					expr, 1, false);
			node = nIscan.get_next();
			String nodeLabel;

			while (node != null) {
				node.setHdr();
				nodeLabel = node.getLabel();
				BatchMapperClass bInsert = new BatchMapperClass();
				NID nodeNID = bInsert.getNidFromNodeLabel(nodeLabel, nhf,btf_node_label);
				
				
				EID eid = new EID();
				Edge edge;
				try {

					NID sourceNID;
					EScan escan = ehf.openScan();
					edge = escan.getNext(eid);
					while (edge != null) {
						edge.setHdr();
						sourceNID = edge.getSource();
						
						if(nodeNID.equals(sourceNID)){
							System.out.println("Label: "+edge.getLabel() + " , Weight: " + edge.getWeight() + " , Source Node Label: " + node.getLabel());
						}
							
						edge = escan.getNext(eid);
					}
					escan.closescan();
					
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				node = nIscan.get_next();
			}
			nIscan.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void query2(EdgeHeapFile ehf, BTreeFile btf_node_label,
			NodeHeapfile nhf, short nodeLabelLength, short numBuf) {
		
		String nodeHeapFileName = nhf.get_fileName();
		String nodeIndexFileName = btf_node_label.get_fileName();
		AttrType[] attrType = new AttrType[2];
		short[] stringSize = new short[1];
		stringSize[0] = nodeLabelLength;
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrDesc);
		FldSpec[] projlist = new FldSpec[2];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);

		CondExpr[] expr = null;
		IndexType indType = new IndexType(1);
		Node node = new Node();
		try {
			NodeIndexScan nIscan = new NodeIndexScan(indType, nodeHeapFileName,
					nodeIndexFileName, attrType, stringSize, 2, 2, projlist,
					expr, 1, false);
			node = nIscan.get_next();
			String nodeLabel;

			while (node != null) {
				node.setHdr();
				nodeLabel = node.getLabel();
				BatchMapperClass bInsert = new BatchMapperClass();
				NID nodeNID = bInsert.getNidFromNodeLabel(nodeLabel, nhf,btf_node_label);
				
				
				EID eid = new EID();
				Edge edge;
				try {

					NID destNID;
					EScan escan = ehf.openScan();
					edge = escan.getNext(eid);
					while (edge != null) {
						edge.setHdr();
						destNID = edge.getDestination();
						
						if(nodeNID.equals(destNID)){
							System.out.println("Label: "+edge.getLabel() + " , Weight: " + edge.getWeight() + " , Destination Node Label: " + node.getLabel());
						}
							
						edge = escan.getNext(eid);
					}
					escan.closescan();
					
					
				} catch (Exception e) {
					e.printStackTrace();
				}
				node = nIscan.get_next();
			}
			nIscan.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * when {QTYPE = 3,Index =1} then the query will print the edge data in
	 * increasing alphanumerical order of edge labels. using edge label index
	 * file
	 * 
	 * @param ehf
	 *            edge heap file
	 * @param btf_edge_label
	 *            b-tree index file on edge label
	 * @param edgeLabelLength
	 * @param numBuf
	 */
	public void query3(EdgeHeapFile ehf, BTreeFile btf_edge_label,
			short edgeLabelLength, short numBuf) {

		String edgeHeapFileName = ehf.get_fileName();
		String edgeIndexFileName = btf_edge_label.get_fileName();

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

		CondExpr[] expr = null;
		IndexType indType = new IndexType(1);
		Edge edge = new Edge();
		try {
			EdgeIndexScan eIscan = new EdgeIndexScan(indType, edgeHeapFileName,
					edgeIndexFileName, attrType, stringSize, 8, 8, projlist,
					expr, 5, false);
			edge = eIscan.get_next();
			String edgeLabel, edgeSrc, edgeDest;
			int sourceNodePageID, sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;

			while (edge != null) {
				edge.setHdr();
				edgeLabel = edge.getLabel();
				edgeSrc = edge.getSourceLabel();
				edgeDest = edge.getDestLabel();
				sourceNodePageID = edge.getSource().pageNo.pid;
				sourceNodeSlotID = edge.getSource().slotNo;
				destinationNodePageID = edge.getDestination().pageNo.pid;
				destinationNodeSlotID = edge.getDestination().slotNo;
				edgeWeight = edge.getWeight();
				edge = eIscan.get_next();

				System.out.println("Label: " + edgeLabel + " , Weight : "
						+ edgeWeight + "Source Node PageID: "
						+ sourceNodePageID + " , Source Node SlotID: "
						+ sourceNodeSlotID + " , Destination Node PageID: "
						+ destinationNodePageID
						+ " , Destination Node SlotID: "
						+ destinationNodeSlotID);
			}
			eIscan.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * when {QTYPE = 4,Index =1} then the query will print the edge data in
	 * increasing order of weights. using B-Tree index file on edge weight
	 * 
	 * @param ehf
	 *            edge heap file
	 * @param btf_edge_weight
	 *            b tree index file on edge weight
	 * @param edgeLabelLength
	 * @param numBuf
	 */
	public void query4(EdgeHeapFile ehf, BTreeFile btf_edge_weight,
			short edgeLabelLength, short numBuf) {
		System.out.println("query4");

		String edgeHeapFileName = ehf.get_fileName();
		String edgeIndexFileName = btf_edge_weight.get_fileName();

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

		CondExpr[] expr = null;
		IndexType indType = new IndexType(1);
		Edge edge = new Edge();
		try {
			EdgeIndexScan eIscan = new EdgeIndexScan(indType, edgeHeapFileName,
					edgeIndexFileName, attrType, stringSize, 8, 8, projlist,
					expr, 6, false);
			edge = eIscan.get_next();
			String edgeLabel, edgeSrc, edgeDest;
			int sourceNodePageID, sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;

			while (edge != null) {
				edge.setHdr();
				edgeLabel = edge.getLabel();
				edgeSrc = edge.getSourceLabel();
				edgeDest = edge.getDestLabel();				
				
				sourceNodePageID = edge.getSource().pageNo.pid;
				sourceNodeSlotID = edge.getSource().slotNo;
				destinationNodePageID = edge.getDestination().pageNo.pid;
				destinationNodeSlotID = edge.getDestination().slotNo;
				edgeWeight = edge.getWeight();
				edge = eIscan.get_next();

				System.out.println("Label: " + edgeLabel + " , Weight : "
						+ edgeWeight + "Source Node PageID: "
						+ sourceNodePageID + " , Source Node SlotID: "
						+ sourceNodeSlotID + " , Destination Node PageID: "
						+ destinationNodePageID
						+ " , Destination Node SlotID: "
						+ destinationNodeSlotID);
			}
			eIscan.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * when {QTYPE = 5,Index =1} then the query will take a lower and upper
	 * bound on edge weights, and will return the matching edge data using edge
	 * weight index file
	 * 
	 * @param ehf
	 *            edge heap file
	 * @param btf_edge_weight
	 *            B tree index file on edge weight
	 * @param edgeLabelLength
	 * @param numBuf
	 * @param bound1
	 * @param bound2
	 */
	public void query5(EdgeHeapFile ehf, BTreeFile btf_edge_weight,
			short edgeLabelLength, short numBuf, int bound1, int bound2) {
		System.out.println("query5");

		int lowerBound, upperBound;
		if (bound1 >= bound2) {
			upperBound = bound1;
			lowerBound = bound2;
		} else {
			upperBound = bound2;
			lowerBound = bound1;
		}
		String edgeHeapFileName = ehf.get_fileName();
		String edgeIndexFileName = btf_edge_weight.get_fileName();

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
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrInteger);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 6);
		expr[0].operand2.integer = lowerBound;
		expr[0].next = null;
		expr[1] = new CondExpr();
		expr[1].op = new AttrOperator(AttrOperator.aopLE);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrInteger);
		expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 6);
		expr[1].operand2.integer = upperBound;
		expr[1].next = null;
		expr[2] = null;

		IndexType indType = new IndexType(1);
		Edge edge = new Edge();
		try {
			EdgeIndexScan eIscan = new EdgeIndexScan(indType, edgeHeapFileName,
					edgeIndexFileName, attrType, stringSize, 8, 8, projlist,
					expr, 6, false);
			edge = eIscan.get_next();
			String edgeLabel, edgeSrc, edgeDest;
			int sourceNodePageID, sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;

			while (edge != null) {
				edge.setHdr();
				edgeLabel = edge.getLabel();
				edgeSrc = edge.getSourceLabel();
				edgeDest = edge.getDestLabel();
				sourceNodePageID = edge.getSource().pageNo.pid;
				sourceNodeSlotID = edge.getSource().slotNo;
				destinationNodePageID = edge.getDestination().pageNo.pid;
				destinationNodeSlotID = edge.getDestination().slotNo;
				edgeWeight = edge.getWeight();
				edge = eIscan.get_next();

				System.out.println("Label: " + edgeLabel + " , Weight : "
						+ edgeWeight + " Source Node PageID: "
						+ sourceNodePageID + " , Source Node SlotID: "
						+ sourceNodeSlotID + " , Destination Node PageID: "
						+ destinationNodePageID
						+ " , Destination Node SlotID: "
						+ destinationNodeSlotID);
			}
			eIscan.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * printing the edge pairs who have the same destination node
	 * but not with itself using IndexNestedLoopsJoins
	 * @param ehf
	 * @param btf_edge_dest_label
	 * @param nhf
	 * @param edgeLabelLength
	 * @param numBuf
	 * @throws JoinsException
	 * @throws IndexException
	 * @throws PageNotReadException
	 * @throws PredEvalException
	 * @throws SortException
	 * @throws LowMemException
	 * @throws UnknowAttrType
	 * @throws UnknownKeyTypeException
	 * @throws Exception
	 */
	public void query6(EdgeHeapFile ehf, BTreeFile btf_edge_dest_label,
			NodeHeapfile nhf, short edgeLabelLength, short numBuf)
			throws JoinsException, IndexException, PageNotReadException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
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
		
		
		Iterator inlj = new IndexNestedLoopsJoins(in2, 8, 8, t2_str_sizes, in2, 8, 8, t2_str_sizes, numBuf, eScan, ehf.get_fileName(), btf_edge_dest_label.get_fileName(), proj_list, out_filter_outer_Iterator, null, outer_proj_list, 8);
		
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
		while((tu = inlj.get_next()) != null){
			tu.setHdr((short)8, output_attr, output_str_sizes);
			System.out.println("Edges " + tu.getStrFld(1)
					+ " and " + tu.getStrFld(5)
					+ " are incident pairs.");
		}
		
		inlj.close();
	}
}
