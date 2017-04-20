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
import index.EdgeIndexScan;
import index.NodeIndexScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
import btree.BTreeFile;

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
					// sourceNodePageID = edge.getSource().pageNo.pid;
					// sourceNodeSlotID = edge.getSource().slotNo;
					// destinationNodePageID = edge.getDestination().pageNo.pid;
					// destinationNodeSlotID = edge.getDestination().slotNo;
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

	public void query6(EdgeHeapFile ehf, BTreeFile btf_edge_label,
			NodeHeapfile nhf, short edgeLabelLength, short numBuf) {
		/*String edgeHeapFileName = ehf.get_fileName();
		String edgeIndexFileName = btf_edge_label.get_fileName();
		ArrayList<Edge> edgeList = new ArrayList<Edge>();

		AttrType[] attrType = new AttrType[6];
		short[] stringSize = new short[1];
		stringSize[0] = edgeLabelLength;
		attrType[0] = new AttrType(AttrType.attrInteger);
		attrType[1] = new AttrType(AttrType.attrInteger);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrInteger);
		attrType[4] = new AttrType(AttrType.attrString);
		attrType[5] = new AttrType(AttrType.attrInteger);

		FldSpec[] projlist = new FldSpec[6];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);
		projlist[3] = new FldSpec(rel, 4);
		projlist[4] = new FldSpec(rel, 5);
		projlist[5] = new FldSpec(rel, 6);

		CondExpr[] expr = null;
		IndexType indType = new IndexType(1);
		Edge edge = new Edge();
		Edge edgeObj;
		Map<String, ArrayList<Edge>> sorceNodeToEdgeMap = new TreeMap<String, ArrayList<Edge>>();

		try {
			EdgeIndexScan eIscan = new EdgeIndexScan(indType, edgeHeapFileName,
					edgeIndexFileName, attrType, stringSize, 6, 6, projlist,
					expr, 6, false);
			edge = eIscan.get_next();
			while (edge != null) {
				edge.setHdr();
				edgeObj = new Edge(edge);
				edgeList.add(edgeObj);
				edge = eIscan.get_next();

			}

			eIscan.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		int totalEdges = edgeList.size();
		int i, j, count = 0;
		Edge firstEdge, secondEdge;
		NID firstEdgeDestination, secondEdgeSource;
		Node incidentNode = new Node();
		for (i = 0; i < totalEdges; i++) {
			firstEdge = (Edge) edgeList.get(i);
			for (j = 0; j < totalEdges; j++) {
				if (i == j)
					continue;

				secondEdge = edgeList.get(j);

				try {
					firstEdgeDestination = firstEdge.getDestination();
					secondEdgeSource = secondEdge.getSource();
					if (firstEdgeDestination.pageNo.pid == secondEdge
							.getSource().pageNo.pid) {
						if (firstEdgeDestination.slotNo == secondEdgeSource.slotNo) {
							try {
								incidentNode = nhf
										.getRecord(firstEdgeDestination);
								incidentNode.setHdr();
							} catch (Exception e) {

								e.printStackTrace();
							}
							System.out.println("Edges " + firstEdge.getLabel()
									+ " and " + secondEdge.getLabel()
									+ " are incident on Node "
									+ incidentNode.getLabel());
							count++;
						}
					}
				} catch (FieldNumberOutOfBoundException e) {

					e.printStackTrace();

				} catch (IOException e) {

					e.printStackTrace();
				}

			}
		}

		System.out.println("No. of incident edge pairs "+count);*/
		
		
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
		
		int count = 0;
		Edge edgeOuter;
		try {
			EdgeIndexScan eOuterscan = new EdgeIndexScan(indType, edgeHeapFileName,
					edgeIndexFileName, attrType, stringSize, 8, 8, projlist,
					expr, 5, false);
			edgeOuter = eOuterscan.get_next();

			while (edgeOuter != null) {
				edgeOuter.setHdr();
				NID outerEdgeDist = edgeOuter.getDestination();
				
				Edge edgeInner;
				EdgeIndexScan eInnerscan = new EdgeIndexScan(indType, edgeHeapFileName,
						edgeIndexFileName, attrType, stringSize, 8, 8, projlist,
						expr, 5, false);
				edgeInner = eInnerscan.get_next();
				
				while(edgeInner != null){
					edgeInner.setHdr();
					NID innerEdgeDist = edgeInner.getDestination();
					if(edgeInner.getLabel() != edgeOuter.getLabel() && edgeInner.getWeight() != edgeOuter.getWeight() 
							&& edgeInner.getSource() != edgeOuter.getSource() && edgeInner.getDestination() != edgeOuter.getDestination()){
						if(outerEdgeDist.equals(innerEdgeDist)){
							System.out.println("Edges " + edgeOuter.getLabel() + " and " + edgeInner.getLabel()
									+ " are incident pairs.");
							count++;
						}
					}
					edgeInner = eInnerscan.get_next();
				}
				eInnerscan.close();
				edgeOuter = eOuterscan.get_next();
			}
			eOuterscan.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("No. of incident edge pairs "+count);
	}
}
