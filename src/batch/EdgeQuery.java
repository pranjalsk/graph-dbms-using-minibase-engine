package batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

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
import heap.Tuple;
import iterator.CondExpr;
import iterator.EFileScan;
import iterator.FldSpec;
import iterator.NFileScan;
import iterator.RelSpec;
import iterator.Sort;
import nodeheap.NScan;
import nodeheap.Node;
import nodeheap.NodeHeapfile;

public class EdgeQuery {
	/**
	 * Prints Edge data on the order of occurrence/storage in Edge Heap File
	 * @param ehf Edge Heap File
	 * @param nhf Node Heap File
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
				
				System.out.print("Label: "+edgeLabel + " , Weight:" + edgeWeight+" , ");
				if (sourceNode != null)
					System.out.print("Source Node: " + sourceNode.getLabel()+" , ");
				if (destinationNode != null)
					System.out.print("Destination Node: " + destinationNode.getLabel());
				System.out.println();
				
				edge = escan.getNext(eid);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Prints Edge data based on increasing alpha-numerical order of label of Source Node
	 * @param ehf Edge Heap File
	 * @param nhf Node Heap File
	 */
	public void query1(EdgeHeapFile ehf, NodeHeapfile nhf) {
		
		EID eid = new EID();
		Edge edge;
		try {

			EScan escan = ehf.openScan();
			edge = escan.getNext(eid);

			Map<String, ArrayList<Edge>> sorceNodeToEdgeMap = new TreeMap<String, ArrayList<Edge>>();
			while (edge != null) {
				edge.setHdr();
				NID sourceNID = new NID();
				sourceNID = edge.getSource();
				Node sourceNode = nhf.getRecord(sourceNID);
				sourceNode.setHdr();
				String sourceLabel = sourceNode.getLabel();
				if (sorceNodeToEdgeMap.containsKey(sourceLabel)) {
					ArrayList<Edge> tempList = sorceNodeToEdgeMap.get(sourceLabel);
					tempList.add(new Edge(edge));
					sorceNodeToEdgeMap.put(sourceLabel, tempList);
				} else {
					ArrayList<Edge> tempList = new ArrayList<Edge>();
					tempList.add(new Edge(edge));
					sorceNodeToEdgeMap.put(sourceLabel, tempList);
				}
				edge = escan.getNext(eid);
			}

			for (String sourceNodeLab : sorceNodeToEdgeMap.keySet()) {
				ArrayList<Edge> edgeList = sorceNodeToEdgeMap.get(sourceNodeLab);
				for (Edge edgeToPrint : edgeList) {
					System.out.println("Label: "+edgeToPrint.getLabel() + " , Weight: " + edgeToPrint.getWeight() + " , Source Node Label: " + sourceNodeLab);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Prints Edge data based on increasing alpha-numerical order of label of Destination Node
	 * @param ehf Edge Heap File
	 * @param nhf Node Heap File
	 */
	public void query2(EdgeHeapFile ehf, NodeHeapfile nhf) {
		EID eid = new EID();
		Edge edge;
		try {
			EScan escan = ehf.openScan();
			edge = escan.getNext(eid);
		
			Map<String, ArrayList<Edge>> destNodeToEdgeMap = new TreeMap<String, ArrayList<Edge>>();
			while (edge != null) {
				edge.setHdr();
				NID destNID = new NID();
				destNID = edge.getDestination();
				Node destNode = nhf.getRecord(destNID);
				destNode.setHdr();
				String destLabel = destNode.getLabel();
				if (destNodeToEdgeMap.containsKey(destLabel)) {
					ArrayList<Edge> tempList = destNodeToEdgeMap.get(destLabel);
					tempList.add(new Edge(edge));
					destNodeToEdgeMap.put(destLabel, tempList);
				} else {
					ArrayList<Edge> tempList = new ArrayList<Edge>();
					tempList.add(new Edge(edge));
					destNodeToEdgeMap.put(destLabel, tempList);
				}
				edge = escan.getNext(eid);
			}

			for (String destNodeLab : destNodeToEdgeMap.keySet()) {
				ArrayList<Edge> edgeList = destNodeToEdgeMap.get(destNodeLab);
				for (Edge edgeToPrint : edgeList) {
					System.out.println("Label: "+edgeToPrint.getLabel() + " , Weight: " + edgeToPrint.getWeight() + " , Destination Node Label: " + destNodeLab);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Prints Edge data based on increasing alpha-numerical order of Edge Label
	 * @param ehf Edge Heap File
	 * @param edgeLabelLength Length of Edge Label
	 * @param numBuf Number of Buffers
	 */
	public void query3(EdgeHeapFile ehf, short edgeLabelLength, short numBuf) {

		String edgeHeapFileName = ehf.get_fileName();
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

		TupleOrder[] order = new TupleOrder[2];
		order[0] = new TupleOrder(TupleOrder.Ascending);
		order[1] = new TupleOrder(TupleOrder.Descending);

		Edge edge = new Edge();
		try {
			EFileScan efscan = new EFileScan(edgeHeapFileName, attrType, stringSize, (short) 6, 6, projlist, null);
			Sort sort = new Sort(attrType, (short) 6, stringSize, efscan, 5, order[0], edgeLabelLength, numBuf);

			String edgeLabel;
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

				System.out.println("Label: " + edgeLabel + " , Weight : " + edgeWeight + "Source Node PageID: "+sourceNodePageID + " , Source Node SlotID: " + sourceNodeSlotID + " , Destination Node PageID: " + destinationNodePageID + " , Destination Node SlotID: "
						+ destinationNodeSlotID );
				t = sort.get_next();
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	/**
	 * Prints Edge data based on increasing numerical order of Edge Weights
	 * @param ehf Edge Heap File
	 * @param edgeLabelLength Length of Edge Label
	 * @param numBuf Number of Buffers
	 */
	public void query4(EdgeHeapFile ehf, short edgeLabelLength, short numBuf) {

		String edgeHeapFileName = ehf.get_fileName();
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

		TupleOrder[] order = new TupleOrder[2];
		order[0] = new TupleOrder(TupleOrder.Ascending);
		order[1] = new TupleOrder(TupleOrder.Descending);

		Edge edge = new Edge();
		try {
			EFileScan efscan = new EFileScan(edgeHeapFileName, attrType, stringSize, (short) 6, 6, projlist, null);
			Sort sort = new Sort(attrType, (short) 6, stringSize, efscan, 6, order[0], edgeLabelLength, numBuf);

			String edgeLabel;
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

				System.out.println("Label: " + edgeLabel + " , Weight : " + edgeWeight + "Source Node PageID: "+sourceNodePageID + " , Source Node SlotID: " + sourceNodeSlotID + " , Destination Node PageID: " + destinationNodePageID + " , Destination Node SlotID: "
						+ destinationNodeSlotID );
				t = sort.get_next();
			}
		} catch (Exception e) {
			e.printStackTrace();

		}

	}

	/**
	 * Prints Edge data whose Weight value is in the range of values specified by lowWeight and highWeight
	 * @param ehf Edge Heap File
	 * @param edgeLabelLength Length of Edge Label
	 * @param numBuf Number of Buffers
	 * @param lowWeight Lower limit of Weight
	 * @param highWeight Higher limit of Weight
	 */
	public void query5(EdgeHeapFile ehf, short edgeLabelLength, short numBuf, int bound1, int bound2) {
	
		int lowerBound, upperBound;
		if (bound1 >= bound2) {
			upperBound = bound1;
			lowerBound = bound2;
		} else {
			upperBound = bound2;
			lowerBound = bound1;
		}
		
		String edgeHeapFileName = ehf.get_fileName();
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
			EFileScan efscan = new EFileScan(edgeHeapFileName, attrType, stringSize, (short) 6, 6, projlist, expr);

			String edgeLabel;
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

				System.out.println("Label: " + edgeLabel + " , Weight : " + edgeWeight + " Source Node PageID: "+sourceNodePageID + " , Source Node SlotID: " + sourceNodeSlotID + " , Destination Node PageID: " + destinationNodePageID + " , Destination Node SlotID: "
						+ destinationNodeSlotID );
				e = efscan.get_next();
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	/**
	 * 
	 * @param ehf Edge Heap File
	 * @param nhf Node Heap File
	 */
	public void query6(EdgeHeapFile ehf, NodeHeapfile nhf) {

		EID eid = new EID();
		Edge edge, edgeObj;
		ArrayList<Edge> edgeList = new ArrayList<Edge>();
		try {

			EScan escan = ehf.openScan();
			edge = escan.getNext(eid);

			while (edge != null) {
				edge.setHdr();
				edgeObj = new Edge(edge);
				edgeList.add(edgeObj);
				edge = escan.getNext(eid);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		int totalEdges = edgeList.size();
		int i, j;
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
					if (firstEdgeDestination.pageNo.pid == secondEdge.getSource().pageNo.pid) {
						if (firstEdgeDestination.slotNo == secondEdgeSource.slotNo) {
							try {
								incidentNode = nhf.getRecord(firstEdgeDestination);
								incidentNode.setHdr();
							} catch (Exception e) {

								e.printStackTrace();
							}
							System.out.println("Edges " + firstEdge.getLabel() + " and " + secondEdge.getLabel()
									+ " are incident on Node " + incidentNode.getLabel());
						}
					}
				} catch (FieldNumberOutOfBoundException  e) {

					e.printStackTrace();

				}catch(IOException e) {

					e.printStackTrace();
				}

			}

		}

	}
}
