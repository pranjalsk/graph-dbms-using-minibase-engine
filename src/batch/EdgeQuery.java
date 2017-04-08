package batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import btree.BTreeFile;

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
			escan.closescan();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Prints Edge data based on increasing alpha-numerical order of label of Source Node
	 * @param ehf Edge Heap File
	 * @param nhf Node Heap File
	 */
	public void query1(EdgeHeapFile ehf, NodeHeapfile nhf, BTreeFile btf_node_label,short nodeLabelLength, short numBuf) {
		
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
			NFileScan nfscan = new NFileScan(nodeHeapFileName, attrType, stringSize, (short) 2, 2, projlist, null);
			Sort sort = new Sort(attrType, (short) 2, stringSize, nfscan, 1, order[0], nodeLabelLength, numBuf);

			String nodeLabel;
			Tuple t;
			t = sort.get_next();

			while (t != null) {
				node.nodeInit(t.getTupleByteArray(), t.getOffset());
				node.setHdr();
				nodeLabel = node.getLabel();
				BatchInsert bInsert = new BatchInsert();
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
				
				t = sort.get_next();
			}
			nfscan.close();
			sort.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Prints Edge data based on increasing alpha-numerical order of label of Destination Node
	 * @param ehf Edge Heap File
	 * @param nhf Node Heap File
	 */
	public void query2(EdgeHeapFile ehf, NodeHeapfile nhf, BTreeFile btf_node_label,short nodeLabelLength, short numBuf) {
		
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
			NFileScan nfscan = new NFileScan(nodeHeapFileName, attrType, stringSize, (short) 2, 2, projlist, null);
			Sort sort = new Sort(attrType, (short) 2, stringSize, nfscan, 1, order[0], nodeLabelLength, numBuf);

			String nodeLabel;
			Tuple t;
			t = sort.get_next();

			while (t != null) {
				node.nodeInit(t.getTupleByteArray(), t.getOffset());
				node.setHdr();
				nodeLabel = node.getLabel();
				BatchInsert bInsert = new BatchInsert();
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
				
				t = sort.get_next();
			}
			nfscan.close();
			sort.close();
			
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
			EFileScan efscan = new EFileScan(edgeHeapFileName, attrType, stringSize, (short) 8, 8, projlist, null);
			Sort sort = new Sort(attrType, (short) 8, stringSize, efscan, 5, order[0], edgeLabelLength, numBuf);

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

				System.out.println("Label: " + edgeLabel + " , Weight : " + edgeWeight + "Source Node PageID: "+sourceNodePageID + " , Source Node SlotID: " + sourceNodeSlotID + " , Destination Node PageID: " + destinationNodePageID + " , Destination Node SlotID: "
						+ destinationNodeSlotID );
				t = sort.get_next();
			}
			efscan.close();
			sort.close();
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
			EFileScan efscan = new EFileScan(edgeHeapFileName, attrType, stringSize, (short) 8, 6, projlist, null);
			Sort sort = new Sort(attrType, (short) 8, stringSize, efscan, 6, order[0], 4, numBuf);

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

				System.out.println("Label: " + edgeLabel + " , Weight : " + edgeWeight + "Source Node PageID: "+sourceNodePageID + " , Source Node SlotID: " + sourceNodeSlotID + " , Destination Node PageID: " + destinationNodePageID + " , Destination Node SlotID: "
						+ destinationNodeSlotID );
				t = sort.get_next();
			}
			sort.close();
			efscan.close();
			
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
			EFileScan efscan = new EFileScan(edgeHeapFileName, attrType, stringSize, (short) 8, 6, projlist, expr);

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

				System.out.println("Label: " + edgeLabel + " , Weight : " + edgeWeight + " Source Node PageID: "+sourceNodePageID + " , Source Node SlotID: " + sourceNodeSlotID + " , Destination Node PageID: " + destinationNodePageID + " , Destination Node SlotID: "
						+ destinationNodeSlotID );
				e = efscan.get_next();
			}
			efscan.close();
			
			
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	/**
	 * 
	 * @param ehf Edge Heap File
	 * @param nhf Node Heap File
	 */
	public void query6(EdgeHeapFile ehf) {

		int count = 0;
		Edge edgeOuter;
		EID eidOuter = new EID();
		try {
			EScan eOuterscan = ehf.openScan();
			edgeOuter = eOuterscan.getNext(eidOuter);

			while (edgeOuter != null) {
				edgeOuter.setHdr();
				NID outerEdgeDist = edgeOuter.getDestination();
				
				Edge edgeInner;
				EID eidInner = new EID();
				EScan eInnerscan = ehf.openScan();
				edgeInner = eInnerscan.getNext(eidInner);
				
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
					edgeInner = eInnerscan.getNext(eidInner);
				}
				eInnerscan.closescan();
				edgeOuter = eOuterscan.getNext(eidOuter);
			}
			eOuterscan.closescan();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("No. of incident edge pairs "+count);
	}
}
