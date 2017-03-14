package batch;

import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.EdgeHeapFile;
import global.AttrOperator;
import global.AttrType;
import global.EID;
import global.IndexType;
import index.EdgeIndexScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import btree.BTreeFile;

public class EdgeQueryWithIndex {

	/**
	 * 
	 * @param ehf
	 * @param btf
	 * @param edgeLabelLength
	 * @param numBuf
	 */
	public void query0(EdgeHeapFile ehf, BTreeFile btf, short edgeLabelLength,
			short numBuf) {
		System.out.println("query0");
		EID eid = new EID();
		Edge edge;
		String edgeHeapFileName = ehf.get_fileName();
		String edgeIndexFileName = btf.get_fileName();
		
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
				expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 5);
				expr[0].operand1.string = targetEdgeLabel;
				expr[1] = null;
				EdgeIndexScan eIscan = new EdgeIndexScan(indType, edgeHeapFileName,
						edgeIndexFileName, attrType, stringSize, 6, 6, projlist,
						expr, 5, false);
				edge = eIscan.get_next();
				
				String edgeLabel;
				int sourceNodePageID,sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;					
				
				if(edge!=null){
					edgeLabel = edge.getLabel();
					sourceNodePageID = edge.getSource().pageNo.pid;
					sourceNodeSlotID = edge.getSource().slotNo;
					destinationNodePageID = edge.getDestination().pageNo.pid;
					destinationNodeSlotID = edge.getDestination().slotNo;
					edgeWeight = edge.getWeight();
					System.out.println(sourceNodePageID + " " + sourceNodeSlotID + " " + destinationNodePageID + " "
							+ destinationNodeSlotID + " " + edgeLabel + " " + edgeWeight);
				}
				edge = escan.getNext(eid);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void query3(EdgeHeapFile ehf, BTreeFile btf, short edgeLabelLength,
			short numBuf) {
		System.out.println("query3");

		String edgeHeapFileName = ehf.get_fileName();
		String edgeIndexFileName = btf.get_fileName();
		
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
		try {
			EdgeIndexScan eIscan = new EdgeIndexScan(indType, edgeHeapFileName,
					edgeIndexFileName, attrType, stringSize, 6, 6, projlist,
					expr, 5, false);
			edge = eIscan.get_next();
			String edgeLabel;
			int sourceNodePageID,sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;
			
			while (edge != null) {
				edge.setHdr();
				edgeLabel = edge.getLabel();
				sourceNodePageID = edge.getSource().pageNo.pid;
				sourceNodeSlotID = edge.getSource().slotNo;
				destinationNodePageID = edge.getDestination().pageNo.pid;
				destinationNodeSlotID = edge.getDestination().slotNo;
				edgeWeight = edge.getWeight();
				edge = eIscan.get_next();
				
				System.out.println(sourceNodePageID + " " + sourceNodeSlotID + " " + destinationNodePageID + " "
						+ destinationNodeSlotID + " " + edgeLabel + " " + edgeWeight);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void query4(EdgeHeapFile ehf, BTreeFile btf, short edgeLabelLength, short numBuf){
		System.out.println("query4");

		String edgeHeapFileName = ehf.get_fileName();
		String edgeIndexFileName = btf.get_fileName();
		
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
		try {
			EdgeIndexScan eIscan = new EdgeIndexScan(indType, edgeHeapFileName,
					edgeIndexFileName, attrType, stringSize, 6, 6, projlist,
					expr, 6, false);
			edge = eIscan.get_next();
			String edgeLabel;
			int sourceNodePageID,sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;

			while (edge != null) {
				edge.setHdr();
				edgeLabel = edge.getLabel();
				sourceNodePageID = edge.getSource().pageNo.pid;
				sourceNodeSlotID = edge.getSource().slotNo;
				destinationNodePageID = edge.getDestination().pageNo.pid;
				destinationNodeSlotID = edge.getDestination().slotNo;
				edgeWeight = edge.getWeight();
				edge = eIscan.get_next();
				
				System.out.println(sourceNodePageID + " " + sourceNodeSlotID + " " + destinationNodePageID + " "
						+ destinationNodeSlotID + " " + edgeLabel + " " + edgeWeight);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	
	public void query5(EdgeHeapFile ehf, BTreeFile btf, short edgeLabelLength, short numBuf, int bound1, int bound2){
		System.out.println("query5");

		int lowerBound, upperBound;
		if (bound1 >= bound2) {
			upperBound = bound1;
			lowerBound = bound2;
		}
		else{
			upperBound = bound2;
			lowerBound = bound1;
		}
		
		
		String edgeHeapFileName = ehf.get_fileName();
		String edgeIndexFileName = btf.get_fileName();
		
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
					edgeIndexFileName, attrType, stringSize, 6, 6, projlist,
					expr, 6, false);
			edge = eIscan.get_next();
			String edgeLabel;
			int sourceNodePageID,sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;

			while (edge != null) {
				edge.setHdr();
				edgeLabel = edge.getLabel();
				sourceNodePageID = edge.getSource().pageNo.pid;
				sourceNodeSlotID = edge.getSource().slotNo;
				destinationNodePageID = edge.getDestination().pageNo.pid;
				destinationNodeSlotID = edge.getDestination().slotNo;
				edgeWeight = edge.getWeight();
				edge = eIscan.get_next();
				
				System.out.println(sourceNodePageID + " " + sourceNodeSlotID + " " + destinationNodePageID + " "
						+ destinationNodeSlotID + " " + edgeLabel + " " + edgeWeight);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
}
