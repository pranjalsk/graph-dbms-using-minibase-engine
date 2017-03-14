package batch;

import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.EdgeHeapFile;
import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.EID;
import global.NID;
import global.TupleOrder;
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
	public void query0(EdgeHeapFile ehf, NodeHeapfile nhf) {
		System.out.println("edgequery 0");
		EID eid = new EID();
		Edge edge;
		try {

			NID sourceNID, destinationNID;
			NScan nscan;
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
			/*	NID nid = new NID();
				nscan = nhf.openScan();
				Node tempNode, sourceNode = null, destinationNode = null;
				while((tempNode = nscan.getNext(nid))!= null){
					//System.out.println("Page Number: "+nid.pageNo.pid+" , Slot Number: "+nid.slotNo);
					System.out.println("Here: "+edgeWeight);
					tempNode.setHdr();
					if(sourceNID.equals(nid))
						sourceNode = new Node(tempNode);
					else if(destinationNID.equals(nid)){
						destinationNode = new Node(tempNode);
					}
				}*/
				sourceNode = nhf.getRecord(sourceNID);
				sourceNode.setHdr();
				destinationNode = nhf.getRecord(destinationNID);
				destinationNode.setHdr();
				if(sourceNode != null)
					System.out.println("Source Node: "+sourceNode.getLabel());
				if(destinationNode != null)
					System.out.println("Destination Node: "+destinationNode.getLabel());
					
				System.out.println(edgeLabel + " "
						+ edgeWeight);
				edge = escan.getNext(eid);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		
	}
	
	
	public void query3(EdgeHeapFile ehf, short edgeLabelLength, short numBuf){
		System.out.println("edgequery 3");

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
			int sourceNodePageID,sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;
			
			
			Tuple t;
			t = sort.get_next();

			while (t != null) {
				edge.edgeInit(t.getTupleByteArray(), t.getOffset());
				edge.setHdr();
				edgeLabel ="";
				edgeLabel = edge.getLabel();
				sourceNodePageID = edge.getSource().pageNo.pid;
				sourceNodeSlotID = edge.getSource().slotNo;
				destinationNodePageID = edge.getDestination().pageNo.pid;
				destinationNodeSlotID = edge.getDestination().slotNo;
				edgeWeight = edge.getWeight();
				
				System.out.println(sourceNodePageID + " " + sourceNodeSlotID + " " + destinationNodePageID + " "
						+ destinationNodeSlotID + " " + edgeLabel + " " + edgeWeight);
				t = sort.get_next();
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
	}
	
	
	public void query4(EdgeHeapFile ehf, short edgeLabelLength, short numBuf){
		System.out.println("edgequery 4");

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
			int sourceNodePageID,sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;
			
			
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
				
				System.out.println(sourceNodePageID + " " + sourceNodeSlotID + " " + destinationNodePageID + " "
						+ destinationNodeSlotID + " " + edgeLabel + " " + edgeWeight);
				t = sort.get_next();
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
		
	}
	
	public void query5(EdgeHeapFile ehf, short edgeLabelLength, short numBuf, int lowWeight, int highWeight){
		System.out.println("edgequery 5");
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
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer),6); 
		expr[0].operand1.integer = lowWeight;
		expr[1] = new CondExpr();
		expr[1].op = new AttrOperator(AttrOperator.aopLE);
		expr[1].type2 = new AttrType(AttrType.attrSymbol);
		expr[1].type1 = new AttrType(AttrType.attrInteger);
		expr[1].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer),6); 
		expr[1].operand1.integer = highWeight;
		expr[2] = null;
		
		Edge edge = new Edge();
		try {
			EFileScan efscan = new EFileScan(edgeHeapFileName, attrType, stringSize, (short) 6, 6, projlist, expr);
			

			String edgeLabel;
			int sourceNodePageID,sourceNodeSlotID, destinationNodePageID, destinationNodeSlotID, edgeWeight;
			
			
			Edge e;
			e = efscan.get_next();

			while (e != null) {
				System.out.println("Here");
				edge.edgeInit(e.getTupleByteArray(), e.getOffset());
				edge.setHdr();
				edgeLabel = edge.getLabel();
				sourceNodePageID = edge.getSource().pageNo.pid;
				sourceNodeSlotID = edge.getSource().slotNo;
				destinationNodePageID = edge.getDestination().pageNo.pid;
				destinationNodeSlotID = edge.getDestination().slotNo;
				edgeWeight = edge.getWeight();
				
				System.out.println(sourceNodePageID + " " + sourceNodeSlotID + " " + destinationNodePageID + " "
						+ destinationNodeSlotID + " " + edgeLabel + " " + edgeWeight);
				e = efscan.get_next();
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
	}
}
