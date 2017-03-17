package batch;


import java.util.ArrayList;
import java.util.List;
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
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.NFileScan;
import iterator.RelSpec;
import iterator.Sort;
import nodeheap.NScan;
import nodeheap.Node;
import nodeheap.NodeHeapfile;


public class NodeQuery {

	/**
	 * Prints Node data in the order of occurrence/storage in Node Heap File
	 * @param nhf Node Heap File
	 */
	public void query0(NodeHeapfile nhf) {
		
		NID nid = new NID();
		Node node;
		
		try {
			NScan nscan = nhf.openScan();
			node = nscan.getNext(nid);
			String nodeLabel;
			Descriptor nodeDescriptor;
			while (node != null) {
				node.setHdr();
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				System.out.println("Label: "+nodeLabel + " , Descriptor: [" + nodeDescriptor.get(0) + " , " + nodeDescriptor.get(1) + " , "
						+ nodeDescriptor.get(2) + " , " + nodeDescriptor.get(3) + " , " + nodeDescriptor.get(4)+"]");
				node = nscan.getNext(nid);
			}
			nscan.closescan();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Prints Node data based on increasing alpha-numerical order of Node Label
	 * @param nhf Node Heap File
	 * @param nodeLabelLength Length of the Node Label
	 * @param numBuf Number of Buffers
	 */
	public void query1(NodeHeapfile nhf, short nodeLabelLength, short numBuf) {
		

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
			Descriptor nodeDescriptor;
			Tuple t;
			t = sort.get_next();

			while (t != null) {
				node.nodeInit(t.getTupleByteArray(), t.getOffset());
				node.setHdr();
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				System.out.println("Label: "+nodeLabel + " , Descriptor: [" + nodeDescriptor.get(0) + " , " + nodeDescriptor.get(1) + " , "
						+ nodeDescriptor.get(2) + " , " + nodeDescriptor.get(3) + " , " + nodeDescriptor.get(4)+"]");
				t = sort.get_next();
			}
			sort.close();
			nfscan.close();
			
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	/**
	 * Prints Node data based on increasing order of distance of Node's Descriptor from the Target Descriptor
	 * @param nhf Node Heap File
	 * @param nodeLabelLength Length of the Node Label
	 * @param numBuf Number of Buffers
	 * @param targetDescriptor Target Descriptor
	 * @param distance Target Distance
	 */
	public void query2(NodeHeapfile nhf, short nodeLabelLength, short numBuf, Descriptor targetDescriptor, double distance){
		
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

		CondExpr[] expr = null;
		Map<Double, Node> distanceToNodeMap = new TreeMap<Double, Node>();
		try {
			NFileScan nfscan = new NFileScan(nodeHeapFileName, attrType, stringSize, (short) 2, 2, projlist, expr);
			String nodeLabel;
			Descriptor nodeDescriptor;
			Node node = new Node();
			Tuple t;
			t = nfscan.get_next();
			while (t != null) {
				node.nodeInit(t.getTupleByteArray(), t.getOffset());
				node.setHdr();
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				double distanceFromTar = nodeDescriptor.distance(targetDescriptor);
				distanceToNodeMap.put(distanceFromTar,new Node( node));
				t = nfscan.get_next();
			}
			for(double dist: distanceToNodeMap.keySet()){
				node = distanceToNodeMap.get(dist);
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				System.out.println("Label: "+nodeLabel + " , Descriptor: [" + nodeDescriptor.get(0) + " , " + nodeDescriptor.get(1) + " , "
						+ nodeDescriptor.get(2) + " , " + nodeDescriptor.get(3) + " , " + nodeDescriptor.get(4)+"]");
			}
			nfscan.close();
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Prints Node Labels that are at a given Distance from the Target Descriptor
	 * @param nhf Node Heap File
	 * @param nodeLabelLength Length of the Node Label
	 * @param numBuf Number of Buffers
	 * @param targetDescriptor Target Descriptor
	 * @param distance Target Distance
	 */
	public void query3(NodeHeapfile nhf, short nodeLabelLength, short numBuf, Descriptor targetDescriptor, double distance){
		
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

		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		expr[0].distance = distance;
		expr[0].op = new AttrOperator(AttrOperator.aopLE);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].type1 = new AttrType(AttrType.attrDesc);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer),2); 
		expr[0].operand1.attrDesc = targetDescriptor;
		expr[1] = null;
		try {
			NFileScan nfscan = new NFileScan(nodeHeapFileName, attrType, stringSize, (short) 2, 2, projlist, expr);
			String nodeLabel;
			Descriptor nodeDescriptor;
			Node node = new Node();
			Tuple t;
			t = nfscan.get_next();
			while (t != null) {
				node.nodeInit(t.getTupleByteArray(), t.getOffset());
				node.setHdr();
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				System.out.println("Label: "+nodeLabel + " , Descriptor: [" + nodeDescriptor.get(0) + " , " + nodeDescriptor.get(1) + " , "
						+ nodeDescriptor.get(2) + " , " + nodeDescriptor.get(3) + " , " + nodeDescriptor.get(4)+"]");
				t = nfscan.get_next();
			}
			nfscan.close();
		}
		
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Prints the Node Label, Incoming and Outgoing Edges based on the Node Label's match with the given Label
	 * @param nhf Node Heap File
	 * @param ehf Edge Heap File
	 * @param nodeLabelLength Length of the Node Label
	 * @param numBuf Number of Buffers
	 * @param label Node Label
	 */
	public void query4(NodeHeapfile nhf, EdgeHeapFile ehf, BTreeFile btf_node_label,short nodeLabelLength, short numBuf, String label){
		
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
		
		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].type1 = new AttrType(AttrType.attrString);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer),1); 
		expr[0].operand1.string = label;
		expr[1] = null;
		
		try {
			NFileScan nfscan = new NFileScan(nodeHeapFileName, attrType, stringSize, (short) 2, 2, projlist, expr);
			String nodeLabel;
			Descriptor nodeDescriptor;
			Node node = new Node();
			Tuple t;
			t = nfscan.get_next();
			while (t != null) {
				node.nodeInit(t.getTupleByteArray(), t.getOffset());
				node.setHdr();
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				System.out.println("Label: "+nodeLabel + " , Descriptor: [" + nodeDescriptor.get(0) + " , " + nodeDescriptor.get(1) + " , "
						+ nodeDescriptor.get(2) + " , " + nodeDescriptor.get(3) + " , " + nodeDescriptor.get(4)+"]");
				
				
				BatchInsert bInsert = new BatchInsert();
				NID nodeNID = bInsert.getNidFromNodeLabel(nodeLabel, nhf);
				
				List<String> outgoingEdges = new ArrayList<String>();
				List<String> incomingEdges = new ArrayList<String>();
				
				EID eid = new EID();
				Edge edge;
				try {

					NID sourceNID, destinationNID;
					EScan escan = ehf.openScan();
					edge = escan.getNext(eid);
					String edgeLabel;
					
					while (edge != null) {
						edge.setHdr();
						edgeLabel = edge.getLabel();
						sourceNID = edge.getSource();
						destinationNID = edge.getDestination();
						
						if(nodeNID.equals(sourceNID)){
							outgoingEdges.add(edgeLabel);
						}
						else if(nodeNID.equals(destinationNID)){
							incomingEdges.add(edgeLabel);
						}
							
						edge = escan.getNext(eid);
					}
					
					System.out.println("Incoming Edges:");
					for(String labelEdge: incomingEdges){
						System.out.println(labelEdge);
					}
					System.out.println("Outgoing Edges:");
					for(String labelEdge: outgoingEdges){
						System.out.println(labelEdge);
					}
					

				} catch (Exception e) {
					e.printStackTrace();
				}
				
				t = nfscan.get_next();
			}
			nfscan.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * Prints the Node Label, Incoming and Outgoing Edges based on the Node's Descriptor's distance from the Target Descriptor
	 * @param nhf Node Heap File
	 * @param ehf Edge Heap File
	 * @param nodeLabelLength Length of the Node Label
	 * @param numBuf Number of Buffers
	 * @param targetDescriptor Target Descriptor
	 * @param distance Target Distance
	 */
	public void query5(NodeHeapfile nhf, EdgeHeapFile ehf, BTreeFile btf_node_label,short nodeLabelLength, short numBuf, Descriptor targetDescriptor, double distance){
		
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

		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		expr[0].distance = distance;
		expr[0].op = new AttrOperator(AttrOperator.aopLE);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].type1 = new AttrType(AttrType.attrDesc);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer),2); 
		expr[0].operand1.attrDesc = targetDescriptor;
		expr[1] = null;
		try {
			NFileScan nfscan = new NFileScan(nodeHeapFileName, attrType, stringSize, (short) 2, 2, projlist, expr);
			String nodeLabel;
			Descriptor nodeDescriptor;
			Node node = new Node();
			Tuple t;
			t = nfscan.get_next();
			while (t != null) {
				node.nodeInit(t.getTupleByteArray(), t.getOffset());
				node.setHdr();
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				System.out.println("Node Label: "+nodeLabel + " , Node Descriptor: [" + nodeDescriptor.get(0) + " , " + nodeDescriptor.get(1) + " , "
						+ nodeDescriptor.get(2) + " , " + nodeDescriptor.get(3) + " , " + nodeDescriptor.get(4)+"]");
				
				
				
				BatchInsert bInsert = new BatchInsert();
				NID nodeNID = bInsert.getNidFromNodeLabel(nodeLabel, nhf);
				
				List<String> outgoingEdges = new ArrayList<String>();
				List<String> incomingEdges = new ArrayList<String>();
				
				EID eid = new EID();
				Edge edge;
				try {

					NID sourceNID, destinationNID;
					EScan escan = ehf.openScan();
					edge = escan.getNext(eid);
					String edgeLabel;
					
					while (edge != null) {
						edge.setHdr();
						edgeLabel = edge.getLabel();
						sourceNID = edge.getSource();
						destinationNID = edge.getDestination();
						
						if(nodeNID.equals(sourceNID)){
							outgoingEdges.add(edgeLabel);
						}
						else if(nodeNID.equals(destinationNID)){
							incomingEdges.add(edgeLabel);
						}
							
						edge = escan.getNext(eid);
					}
					
					System.out.println("Incoming Edges:");
					for(String labelEdge: incomingEdges){
						System.out.println(labelEdge);
					}
					System.out.println("Outgoing Edges:");
					for(String labelEdge: outgoingEdges){
						System.out.println(labelEdge);
					}
					

				} catch (Exception e) {
					e.printStackTrace();
				}
				
				t = nfscan.get_next();
			}
			nfscan.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
}