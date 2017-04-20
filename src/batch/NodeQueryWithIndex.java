package batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import zindex.ZTreeFile;

import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.EdgeHeapFile;
import btree.BTFileScan;
import btree.BTreeFile;
import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.EID;
import global.IndexType;
import global.NID;
import global.TupleOrder;
import heap.Tuple;
import index.NodeIndexScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.NFileScan;
import iterator.RelSpec;
import iterator.Sort;
import nodeheap.NScan;
import nodeheap.Node;
import nodeheap.NodeHeapfile;

public class NodeQueryWithIndex {

	/**
	 * Prints Node data in the order of occurrence/storage in Node Heap File
	 * 
	 * @param nhf
	 *            Node Heap File
	 * @param btf
	 *            BTree File
	 * @param nodeLabelLength
	 *            Length of the Node Label
	 * @param numBuf
	 *            Number of Buffers
	 */
	public void query0(NodeHeapfile nhf, BTreeFile btf_node_label,
			short nodeLabelLength, short numBuf) {
		NID nid = new NID();
		Node node;
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

		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		IndexType indType = new IndexType(1);
		try {
			NScan nscan = nhf.openScan();
			node = nscan.getNext(nid);
			String targetnodeLabel;

			while (node != null) {
				node.setHdr();
				targetnodeLabel = node.getLabel();
				expr[0].op = new AttrOperator(AttrOperator.aopEQ);
				expr[0].type2 = new AttrType(AttrType.attrSymbol);
				expr[0].type1 = new AttrType(AttrType.attrString);
				expr[0].operand2.symbol = new FldSpec(
						new RelSpec(RelSpec.outer), 1);
				expr[0].operand1.string = targetnodeLabel;
				expr[1] = null;
				NodeIndexScan nIscan = new NodeIndexScan(indType,
						nodeHeapFileName, nodeIndexFileName, attrType,
						stringSize, 2, 2, projlist, expr, 1, false);
				node = nIscan.get_next();
				String nodeLabel;
				Descriptor nodeDescriptor;
				if (node != null) {
					nodeLabel = node.getLabel();
					nodeDescriptor = node.getDesc();
					System.out.println("Label: " + nodeLabel
							+ " , Descriptor: [" + nodeDescriptor.get(0)
							+ " , " + nodeDescriptor.get(1) + " , "
							+ nodeDescriptor.get(2) + " , "
							+ nodeDescriptor.get(3) + " , "
							+ nodeDescriptor.get(4) + "]");
				}
				node = nscan.getNext(nid);
				nIscan.close();
			}
			nscan.closescan();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Prints Node data based on increasing alpha-numerical order of Node Label
	 * 
	 * @param nhf
	 *            Node Heap File
	 * @param btf
	 *            BTree File
	 * @param nodeLabelLength
	 *            Length of the Node Label
	 * @param numBuf
	 *            Number of Buffers
	 */
	public void query1(NodeHeapfile nhf, BTreeFile btf_node_label,
			short nodeLabelLength, short numBuf) {
		System.out.println("query1");

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
			Descriptor nodeDescriptor;

			while (node != null) {
				node.setHdr();
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				System.out.println("Label: " + nodeLabel + " , Descriptor: ["
						+ nodeDescriptor.get(0) + " , " + nodeDescriptor.get(1)
						+ " , " + nodeDescriptor.get(2) + " , "
						+ nodeDescriptor.get(3) + " , " + nodeDescriptor.get(4)
						+ "]");
				node = nIscan.get_next();
			}
			nIscan.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Prints Node data based on increasing order of distance of Node's
	 * Descriptor from the Target Descriptor
	 * 
	 * @param nhf
	 *            Node Heap File
	 * @param ztf
	 *            ZTree File
	 * @param nodeLabelLength
	 *            Length of the Node Label
	 * @param numBuf
	 *            Number of Buffers
	 * @param targetDescriptor
	 *            Target Descriptor
	 * @param distance
	 *            Target Distance
	 */
	public void query2(NodeHeapfile nhf, ZTreeFile ztf_Descriptor,
			short nodeLabelLength, short numBuf, Descriptor targetDescriptor,
			double distance) {

		String nodeHeapFileName = nhf.get_fileName();
		String nodeIndexFileName = "";
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

		IndexType indType = new IndexType(3);
		Node node = new Node();
		Map<Double, Node> distanceToNodeMap = new TreeMap<Double, Node>();
		try {
			NodeIndexScan nIscan = new NodeIndexScan(indType, nodeHeapFileName,
					nodeIndexFileName, attrType, stringSize, 2, 2, projlist,
					expr, 1, false);
			node = nIscan.get_next();
			String nodeLabel;
			Descriptor nodeDescriptor;

			while (node != null) {
				node.setHdr();
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				double distanceFromTar = nodeDescriptor
						.distance(targetDescriptor);
				distanceToNodeMap.put(distanceFromTar, new Node(node));
				node = nIscan.get_next();
			}

			for (double dist : distanceToNodeMap.keySet()) {
				node = distanceToNodeMap.get(dist);
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				System.out.println("Label: " + nodeLabel + " , Descriptor: ["
						+ nodeDescriptor.get(0) + " , " + nodeDescriptor.get(1)
						+ " , " + nodeDescriptor.get(2) + " , "
						+ nodeDescriptor.get(3) + " , " + nodeDescriptor.get(4)
						+ "]");
			}
			nIscan.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Prints Node Labels that are at a given Distance from the Target
	 * Descriptor
	 * 
	 * @param nhf
	 *            Node Heap File
	 * @param ztf
	 *            ZTree File
	 * @param nodeLabelLength
	 *            Length of the Node Label
	 * @param numBuf
	 *            Number of Buffers
	 * @param targetDescriptor
	 *            Target Descriptor
	 * @param distance
	 *            Target Distance
	 */
	public void query3(NodeHeapfile nhf, ZTreeFile ztf_Descriptor,
			short nodeLabelLength, short numBuf, Descriptor targetDescriptor,
			double distance) {

		String nodeHeapFileName = nhf.get_fileName();
		String nodeIndexFileName = "";
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
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		expr[0].operand1.attrDesc = targetDescriptor;
		expr[1] = null;

		IndexType indType = new IndexType(3);
		Node node = new Node();
		try {
			NodeIndexScan nIscan = new NodeIndexScan(indType, nodeHeapFileName,
					nodeIndexFileName, attrType, stringSize, 2, 2, projlist,
					expr, 1, false);
			node = nIscan.get_next();
			String nodeLabel;
			Descriptor nodeDescriptor;

			while (node != null) {
				node.setHdr();
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				System.out.println("Label: " + nodeLabel + " , Descriptor: ["
						+ nodeDescriptor.get(0) + " , " + nodeDescriptor.get(1)
						+ " , " + nodeDescriptor.get(2) + " , "
						+ nodeDescriptor.get(3) + " , " + nodeDescriptor.get(4)
						+ "]");
				node = nIscan.get_next();
			}
			nIscan.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Prints the Node Label, Incoming and Outgoing Edges based on the Node
	 * Label's match with the given Label
	 * 
	 * @param nhf
	 *            Node Heap File
	 * @param btf
	 *            BTree File
	 * @param ehf
	 *            Edge Heap File
	 * @param nodeLabelLength
	 *            Length of the Node Label
	 * @param numBuf
	 *            Number of Buffers
	 * @param targetLabelNode
	 *            Node Label
	 */
	public void query4(NodeHeapfile nhf, BTreeFile btf_node_label,
			EdgeHeapFile ehf, short nodeLabelLength, short numBuf,
			String targetLabelNode) {
		System.out.println("query1");

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

		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].type1 = new AttrType(AttrType.attrString);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[0].operand1.string = targetLabelNode;
		expr[1] = null;
		IndexType indType = new IndexType(1);
		Node node = new Node();
		try {
			NodeIndexScan nIscan = new NodeIndexScan(indType, nodeHeapFileName,
					nodeIndexFileName, attrType, stringSize, 2, 2, projlist,
					expr, 1, false);
			node = nIscan.get_next();
			String nodeLabel;
			Descriptor nodeDescriptor;

			while (node != null) {
				node.setHdr();
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				System.out.println("Label: " + nodeLabel + " , Descriptor: ["
						+ nodeDescriptor.get(0) + " , " + nodeDescriptor.get(1)
						+ " , " + nodeDescriptor.get(2) + " , "
						+ nodeDescriptor.get(3) + " , " + nodeDescriptor.get(4)
						+ "]");
				BatchMapperClass bInsert = new BatchMapperClass();
				NID nodeNID = bInsert.getNidFromNodeLabel(nodeLabel, nhf, btf_node_label);

				EdgeHeapFile tempIncomingEdgeFile = new EdgeHeapFile(null);
				EdgeHeapFile tempOutgoingEdgeFile = new EdgeHeapFile(null);

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
							tempOutgoingEdgeFile.insertEdge(edge.getEdgeByteArray());
						}
						else if(nodeNID.equals(destinationNID)){
							tempIncomingEdgeFile.insertEdge(edge.getEdgeByteArray());
						}

						edge = escan.getNext(eid);
					}
					escan.closescan();
					
					EScan incoming_edge_scan = tempIncomingEdgeFile.openScan();
					EScan outgoing_edge_scan = tempOutgoingEdgeFile.openScan();
					
					edge = incoming_edge_scan.getNext(eid);
					System.out.println("Incoming Edges:");
					while (edge != null) {
						edge.setHdr();
						System.out.println(edge.getLabel());
						edge = incoming_edge_scan.getNext(eid);
					}
					incoming_edge_scan.closescan();
					edge = outgoing_edge_scan.getNext(eid);
					System.out.println("Outgoing Edges:");
					while (edge != null) {
						edge.setHdr();
						System.out.println(edge.getLabel());
						edge = outgoing_edge_scan.getNext(eid);
					}
					outgoing_edge_scan.closescan();
					
					tempIncomingEdgeFile.deleteFile();
					tempOutgoingEdgeFile.deleteFile();
					
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
	 * Prints the Node Label, Incoming and Outgoing Edges based on the Node's
	 * Descriptor's distance from the Target Descriptor
	 * 
	 * @param nhf
	 *            Node Heap File
	 * @param ztf
	 *            ZTRee File
	 * @param ehf
	 *            Edge Heap File
	 * @param nodeLabelLength
	 *            Length of the Node Label
	 * @param numBuf
	 *            Number of Buffers
	 * @param targetDescriptor
	 *            Target Descriptor
	 * @param distance
	 *            Target Distance
	 */
	public void query5(NodeHeapfile nhf, ZTreeFile ztf_Descriptor, BTreeFile btf_node_label,
			EdgeHeapFile ehf, short nodeLabelLength, short numBuf,
			Descriptor targetDescriptor, double distance) {

		String nodeHeapFileName = nhf.get_fileName();
		String nodeIndexFileName = "";
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
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		expr[0].operand1.attrDesc = targetDescriptor;
		expr[1] = null;

		IndexType indType = new IndexType(3);
		Node node = new Node();
		try {
			NodeIndexScan nIscan = new NodeIndexScan(indType, nodeHeapFileName,
					nodeIndexFileName, attrType, stringSize, 2, 2, projlist,
					expr, 1, false);
			node = nIscan.get_next();
			String nodeLabel;
			Descriptor nodeDescriptor;

			while (node != null) {
				node.setHdr();
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				System.out.println("Label: " + nodeLabel + " , Descriptor: ["
						+ nodeDescriptor.get(0) + " , " + nodeDescriptor.get(1)
						+ " , " + nodeDescriptor.get(2) + " , "
						+ nodeDescriptor.get(3) + " , " + nodeDescriptor.get(4)
						+ "]");
				BatchMapperClass bInsert = new BatchMapperClass();
				NID nodeNID = bInsert.getNidFromNodeLabel(nodeLabel, nhf, btf_node_label);

				EdgeHeapFile tempIncomingEdgeFile = new EdgeHeapFile(null);
				EdgeHeapFile tempOutgoingEdgeFile = new EdgeHeapFile(null);

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
							tempOutgoingEdgeFile.insertEdge(edge.getEdgeByteArray());
						}
						else if(nodeNID.equals(destinationNID)){
							tempIncomingEdgeFile.insertEdge(edge.getEdgeByteArray());
						}

						edge = escan.getNext(eid);
					}
					escan.closescan();

					EScan incoming_edge_scan = tempIncomingEdgeFile.openScan();
					EScan outgoing_edge_scan = tempOutgoingEdgeFile.openScan();
					
					edge = incoming_edge_scan.getNext(eid);
					System.out.println("Incoming Edges:");
					while (edge != null) {
						edge.setHdr();
						System.out.println(edge.getLabel());
						edge = incoming_edge_scan.getNext(eid);
					}
					incoming_edge_scan.closescan();
					edge = outgoing_edge_scan.getNext(eid);
					System.out.println("Outgoing Edges:");
					while (edge != null) {
						edge.setHdr();
						System.out.println(edge.getLabel());
						edge = outgoing_edge_scan.getNext(eid);
					}
					outgoing_edge_scan.closescan();
					
					tempIncomingEdgeFile.deleteFile();
					tempOutgoingEdgeFile.deleteFile();
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
}