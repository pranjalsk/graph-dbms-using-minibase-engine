package batch;

import java.io.IOException;
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
import bufmgr.PageNotReadException;
import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.EID;
import global.IndexType;
import global.NID;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import index.NodeIndexScan;
import index.UnknownIndexTypeException;
import iterator.CondExpr;
import iterator.FldSpec;
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
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
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
	 * @throws Exception
	 * @throws UnknownKeyTypeException
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws SortException
	 * @throws PredEvalException
	 * @throws TupleUtilsException
	 * @throws PageNotReadException
	 * @throws JoinsException
	 */
	public void query4(NodeHeapfile nhf, BTreeFile btf_node_label,
			EdgeHeapFile ehf, short nodeLabelLength, short numBuf,
			String targetLabelNode) throws JoinsException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception {
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
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[0].operand1.string = targetLabelNode;
		expr[0].next = null;
		expr[1] = null;

		Iterator nIscan = new IndexScan(new IndexType(IndexType.B_Index),
				nodeHeapFileName, btf_node_label.get_fileName(), attrType,
				stringSize, 2, 2, projlist, expr, 1, false);

		String edgeHeapFileName = ehf.get_fileName();
		AttrType[] edgeattrType = new AttrType[8];
		short[] edgestringSize = new short[3];
		edgestringSize[0] = nodeLabelLength;
		edgestringSize[1] = nodeLabelLength;
		edgestringSize[2] = nodeLabelLength;
		edgeattrType[0] = new AttrType(AttrType.attrInteger);
		edgeattrType[1] = new AttrType(AttrType.attrInteger);
		edgeattrType[2] = new AttrType(AttrType.attrInteger);
		edgeattrType[3] = new AttrType(AttrType.attrInteger);
		edgeattrType[4] = new AttrType(AttrType.attrString);
		edgeattrType[5] = new AttrType(AttrType.attrInteger);
		edgeattrType[6] = new AttrType(AttrType.attrString);
		edgeattrType[7] = new AttrType(AttrType.attrString);

		FldSpec[] outprojlist = new FldSpec[5];
		RelSpec innerrel = new RelSpec(RelSpec.innerRel);
		RelSpec outerrel = new RelSpec(RelSpec.outer);
		outprojlist[0] = new FldSpec(outerrel, 1);
		outprojlist[1] = new FldSpec(outerrel, 2);
		outprojlist[2] = new FldSpec(innerrel, 5);
		outprojlist[3] = new FldSpec(innerrel, 7);
		outprojlist[4] = new FldSpec(innerrel, 8);

		CondExpr orExp = new CondExpr();
		orExp = new CondExpr();
		orExp.next = null;
		orExp.op = new AttrOperator(AttrOperator.aopEQ);
		orExp.type1 = new AttrType(AttrType.attrSymbol);
		orExp.type2 = new AttrType(AttrType.attrSymbol);
		orExp.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		orExp.operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 8);

		CondExpr[] outFilter = new CondExpr[2];
		outFilter[0] = new CondExpr();
		outFilter[0].next = orExp;
		outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
				1);
		outFilter[0].operand2.symbol = new FldSpec(
				new RelSpec(RelSpec.innerRel), 7);

		Iterator am = new NestedLoopsJoins(attrType, 2, stringSize,
				edgeattrType, 8, edgestringSize, numBuf, nIscan,
				edgeHeapFileName, outFilter, null, outprojlist, 5);

		AttrType[] types = new AttrType[5];
		types[0] = new AttrType(AttrType.attrString);
		types[1] = new AttrType(AttrType.attrDesc);
		types[2] = new AttrType(AttrType.attrString);
		types[3] = new AttrType(AttrType.attrString);
		types[4] = new AttrType(AttrType.attrString);
		short[] strSizes = new short[4];
		strSizes[0] = nodeLabelLength;
		strSizes[1] = nodeLabelLength;
		strSizes[2] = nodeLabelLength;
		strSizes[3] = nodeLabelLength;
		Tuple tu;
		while ((tu = am.get_next()) != null) {
			tu.setHdr((short) 5, types, strSizes);
			if (tu.getStrFld(1).equalsIgnoreCase(tu.getStrFld(5))) {
				System.out.println("Node label: " + tu.getStrFld(1)
						+ " Descriptor: [" + tu.getDescFld(2)
						+ "] Incoming edge: " + tu.getStrFld(3));
			} else {
				System.out.println("Node label: " + tu.getStrFld(1)
						+ " Descriptor: [" + tu.getDescFld(2)
						+ "] Outgoing edge: " + tu.getStrFld(3));
			}
		}
		nIscan.close();
		am.close();
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
	 * @throws Exception
	 * @throws FieldNumberOutOfBoundException
	 * @throws UnknownKeyTypeException
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws SortException
	 * @throws PredEvalException
	 * @throws TupleUtilsException
	 * @throws PageNotReadException
	 * @throws JoinsException
	 */
	public void query5(NodeHeapfile nhf, ZTreeFile ztf_Descriptor,
			BTreeFile btf_node_label, EdgeHeapFile ehf, short nodeLabelLength,
			short numBuf, Descriptor targetDescriptor, double distance)
			throws JoinsException, PageNotReadException, TupleUtilsException,
			PredEvalException, SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, FieldNumberOutOfBoundException, Exception {
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
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		expr[0].operand1.attrDesc = targetDescriptor;
		expr[1] = null;

		Iterator nIscan = new IndexScan(new IndexType(IndexType.Z_Index),
				nodeHeapFileName, ztf_Descriptor.get_fileName(), attrType,
				stringSize, 2, 2, projlist, expr, 2, false);

		String edgeHeapFileName = ehf.get_fileName();
		AttrType[] edgeattrType = new AttrType[8];
		short[] edgestringSize = new short[3];
		edgestringSize[0] = nodeLabelLength;
		edgestringSize[1] = nodeLabelLength;
		edgestringSize[2] = nodeLabelLength;
		edgeattrType[0] = new AttrType(AttrType.attrInteger);
		edgeattrType[1] = new AttrType(AttrType.attrInteger);
		edgeattrType[2] = new AttrType(AttrType.attrInteger);
		edgeattrType[3] = new AttrType(AttrType.attrInteger);
		edgeattrType[4] = new AttrType(AttrType.attrString);
		edgeattrType[5] = new AttrType(AttrType.attrInteger);
		edgeattrType[6] = new AttrType(AttrType.attrString);
		edgeattrType[7] = new AttrType(AttrType.attrString);

		FldSpec[] outprojlist = new FldSpec[5];
		RelSpec innerrel = new RelSpec(RelSpec.innerRel);
		RelSpec outerrel = new RelSpec(RelSpec.outer);
		outprojlist[0] = new FldSpec(outerrel, 1);
		outprojlist[1] = new FldSpec(outerrel, 2);
		outprojlist[2] = new FldSpec(innerrel, 5);
		outprojlist[3] = new FldSpec(innerrel, 7);
		outprojlist[4] = new FldSpec(innerrel, 8);

		CondExpr orExp = new CondExpr();
		orExp = new CondExpr();
		orExp.next = null;
		orExp.op = new AttrOperator(AttrOperator.aopEQ);
		orExp.type1 = new AttrType(AttrType.attrSymbol);
		orExp.type2 = new AttrType(AttrType.attrSymbol);
		orExp.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		orExp.operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 8);

		CondExpr[] outFilter = new CondExpr[2];
		outFilter[0] = new CondExpr();
		outFilter[0].next = orExp;
		outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
				1);
		outFilter[0].operand2.symbol = new FldSpec(
				new RelSpec(RelSpec.innerRel), 7);

		Iterator am = new NestedLoopsJoins(attrType, 2, stringSize,
				edgeattrType, 8, edgestringSize, numBuf, nIscan,
				edgeHeapFileName, outFilter, null, outprojlist, 5);

		AttrType[] types = new AttrType[5];
		types[0] = new AttrType(AttrType.attrString);
		types[1] = new AttrType(AttrType.attrDesc);
		types[2] = new AttrType(AttrType.attrString);
		types[3] = new AttrType(AttrType.attrString);
		types[4] = new AttrType(AttrType.attrString);
		short[] strSizes = new short[4];
		strSizes[0] = nodeLabelLength;
		strSizes[1] = nodeLabelLength;
		strSizes[2] = nodeLabelLength;
		strSizes[3] = nodeLabelLength;
		Tuple tu;
		while ((tu = am.get_next()) != null) {
			tu.setHdr((short) 5, types, strSizes);
			if (tu.getStrFld(1).equalsIgnoreCase(tu.getStrFld(5))) {
				System.out.println("Node label: " + tu.getStrFld(1)
						+ " Descriptor: [" + tu.getDescFld(2)
						+ "] Incoming edge: " + tu.getStrFld(3));
			} else {
				System.out.println("Node label: " + tu.getStrFld(1)
						+ " Descriptor: [" + tu.getDescFld(2)
						+ "] Outgoing edge: " + tu.getStrFld(3));
			}
		}
		nIscan.close();
		am.close();
	}
}