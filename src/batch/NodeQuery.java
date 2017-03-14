package batch;

import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
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

	public void query0(NodeHeapfile nhf) {
		System.out.println("query0");
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
				System.out.println(nodeLabel + " " + nodeDescriptor.get(0) + " " + nodeDescriptor.get(1) + " "
						+ nodeDescriptor.get(2) + " " + nodeDescriptor.get(3) + " " + nodeDescriptor.get(4));
				node = nscan.getNext(nid);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void query1(NodeHeapfile nhf, short nodeLabelLength, short numBuf) {
		System.out.println("query1");

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
				System.out.println(nodeLabel + " " + nodeDescriptor.get(0) + " " + nodeDescriptor.get(1) + " "
						+ nodeDescriptor.get(2) + " " + nodeDescriptor.get(3) + " " + nodeDescriptor.get(4));
				t = sort.get_next();
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

	public void query2(NodeHeapfile nhf, short nodeLabelLength, short numBuf, Descriptor targetDescriptor) {
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

		Node node;
		
		try {
			NFileScan nfscan = new NFileScan(nodeHeapFileName, attrType, stringSize, (short) 2, 2, projlist, null);
			Sort sort = new Sort(attrType, (short) 2, stringSize, nfscan, 2, order[0], nodeLabelLength, numBuf, 0,
					targetDescriptor);

			String nodeLabel;
			Descriptor nodeDescriptor;
			Tuple t;
			t = sort.get_next();

			node = (Node) sort.get_next();
			while (t != null) {
				node.nodeInit(t.getTupleByteArray(), t.getOffset());
				node.setHdr();
				nodeLabel = node.getLabel();
				nodeDescriptor = node.getDesc();
				System.out.println(nodeLabel + " " + nodeDescriptor.get(0) + " " + nodeDescriptor.get(1) + " "
						+ nodeDescriptor.get(2) + " " + nodeDescriptor.get(3) + " " + nodeDescriptor.get(4));
				t = sort.get_next();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
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

		TupleOrder[] order = new TupleOrder[2];
		order[0] = new TupleOrder(TupleOrder.Ascending);
		order[1] = new TupleOrder(TupleOrder.Descending);
		
		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		expr[0].distance = distance;
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
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
				System.out.println(nodeLabel + " " + nodeDescriptor.get(0) + " " + nodeDescriptor.get(1) + " "
						+ nodeDescriptor.get(2) + " " + nodeDescriptor.get(3) + " " + nodeDescriptor.get(4));
				t = nfscan.get_next();
			}

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}