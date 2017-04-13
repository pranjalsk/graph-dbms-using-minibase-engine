package batch;

import global.AttrType;
import global.NID;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Tuple;

import iterator.Iterator;

import java.util.ArrayList;
import java.util.List;

import nodeheap.Node;
import nodeheap.NodeHeapfile;

public class PathExpressionQuery {

	public void pathExpressQuery1(String pathExpression, String nhfName,
			String ehfName, String indexEhfSourceNodeName,
			String indexNodeLabelName, short numBuf, short nodeLabelLength)
			throws InvalidSlotNumberException, InvalidTupleSizeException,
			Exception {
		
		PathExpressionParser parsr = new PathExpressionParser();
		List<AttrType[]> attrTypeList = new ArrayList<AttrType[]>();
		List<Object[]> objExpList = parsr.pathExpressionQuery1Parser(
				attrTypeList, pathExpression);
		PathExpression pathExp = new PathExpression();

		NodeHeapfile nhf = new NodeHeapfile(nhfName);
		Heapfile pathExprQuery1Result = new Heapfile("pathExprQuery1Result");
		
		for (int i = 0; i < objExpList.size(); i++) {
			Object[] expression = objExpList.get(i);
			AttrType[] attr = attrTypeList.get(i);
			Iterator tailNodeIds = pathExp.pathExpress1(expression, attr,
					nhfName, ehfName, indexEhfSourceNodeName,
					indexNodeLabelName, numBuf, nodeLabelLength);
			Tuple tail;
			NID headNID = (NID) expression[0];
			Node headNode = nhf.getRecord(headNID);
			headNode.setHdr();
			Node tailNode;
			Tuple headTailPair = new Tuple();
			headTailPair.setHdr((short) 2, new AttrType[] {
					new AttrType(AttrType.attrString),
					new AttrType(AttrType.attrString) }, new short[] {
					(short) 32, (short) 32 });
			while ((tail = tailNodeIds.get_next()) != null) {
				tail.setHdr((short) 1, new AttrType[] { new AttrType(
						AttrType.attrId) }, new short[] {});
				NID tailNid = (NID) tail.getIDFld(1);
				tailNode = nhf.getRecord(tailNid);
				tailNode.setHdr();
				headTailPair.setStrFld(1, headNode.getLabel());
				headTailPair.setStrFld(2, tailNode.getLabel());
				pathExprQuery1Result.insertRecord(headTailPair.getTupleByteArray());
			}
		}

	}

	public void pathExpressQuery2(String pathExpression, String nhfName,
			String ehfName, String indexEhfSourceNodeName,
			String indexNodeLabelName, short numBuf, short nodeLabelLength)
			throws InvalidSlotNumberException, InvalidTupleSizeException, Exception {

		PathExpressionParser parsr = new PathExpressionParser();
		List<AttrType[]> attrTypeList = new ArrayList<AttrType[]>();
		List<Object[]> objExpList = parsr.pathExpressionQuery2Parser(
				attrTypeList, pathExpression);
		PathExpression pathExp = new PathExpression();

		NodeHeapfile nhf = new NodeHeapfile(nhfName);
		Heapfile pathExprQuery2Result = new Heapfile("pathExprQuery2Result");
		
		for (int i = 0; i < objExpList.size(); i++) {
			Object[] expression = objExpList.get(i);
			AttrType[] attr = attrTypeList.get(i);
			Iterator tailNodeIds = pathExp.pathExpress2(expression, attr,
					nhfName, ehfName, indexEhfSourceNodeName,
					indexNodeLabelName, numBuf, nodeLabelLength);
			Tuple tail;
			NID headNID = (NID) expression[0];
			Node headNode = nhf.getRecord(headNID);
			headNode.setHdr();
			Node tailNode;
			Tuple headTailPair = new Tuple();
			headTailPair.setHdr((short) 2, new AttrType[] {
					new AttrType(AttrType.attrString),
					new AttrType(AttrType.attrString) }, new short[] {
					(short) 32, (short) 32 });
			while ((tail = tailNodeIds.get_next()) != null) {
				tail.setHdr((short) 1, new AttrType[] { new AttrType(
						AttrType.attrId) }, new short[] {});
				NID tailNid = (NID) tail.getIDFld(1);
				tailNode = nhf.getRecord(tailNid);
				tailNode.setHdr();
				headTailPair.setStrFld(1, headNode.getLabel());
				headTailPair.setStrFld(2, tailNode.getLabel());
				pathExprQuery2Result.insertRecord(headTailPair.getTupleByteArray());
			}
		}
	}
}
