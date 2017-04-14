package batch;

import global.AttrType;
import global.NID;
import global.TupleOrder;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import index.IndexException;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import bufmgr.PageNotReadException;

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
		List<Object[]> objExpList = new ArrayList<Object[]>();
		int type = parsr.pathExpressionQuery1Parser(objExpList, attrTypeList, pathExpression);
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
			tailNodeIds.close();
			
			switch(type){
			case 0:
				typeA("pathExprQuery1Result");
				break;
			case 1:
				typeB("pathExprQuery1Result", numBuf);
				break;
			case 2:
				typeC("pathExprQuery1Result", numBuf);
				break;
			}
		}

	}

	public void pathExpressQuery2(String pathExpression, String nhfName,
			String ehfName, String indexEhfSourceNodeName,
			String indexNodeLabelName, short numBuf, short nodeLabelLength)
			throws InvalidSlotNumberException, InvalidTupleSizeException, Exception {

		PathExpressionParser parsr = new PathExpressionParser();
		List<AttrType[]> attrTypeList = new ArrayList<AttrType[]>();
		List<Object[]> objExpList = new ArrayList<Object[]>();
		int type = parsr.pathExpressionQuery2Parser(objExpList, attrTypeList, pathExpression);
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
			tailNodeIds.close();
			
			switch(type){
			case 0:
				typeA("pathExprQuery2Result");
				break;
			case 1:
				typeB("pathExprQuery2Result", numBuf);
				break;
			case 2:
				typeC("pathExprQuery2Result", numBuf);
				break;
			}
		}
	}
	
	private void typeA(String fileName) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
		Heapfile pathExprQuery1Result = new Heapfile(fileName);
		AttrType[] type = new AttrType[2];
		type[0] = new AttrType(AttrType.attrString);
		type[1] = new AttrType(AttrType.attrString);
		short[] str_sizes = new short[2];
		str_sizes[0] =	(short) 32;
		str_sizes[1] =(short) 32;
		FldSpec[] proj_list = new FldSpec[2];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		
		Iterator resultScanner = new FileScan(fileName, type, str_sizes, (short)2, (short)2, proj_list, null);
		
		Tuple res;
		while((res = resultScanner.get_next()) != null){
			res.setHdr((short)2, type, str_sizes);
			res.print(type);
		}
		
		resultScanner.close();
		pathExprQuery1Result.deleteFile();
	}
	
	private void typeB(String fileName, int numBuf) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
		Heapfile pathExprQuery1Result = new Heapfile(fileName);
		AttrType[] type = new AttrType[2];
		type[0] = new AttrType(AttrType.attrString);
		type[1] = new AttrType(AttrType.attrString);
		short[] str_sizes = new short[2];
		str_sizes[0] =	(short) 32;
		str_sizes[1] =(short) 32;
		FldSpec[] proj_list = new FldSpec[2];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		
		Iterator resultScanner = new FileScan(fileName, type, str_sizes, (short)2, (short)2, proj_list, null);
		
		
		Iterator tailNodesSort = new Sort(type, (short)2, str_sizes, resultScanner, 2, new TupleOrder(0), 32, numBuf);
		
		Iterator headNodesSort = new Sort(type, (short)2, str_sizes, tailNodesSort, 1, new TupleOrder(0), 32, numBuf);
		
		Tuple res;
		while((res = headNodesSort.get_next()) != null){
			res.setHdr((short)2, type, str_sizes);
			res.print(type);
		}
		
		headNodesSort.close();
		pathExprQuery1Result.deleteFile();
	}
	
	private void typeC(String fileName, int numBuf) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception{
		Heapfile pathExprQuery1Result = new Heapfile(fileName);
		AttrType[] type = new AttrType[2];
		type[0] = new AttrType(AttrType.attrString);
		type[1] = new AttrType(AttrType.attrString);
		short[] str_sizes = new short[2];
		str_sizes[0] =	(short) 32;
		str_sizes[1] =(short) 32;
		FldSpec[] proj_list = new FldSpec[2];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		
		Iterator resultScanner = new FileScan(fileName, type, str_sizes, (short)2, (short)2, proj_list, null);
		
		
		Iterator tailNodesSort = new Sort(type, (short)2, str_sizes, resultScanner, 2, new TupleOrder(0), 32, numBuf);
		
		Iterator headNodesSort = new Sort(type, (short)2, str_sizes, tailNodesSort, 1, new TupleOrder(0), 32, numBuf);
		
		Tuple prevRes = null;
		Tuple res;
		while((res = headNodesSort.get_next()) != null){
			res.setHdr((short)2, type, str_sizes);
			if(prevRes == null || !(prevRes.getStrFld(1) == res.getStrFld(1) && prevRes.getStrFld(2) == res.getStrFld(2))){
				res.print(type);
				prevRes = res;
			}
		}
		
		headNodesSort.close();
		pathExprQuery1Result.deleteFile();
	}
}
