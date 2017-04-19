package batch;

import edgeheap.EdgeHeapFile;
import global.AttrOperator;
import global.AttrType;
import global.NID;
import global.PageId;
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
import iterator.CondExpr;
import iterator.EFileScan;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.IndexNestedLoopsJoins;
import iterator.InvalidRelation;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NestedLoopsJoins;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.SortMerge;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import zindex.ZTreeFile;

import btree.BTreeFile;
import bufmgr.PageNotReadException;

import nodeheap.Node;
import nodeheap.NodeHeapfile;

public class PathExpressionQuery {

	public void pathExpressQuery1(String pathExpression, NodeHeapfile nhfRef,
			EdgeHeapFile ehfRef, BTreeFile btf_edge_src_label,
			BTreeFile btf_node_label, ZTreeFile ztf_node_desc, short numBuf,
			short nodeLabelLength) throws InvalidSlotNumberException,
			InvalidTupleSizeException, Exception {

		String nhfName = nhfRef.get_fileName();
		String ehfName = ehfRef.get_fileName();
		String indexEhfSourceNodeName = btf_edge_src_label.get_fileName();
		String indexNodeLabelName = btf_node_label.get_fileName();

		PathExpressionParser parsr = new PathExpressionParser();
		List<AttrType[]> attrTypeList = new ArrayList<AttrType[]>();
		List<Object[]> objExpList = new ArrayList<Object[]>();

		int type = parsr.pathExpressionQuery1Parser(objExpList, attrTypeList,
				pathExpression, btf_node_label, nhfRef, ztf_node_desc);

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
				NID tailNid = new NID(tail.getIDFld(1).pageNo,
						tail.getIDFld(1).slotNo);
				tailNode = nhf.getRecord(tailNid);
				tailNode.setHdr();
				headTailPair.setStrFld(1, headNode.getLabel());
				headTailPair.setStrFld(2, tailNode.getLabel());
				pathExprQuery1Result.insertRecord(headTailPair
						.getTupleByteArray());
			}
			tailNodeIds.close();

		}

		switch (type) {
		case 0:
			System.out.println("type a");
			typeA("pathExprQuery1Result");
			break;
		case 1:
			System.out.println("type b");
			typeB("pathExprQuery1Result", numBuf);
			break;
		case 2:
			System.out.println("type c");
			typeC("pathExprQuery1Result", numBuf);
			break;
		}	
	}

	public void pathExpressQuery2(String pathExpression, NodeHeapfile nhfRef,
			EdgeHeapFile ehfRef, BTreeFile btf_edge_src_label,
			BTreeFile btf_node_label, ZTreeFile ztf_node_desc, short numBuf,
			short nodeLabelLength) throws InvalidSlotNumberException,
			InvalidTupleSizeException, Exception {

		String nhfName = nhfRef.get_fileName();
		String ehfName = ehfRef.get_fileName();
		String indexEhfSourceNodeName = btf_edge_src_label.get_fileName();
		String indexNodeLabelName = btf_node_label.get_fileName();

		PathExpressionParser parsr = new PathExpressionParser();
		List<AttrType[]> attrTypeList = new ArrayList<AttrType[]>();
		List<Object[]> objExpList = new ArrayList<Object[]>();
		int type = parsr.pathExpressionQuery2Parser(objExpList, attrTypeList,
				pathExpression, nhfRef, ztf_node_desc, btf_node_label);
		PathExpression pathExp = new PathExpression();
		NodeHeapfile nhf = new NodeHeapfile(nhfName);
		
		Heapfile pathExprQuery2Result = new Heapfile("pathExprQuery2Result");
		//DummyHeadTailPair hadTailPair = new DummyHeadTailPair(64);

		for (int i = 0; i < objExpList.size(); i++) {
			Object[] expression = objExpList.get(i);
			AttrType[] attr = attrTypeList.get(i);
			Iterator tailNodeIds = pathExp.pathExpress2(expression, attr,
					nhfName, ehfName, indexEhfSourceNodeName,
					indexNodeLabelName, numBuf, nodeLabelLength);
			Tuple tail;
			NID headNID = (NID) expression[0];
			Node headNode = nhf.getRecord(headNID);
			
			Node tailNode;
			
			while ((tail = tailNodeIds.get_next()) != null) {
				headNode.setHdr();
				
				Tuple headTailPair = new Tuple();
				headTailPair.setHdr((short) 2, new AttrType[] {
						new AttrType(AttrType.attrString),
						new AttrType(AttrType.attrString) }, new short[] {
						(short) 32, (short) 32 });
				
				int size = headTailPair.size();
				headTailPair = new Tuple(size);

				headTailPair.setHdr((short) 2, new AttrType[] {
						new AttrType(AttrType.attrString),
						new AttrType(AttrType.attrString) }, new short[] {
						(short) 32, (short) 32 });			
				
								
				tail.setHdr((short) 1, new AttrType[] { new AttrType(
						AttrType.attrId) }, new short[] {});
				NID tailNid = (NID) new NID(tail.getIDFld(1).pageNo,
						tail.getIDFld(1).slotNo);
//				System.out.println(tailNid);
				tailNode = nhf.getRecord(tailNid);
				tailNode.setHdr();
				
				headTailPair.setStrFld(1, headNode.getLabel());
				headTailPair.setStrFld(2, tailNode.getLabel());

//				System.out.println(headNode.getLabel() + ":"
//						+ tailNode.getLabel());
				pathExprQuery2Result.insertRecord(headTailPair
						.getTupleByteArray());
			}
			tailNodeIds.close();

			
		}
		switch (type) {
		case 0:
			System.out.println("type a");
			typeA("pathExprQuery2Result");
			break;
		case 1:
			System.out.println("type b");
			typeB("pathExprQuery2Result", numBuf);
			break;
		case 2:
			System.out.println("type c");
			typeC("pathExprQuery2Result", numBuf);
			break;
		}
	}

	private void typeA(String fileName) throws JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		Heapfile pathExprQuery1Result = new Heapfile(fileName);
		AttrType[] type = new AttrType[2];
		type[0] = new AttrType(AttrType.attrString);
		type[1] = new AttrType(AttrType.attrString);
		short[] str_sizes = new short[2];
		str_sizes[0] = (short) 32;
		str_sizes[1] = (short) 32;
		FldSpec[] proj_list = new FldSpec[2];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

		Iterator resultScanner = new FileScan(fileName, type, str_sizes,
				(short) 2, (short) 2, proj_list, null);

		Tuple res;
		while ((res = resultScanner.get_next()) != null) {
			res.setHdr((short) 2, type, str_sizes);
			res.print(type);
		}

		resultScanner.close();
		pathExprQuery1Result.deleteFile();
	}

	private void typeB(String fileName, int numBuf) throws JoinsException,
			IndexException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		Heapfile pathExprQuery1Result = new Heapfile(fileName);
		AttrType[] type = new AttrType[2];
		type[0] = new AttrType(AttrType.attrString);
		type[1] = new AttrType(AttrType.attrString);
		short[] str_sizes = new short[2];
		str_sizes[0] = (short) 32;
		str_sizes[1] = (short) 32;
		FldSpec[] proj_list = new FldSpec[2];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

		Iterator resultScanner = new FileScan(fileName, type, str_sizes,
				(short) 2, (short) 2, proj_list, null);

		Iterator tailNodesSort = new Sort(type, (short) 2, str_sizes,
				resultScanner, 1, new TupleOrder(0), 32, numBuf);

		Iterator headNodesSort = new Sort(type, (short) 2, str_sizes,
				tailNodesSort, 2, new TupleOrder(0), 32, numBuf);

		Tuple res;
		while ((res = headNodesSort.get_next()) != null) {
			res.setHdr((short) 2, type, str_sizes);
			res.print(type);
		}

		headNodesSort.close();
		pathExprQuery1Result.deleteFile();
	}

	private void typeC(String fileName, int numBuf) throws JoinsException,
			IndexException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		Heapfile pathExprQuery1Result = new Heapfile(fileName);
		AttrType[] type = new AttrType[2];
		type[0] = new AttrType(AttrType.attrString);
		type[1] = new AttrType(AttrType.attrString);
		short[] str_sizes = new short[2];
		str_sizes[0] = (short) 32;
		str_sizes[1] = (short) 32;
		FldSpec[] proj_list = new FldSpec[2];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

		Iterator resultScanner = new FileScan(fileName, type, str_sizes,
				(short) 2, (short) 2, proj_list, null);

		Iterator tailNodesSort = new Sort(type, (short) 2, str_sizes,
				resultScanner, 2, new TupleOrder(0), 32, numBuf);

		Iterator headNodesSort = new Sort(type, (short) 2, str_sizes,
				tailNodesSort, 1, new TupleOrder(0), 32, numBuf);

		Tuple prevRes = null;
		Tuple res;
		while ((res = headNodesSort.get_next()) != null) {
			res.setHdr((short) 2, type, str_sizes);
			if (prevRes == null
					|| !(prevRes.getStrFld(1)
							.equalsIgnoreCase(res.getStrFld(1)) && prevRes
							.getStrFld(2).equalsIgnoreCase(res.getStrFld(2)))) {
				prevRes = new Tuple(res);
				res.print(type);
			}
		}

		headNodesSort.close();
		pathExprQuery1Result.deleteFile();
	}

	public void triangleQuery(String trianglePathExpression, String nhfName,
			String ehfName, String indexEhfSourceNodeName,
			String indexNodeLabelName, short numBuf, short nodeLabelLength)
			throws JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException,
			PredEvalException, SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception {

		PathExpressionParser parsr = new PathExpressionParser();
		AttrType[] attrTypes = new AttrType[3];
		Object[] objExpressions = new Object[3];
		int type = parsr.triangleQueryParser(objExpressions, attrTypes,
				trianglePathExpression);
		PathExpression pathExp = new PathExpression();

		/*************************/
		/*
		 * attrTypes[0] = new AttrType(AttrType.attrInteger); attrTypes[1] = new
		 * AttrType(AttrType.attrInteger); attrTypes[2] = new
		 * AttrType(AttrType.attrInteger);
		 * 
		 * objExpressions[0] = new Integer(50); objExpressions[1] = new
		 * Integer(50); objExpressions[2] = new Integer(50);
		 */
		/*************************/
		Heapfile triangleQueryResult = new Heapfile("triangleQueryResult");
		Iterator am1 = getTriNodeEdgePair(objExpressions, attrTypes, ehfName,
				numBuf);

		Iterator am2 = getThirdConnectingEdge(objExpressions, attrTypes,
				ehfName, indexEhfSourceNodeName, am1, numBuf);

		AttrType[] types = new AttrType[7];
		types[0] = new AttrType(AttrType.attrString);
		types[1] = new AttrType(AttrType.attrInteger);
		types[2] = new AttrType(AttrType.attrString);
		types[3] = new AttrType(AttrType.attrString);
		types[4] = new AttrType(AttrType.attrString);
		types[5] = new AttrType(AttrType.attrString);
		types[6] = new AttrType(AttrType.attrString);
		/*******************************************/
		/*
		 * AttrType[] types = new AttrType[8]; types[0] = new
		 * AttrType(AttrType.attrString); types[1] = new
		 * AttrType(AttrType.attrInteger); types[2] = new
		 * AttrType(AttrType.attrString); types[3] = new
		 * AttrType(AttrType.attrString); types[4] = new
		 * AttrType(AttrType.attrString); types[5] = new
		 * AttrType(AttrType.attrInteger); types[6] = new
		 * AttrType(AttrType.attrString); types[7] = new
		 * AttrType(AttrType.attrString);
		 */
		/*******************************************/
		short s1_sizes[] = new short[6];
		s1_sizes[0] = 32;
		s1_sizes[1] = 32;
		s1_sizes[2] = 32;
		s1_sizes[3] = 32;
		s1_sizes[4] = 32;
		s1_sizes[5] = 32;

		AttrType[] types_2 = new AttrType[7];
		types_2[0] = new AttrType(AttrType.attrString);
		types_2[1] = new AttrType(AttrType.attrString);
		types_2[2] = new AttrType(AttrType.attrString);
		short s2_sizes[] = new short[3];
		s2_sizes[0] = 32;
		s2_sizes[1] = 32;
		s2_sizes[2] = 32;

		Tuple finalTrio = new Tuple();
		Tuple tu;
		// int count = 0;
		while ((tu = am2.get_next()) != null) {
			tu.setHdr((short) 7, types, s1_sizes);
			finalTrio.setHdr((short) 3, types_2, s2_sizes);
			String node1 = tu.getStrFld(5);
			String node2 = tu.getStrFld(6);
			String node3 = tu.getStrFld(7);

			finalTrio.setStrFld(1, node1);
			finalTrio.setStrFld(2, node2);
			finalTrio.setStrFld(3, node3);
			System.out.println(node1 + ":" + node2 + ":" + node3);
			triangleQueryResult.insertRecord(finalTrio.getTupleByteArray());

			/*
			 * tu.setHdr((short)8, types, s1_sizes);
			 * 
			 * count++;
			 * System.out.println(tu.getStrFld(3)+":"+tu.getStrFld(4)+":"
			 * +tu.getStrFld(8));
			 */
		}
		// System.out.println(count);
		am2.close();
	}

	private Iterator getTriNodeEdgePair(Object[] objExpressions,
			AttrType[] attrTypes, String ehfName, int numBuf) {

		AttrType[] attrType = new AttrType[8];
		attrType[0] = new AttrType(AttrType.attrInteger); // SrcNID.pageid
		attrType[1] = new AttrType(AttrType.attrInteger); // SrcNID.slotno
		attrType[2] = new AttrType(AttrType.attrInteger); // DestNID.pageid
		attrType[3] = new AttrType(AttrType.attrInteger); // DestNID.slotno
		attrType[4] = new AttrType(AttrType.attrString); // EdgeLabel
		attrType[5] = new AttrType(AttrType.attrInteger); // EdgeWeight
		attrType[6] = new AttrType(AttrType.attrString); // SrcLabel
		attrType[7] = new AttrType(AttrType.attrString); // DestLabel

		AttrType[] jtype = new AttrType[8];
		// jtype[0] = new AttrType(AttrType.attrInteger); // SrcNID1.pageid
		// jtype[1] = new AttrType(AttrType.attrInteger); // SrcNID1.slotno
		// jtype[2] = new AttrType(AttrType.attrInteger); // DestNID1.pageid
		// jtype[3] = new AttrType(AttrType.attrInteger); // DestNID1.slotno
		jtype[0] = new AttrType(AttrType.attrString); // EdgeLabel1
		jtype[1] = new AttrType(AttrType.attrInteger); // EdgeWeight1
		jtype[2] = new AttrType(AttrType.attrString); // SrcLabel1
		jtype[3] = new AttrType(AttrType.attrString); // DestLabel1

		// jtype[8] = new AttrType(AttrType.attrInteger); // SrcNID.pageid
		// jtype[9] = new AttrType(AttrType.attrInteger); // SrcNID.slotno
		// jtype[10] = new AttrType(AttrType.attrInteger); // DestNID.pageid
		// jtype[11] = new AttrType(AttrType.attrInteger); // DestNID.slotno
		jtype[4] = new AttrType(AttrType.attrString); // EdgeLabel
		jtype[5] = new AttrType(AttrType.attrInteger); // EdgeWeight
		jtype[6] = new AttrType(AttrType.attrString); // SrcLabel
		jtype[7] = new AttrType(AttrType.attrString); // DestLabel

		FldSpec[] inputProjList = new FldSpec[8];
		RelSpec rel1 = new RelSpec(RelSpec.outer);
		RelSpec rel2 = new RelSpec(RelSpec.innerRel);
		inputProjList[0] = new FldSpec(rel1, 1);
		inputProjList[1] = new FldSpec(rel1, 2);
		inputProjList[2] = new FldSpec(rel1, 3);
		inputProjList[3] = new FldSpec(rel1, 4);
		inputProjList[4] = new FldSpec(rel1, 5);
		inputProjList[5] = new FldSpec(rel1, 6);
		inputProjList[6] = new FldSpec(rel1, 7);
		inputProjList[7] = new FldSpec(rel1, 8);

		FldSpec[] outputProjList = new FldSpec[8];
		outputProjList[0] = new FldSpec(rel1, 5);
		outputProjList[1] = new FldSpec(rel1, 6);
		outputProjList[2] = new FldSpec(rel1, 7);
		outputProjList[3] = new FldSpec(rel1, 8);
		outputProjList[4] = new FldSpec(rel2, 5);
		outputProjList[5] = new FldSpec(rel2, 6);
		outputProjList[6] = new FldSpec(rel2, 7);
		outputProjList[7] = new FldSpec(rel2, 8);
		/*
		 * outputProjList[8] = new FldSpec(rel2, 1); outputProjList[9] = new
		 * FldSpec(rel2, 2); outputProjList[10] = new FldSpec(rel2, 3);
		 * outputProjList[11] = new FldSpec(rel2, 4); outputProjList[12] = new
		 * FldSpec(rel2, 5); outputProjList[13] = new FldSpec(rel2, 6);
		 * outputProjList[14] = new FldSpec(rel2, 7); outputProjList[15] = new
		 * FldSpec(rel2, 8);
		 */

		short s1_sizes[] = new short[3];
		s1_sizes[0] = 32;
		s1_sizes[1] = 32;
		s1_sizes[2] = 32;

		CondExpr[] expr = new CondExpr[4];
		expr[0] = new CondExpr();
		expr[0].next = null;
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 8);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 7);

		expr[1] = new CondExpr();
		expr[1].next = null;
		expr[1].type2 = new AttrType(AttrType.attrSymbol);
		if (attrTypes[0].attrType == AttrType.attrString) {
			expr[1].op = new AttrOperator(AttrOperator.aopEQ);
			expr[1].type1 = new AttrType(AttrType.attrString);
			expr[1].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 5);
			expr[1].operand1.string = (String) objExpressions[0];
		} else {
			expr[1].op = new AttrOperator(AttrOperator.aopGE);
			expr[1].type1 = new AttrType(AttrType.attrInteger);
			expr[1].operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 6);
			expr[1].operand1.integer = (Integer) objExpressions[0];
		}
		expr[2] = new CondExpr();
		expr[2].next = null;
		expr[2].type2 = new AttrType(AttrType.attrSymbol);
		if (attrTypes[1].attrType == AttrType.attrString) {
			expr[2].op = new AttrOperator(AttrOperator.aopEQ);
			expr[2].type1 = new AttrType(AttrType.attrString);
			expr[2].operand2.symbol = new FldSpec(
					new RelSpec(RelSpec.innerRel), 5);
			expr[2].operand1.string = (String) objExpressions[1];
		} else {
			expr[2].op = new AttrOperator(AttrOperator.aopGE);
			expr[2].type1 = new AttrType(AttrType.attrInteger);
			expr[2].operand2.symbol = new FldSpec(
					new RelSpec(RelSpec.innerRel), 6);
			expr[2].operand1.integer = (Integer) objExpressions[1];
		}
		expr[3] = null;

		TupleOrder order = new TupleOrder(TupleOrder.Ascending);
		EFileScan efscan1 = null;
		EFileScan efscan2 = null;
		Iterator sm = null;

		try {
			efscan1 = new EFileScan(ehfName, attrType, s1_sizes, (short) 8, 8,
					inputProjList, null);
			efscan2 = new EFileScan(ehfName, attrType, s1_sizes, (short) 8, 8,
					inputProjList, null);
			/*
			 * sm = new SortMerge(attrType, 8, s1_sizes, attrType, 8, s1_sizes,
			 * 8, 32, 7, 32, numBuf, efscan1, efscan2, false, false, order,
			 * expr, outputProjList, outputProjList.length);
			 */
			sm = new NestedLoopsJoins(attrType, 8, s1_sizes, attrType, 8,
					s1_sizes, numBuf, efscan1, ehfName, expr, null,
					outputProjList, outputProjList.length);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return sm;
	}

	private Iterator getThirdConnectingEdge(Object[] objExpressions,
			AttrType[] attrTypes, String ehfName,
			String indexEhfSourceNodeName, Iterator am1, int numBuf) {

		AttrType[] attrType = new AttrType[8];
		attrType[0] = new AttrType(AttrType.attrInteger); // SrcNID.pageid
		attrType[1] = new AttrType(AttrType.attrInteger); // SrcNID.slotno
		attrType[2] = new AttrType(AttrType.attrInteger); // DestNID.pageid
		attrType[3] = new AttrType(AttrType.attrInteger); // DestNID.slotno
		attrType[4] = new AttrType(AttrType.attrString); // EdgeLabel
		attrType[5] = new AttrType(AttrType.attrInteger); // EdgeWeight
		attrType[6] = new AttrType(AttrType.attrString); // SrcLabel
		attrType[7] = new AttrType(AttrType.attrString); // DestLabel

		AttrType[] jtype = new AttrType[8];
		// jtype[0] = new AttrType(AttrType.attrInteger); // SrcNID1.pageid
		// jtype[1] = new AttrType(AttrType.attrInteger); // SrcNID1.slotno
		// jtype[2] = new AttrType(AttrType.attrInteger); // DestNID1.pageid
		// jtype[3] = new AttrType(AttrType.attrInteger); // DestNID1.slotno
		jtype[0] = new AttrType(AttrType.attrString); // EdgeLabel1
		jtype[1] = new AttrType(AttrType.attrInteger); // EdgeWeight1
		jtype[2] = new AttrType(AttrType.attrString); // SrcLabel1
		jtype[3] = new AttrType(AttrType.attrString); // DestLabel1

		// jtype[8] = new AttrType(AttrType.attrInteger); // SrcNID.pageid
		// jtype[9] = new AttrType(AttrType.attrInteger); // SrcNID.slotno
		// jtype[10] = new AttrType(AttrType.attrInteger); // DestNID.pageid
		// jtype[11] = new AttrType(AttrType.attrInteger); // DestNID.slotno
		jtype[4] = new AttrType(AttrType.attrString); // EdgeLabel
		jtype[5] = new AttrType(AttrType.attrInteger); // EdgeWeight
		jtype[6] = new AttrType(AttrType.attrString); // SrcLabel
		jtype[7] = new AttrType(AttrType.attrString); // DestLabel

		FldSpec[] inputProjList = new FldSpec[8];
		RelSpec rel1 = new RelSpec(RelSpec.outer);
		RelSpec rel2 = new RelSpec(RelSpec.innerRel);
		inputProjList[0] = new FldSpec(rel1, 1);
		inputProjList[1] = new FldSpec(rel1, 2);
		inputProjList[2] = new FldSpec(rel1, 3);
		inputProjList[3] = new FldSpec(rel1, 4);
		inputProjList[4] = new FldSpec(rel1, 5);
		inputProjList[5] = new FldSpec(rel1, 6);
		inputProjList[6] = new FldSpec(rel1, 7);
		inputProjList[7] = new FldSpec(rel1, 8);

		FldSpec[] outputProjList = new FldSpec[7];
		outputProjList[0] = new FldSpec(rel2, 5);
		outputProjList[1] = new FldSpec(rel2, 6);
		outputProjList[2] = new FldSpec(rel2, 7);
		outputProjList[3] = new FldSpec(rel2, 8);
		outputProjList[4] = new FldSpec(rel1, 3);
		outputProjList[5] = new FldSpec(rel1, 4);
		outputProjList[6] = new FldSpec(rel1, 8);
		// outputProjList[5] = new FldSpec(rel2, 6);
		// outputProjList[6] = new FldSpec(rel2, 7);
		// outputProjList[7] = new FldSpec(rel2, 8);
		/*
		 * outputProjList[8] = new FldSpec(rel2, 1); outputProjList[9] = new
		 * FldSpec(rel2, 2); outputProjList[10] = new FldSpec(rel2, 3);
		 * outputProjList[11] = new FldSpec(rel2, 4); outputProjList[12] = new
		 * FldSpec(rel2, 5); outputProjList[13] = new FldSpec(rel2, 6);
		 * outputProjList[14] = new FldSpec(rel2, 7); outputProjList[15] = new
		 * FldSpec(rel2, 8);
		 */

		short s2_sizes[] = new short[3];
		s2_sizes[0] = 32;
		s2_sizes[1] = 32;
		s2_sizes[2] = 32;

		short s1_sizes[] = new short[6];
		s1_sizes[0] = 32;
		s1_sizes[1] = 32;
		s1_sizes[2] = 32;
		s1_sizes[3] = 32;
		s1_sizes[4] = 32;
		s1_sizes[5] = 32;

		CondExpr[] expr = new CondExpr[4];
		expr[0] = new CondExpr();
		expr[0].next = null;
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 8);

		expr[1] = new CondExpr();
		expr[1].next = null;
		expr[1].op = new AttrOperator(AttrOperator.aopEQ);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 8);
		expr[1].type2 = new AttrType(AttrType.attrSymbol);
		expr[1].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 7);

		expr[2] = new CondExpr();
		expr[2].next = null;
		expr[2].type2 = new AttrType(AttrType.attrSymbol);
		if (attrTypes[2].attrType == AttrType.attrString) {
			expr[2].op = new AttrOperator(AttrOperator.aopEQ);
			expr[2].type1 = new AttrType(AttrType.attrString);
			expr[2].operand2.symbol = new FldSpec(
					new RelSpec(RelSpec.innerRel), 5);
			expr[2].operand1.string = (String) objExpressions[2];
		} else {
			expr[2].op = new AttrOperator(AttrOperator.aopGE);
			expr[2].type1 = new AttrType(AttrType.attrInteger);
			expr[2].operand2.symbol = new FldSpec(
					new RelSpec(RelSpec.innerRel), 6);
			expr[2].operand1.integer = (Integer) objExpressions[2];
		}
		expr[3] = null;

		TupleOrder order = new TupleOrder(TupleOrder.Ascending);
		EFileScan efscan2 = null;
		Iterator sm = null;

		try {

			efscan2 = new EFileScan(ehfName, attrType, s2_sizes, (short) 8, 8,
					inputProjList, null);
			
			 /*sm = new NestedLoopsJoins(jtype, 8, s1_sizes, attrType, 8,
			 s2_sizes, numBuf, am1, ehfName, expr, null, outputProjList,
			 outputProjList.length);*/
			 

			
			 /*sm = new SortMerge(jtype, 8, s1_sizes, attrType, 8, s2_sizes, 8,
			 32, 7, 32, numBuf, am1, efscan2, false, false, order, expr,
			 outputProjList, outputProjList.length);*/
			 

			sm = new IndexNestedLoopsJoins(jtype, 8, 8, s1_sizes, attrType, 8,
					7, s2_sizes, (short) numBuf, am1, ehfName,
					indexEhfSourceNodeName, inputProjList, expr, null,
					outputProjList, outputProjList.length);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sm;
	}
}
