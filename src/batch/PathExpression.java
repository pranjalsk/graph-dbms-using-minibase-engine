package batch;

import java.io.IOException;

import btree.BTreeFile;
import btree.KeyClass;
import btree.StringKey;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.EdgeHeapFile;

import nodeheap.Node;
import nodeheap.NodeHeapfile;
import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.EID;
import global.IndexType;
import global.NID;
import global.RID;
import global.TupleOrder;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Tuple;
import index.IndexScan;
import iterator.CondExpr;
import iterator.EFileScan;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.IndexNestedLoopsJoins;
import iterator.Iterator;
import iterator.NFileScan;
import iterator.RelSpec;
import iterator.SortMerge;

public class PathExpression {

	public Iterator pathExpress1(Object[] expression, AttrType[] attr,
			String nhfName, String ehfName, String indexEhfSourceNodeName,
			String indexNodeLabelName, short numBuf, short nodeLabelLength)
			throws InvalidSlotNumberException, InvalidTupleSizeException,
			Exception {

		NodeHeapfile nodeHeapFile = new NodeHeapfile(nhfName);
		Node starNode = nodeHeapFile.getRecord((NID) expression[0]);
		starNode.setHdr();

		short[] outer_Iterator_str_sizes = new short[1];
		outer_Iterator_str_sizes[0] = nodeLabelLength;

		AttrType[] in1_outer_Iterator = new AttrType[2];
		in1_outer_Iterator[0] = new AttrType(AttrType.attrString);
		in1_outer_Iterator[1] = new AttrType(AttrType.attrDesc);

		FldSpec[] outer_Iterator_projlist = new FldSpec[2];
		outer_Iterator_projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		outer_Iterator_projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

		CondExpr[] out_filter_outer_Iterator = new CondExpr[2];
		out_filter_outer_Iterator[0] = new CondExpr();
		out_filter_outer_Iterator[0].op = new AttrOperator(AttrOperator.aopEQ);
		out_filter_outer_Iterator[0].type2 = new AttrType(AttrType.attrSymbol);
		out_filter_outer_Iterator[0].type1 = new AttrType(AttrType.attrString);
		out_filter_outer_Iterator[0].operand2.symbol = new FldSpec(new RelSpec(
				RelSpec.outer), 1);
		out_filter_outer_Iterator[0].operand1.string = starNode.getLabel();
		out_filter_outer_Iterator[1] = null;

		Iterator am_outer = new FileScan(nhfName, in1_outer_Iterator,
				outer_Iterator_str_sizes, (short) 2, 2,
				outer_Iterator_projlist, out_filter_outer_Iterator);

		IndexNestedLoopsJoins inlj = null;
		for (int i = 0; i < expression.length - 1; i++) {
			short[] t1_str_sizes = new short[1];
			t1_str_sizes[0] = nodeLabelLength;
			AttrType[] in1 = new AttrType[2];
			in1[0] = new AttrType(AttrType.attrString);
			in1[1] = new AttrType(AttrType.attrDesc);

			short[] t2_str_sizes = new short[3];
			t2_str_sizes[0] = 32;
			t2_str_sizes[1] = 32;
			t2_str_sizes[2] = 32;
			AttrType[] in2 = new AttrType[8];
			in2[0] = new AttrType(AttrType.attrInteger);
			in2[1] = new AttrType(AttrType.attrInteger);
			in2[2] = new AttrType(AttrType.attrInteger);
			in2[3] = new AttrType(AttrType.attrInteger);
			in2[4] = new AttrType(AttrType.attrString);
			in2[5] = new AttrType(AttrType.attrInteger);
			in2[6] = new AttrType(AttrType.attrString);
			in2[7] = new AttrType(AttrType.attrString);

			FldSpec[] inner_projlist = new FldSpec[8];
			RelSpec outer = new RelSpec(RelSpec.outer);
			inner_projlist[0] = new FldSpec(outer, 1);
			inner_projlist[1] = new FldSpec(outer, 2);
			inner_projlist[2] = new FldSpec(outer, 3);
			inner_projlist[3] = new FldSpec(outer, 4);
			inner_projlist[4] = new FldSpec(outer, 5);
			inner_projlist[5] = new FldSpec(outer, 6);
			inner_projlist[6] = new FldSpec(outer, 7);
			inner_projlist[7] = new FldSpec(outer, 8);

			FldSpec[] proj_list = new FldSpec[6];
			RelSpec inner_relation = new RelSpec(RelSpec.innerRel);
			RelSpec outer_relation = new RelSpec(RelSpec.outer);
			proj_list[0] = new FldSpec(inner_relation, 1);
			proj_list[1] = new FldSpec(inner_relation, 2);
			proj_list[2] = new FldSpec(inner_relation, 3);
			proj_list[3] = new FldSpec(inner_relation, 4);
			proj_list[4] = new FldSpec(inner_relation, 7);
			proj_list[5] = new FldSpec(inner_relation, 8);

			if (i != 0) {
				out_filter_outer_Iterator = null;
			}

			try {
				inlj = new IndexNestedLoopsJoins(in1, 2, 1, t1_str_sizes, in2,
						8, 7, t2_str_sizes, numBuf, am_outer, ehfName,
						indexEhfSourceNodeName, inner_projlist, null, null,
						proj_list, 6);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			in1 = new AttrType[6];
			in1[0] = new AttrType(AttrType.attrInteger);
			in1[1] = new AttrType(AttrType.attrInteger);
			in1[2] = new AttrType(AttrType.attrInteger);
			in1[3] = new AttrType(AttrType.attrInteger);
			in1[4] = new AttrType(AttrType.attrString);
			in1[5] = new AttrType(AttrType.attrString);
			t1_str_sizes = new short[2];
			t1_str_sizes[0] = nodeLabelLength;
			t1_str_sizes[1] = nodeLabelLength;
			inner_projlist = new FldSpec[2];
			inner_projlist[0] = new FldSpec(outer, 1);
			inner_projlist[1] = new FldSpec(outer, 2);

			FldSpec[] outer_projlist = new FldSpec[2];
			outer_projlist[0] = new FldSpec(inner_relation, 1);
			outer_projlist[1] = new FldSpec(inner_relation, 2);

			t2_str_sizes = new short[2];
			t2_str_sizes[0] = nodeLabelLength;
			in2 = new AttrType[2];
			in2[0] = new AttrType(AttrType.attrString);
			in2[1] = new AttrType(AttrType.attrDesc);

			CondExpr[] outFilter = new CondExpr[2];
			outFilter[0] = new CondExpr();
			outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
			if (attr[i + 1].attrType == AttrType.attrString) {
				outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
				outFilter[0].type1 = new AttrType(AttrType.attrString);
				outFilter[0].operand2.symbol = new FldSpec(new RelSpec(
						RelSpec.innerRel), 1);
				outFilter[0].operand1.string = (String) expression[i + 1];
			} else {
				outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
				outFilter[0].type1 = new AttrType(AttrType.attrDesc);
				outFilter[0].operand2.symbol = new FldSpec(new RelSpec(
						RelSpec.innerRel), 2);
				outFilter[0].operand1.attrDesc = (Descriptor) expression[i + 1];
			}
			outFilter[1] = null;

			TupleOrder order = new TupleOrder(TupleOrder.Ascending);
			try {
				Iterator nodeIndexScan = new IndexScan(new IndexType(
						IndexType.B_Index), nhfName, indexNodeLabelName, in2,
						t2_str_sizes, 2, 2, inner_projlist, null, 1, false);
				am_outer = new SortMerge(in1, 6, t1_str_sizes, in2, 2,
						t2_str_sizes, 6, nodeLabelLength, 1, nodeLabelLength,
						numBuf, inlj, nodeIndexScan, false, true, order,
						outFilter, outer_projlist, outer_projlist.length);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

		}
		Tuple tu;

		AttrType[] types = new AttrType[2];
		short[] strSizes = new short[1];
		strSizes[0] = 32;
		types[0] = new AttrType(AttrType.attrString);
		types[1] = new AttrType(AttrType.attrDesc);

		Heapfile tailNodeFile = new Heapfile("TailNodeFileForPQ1");
		BatchMapperClass bi = new BatchMapperClass();
		BTreeFile btfNodeLabel = new BTreeFile(indexNodeLabelName);
		while ((tu = am_outer.get_next()) != null) {
			tu.setHdr((short) 2, types, strSizes);
			Tuple tail = new Tuple();
			RID rid = (RID) (bi.getNidFromNodeLabel(tu.getStrFld(1),
					nodeHeapFile, btfNodeLabel));
			tail.setHdr((short) 1, new AttrType[] { new AttrType(
					AttrType.attrId) }, new short[] {});
			tail.setIDFld(1, rid);
			tailNodeFile.insertRecord(tail.getTupleByteArray());
		}
		btfNodeLabel.close();
		inlj.close();
		am_outer.close();

		short[] str_sizes = new short[0];

		AttrType[] atrType = new AttrType[1];
		atrType[0] = new AttrType(AttrType.attrId);

		FldSpec[] projlist = new FldSpec[1];
		projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

		Iterator tail_iterator = new FileScan("TailNodeFileForPQ1", atrType,
				str_sizes, (short) 1, 1, projlist, null);

		return tail_iterator;
	}

	public Iterator pathExpress2(Object[] expression, AttrType[] attr,
			String nhfName, String ehfName, String indexEhfSourceNodeName,
			String indexNodeLabelName, short numBuf, short nodeLabelLength)
			throws InvalidSlotNumberException, InvalidTupleSizeException,
			Exception {

		NodeHeapfile nodeHeapFile = new NodeHeapfile(nhfName);
		Node starNode = nodeHeapFile.getRecord((NID) expression[0]);
		starNode.setHdr();

		short[] outer_Iterator_str_sizes = new short[1];
		outer_Iterator_str_sizes[0] = nodeLabelLength;

		AttrType[] in1_outer_Iterator = new AttrType[2];
		in1_outer_Iterator[0] = new AttrType(AttrType.attrString);
		in1_outer_Iterator[1] = new AttrType(AttrType.attrDesc);

		FldSpec[] outer_Iterator_projlist = new FldSpec[2];
		outer_Iterator_projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		outer_Iterator_projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

		CondExpr[] out_filter_outer_Iterator = new CondExpr[2];
		out_filter_outer_Iterator[0] = new CondExpr();
		out_filter_outer_Iterator[0].op = new AttrOperator(AttrOperator.aopEQ);
		out_filter_outer_Iterator[0].type2 = new AttrType(AttrType.attrSymbol);
		out_filter_outer_Iterator[0].type1 = new AttrType(AttrType.attrString);
		out_filter_outer_Iterator[0].operand2.symbol = new FldSpec(new RelSpec(
				RelSpec.outer), 1);
		out_filter_outer_Iterator[0].operand1.string = starNode.getLabel();
		out_filter_outer_Iterator[1] = null;

		Iterator am_outer = new FileScan(nhfName, in1_outer_Iterator,
				outer_Iterator_str_sizes, (short) 2, 2,
				outer_Iterator_projlist, out_filter_outer_Iterator);

		Iterator inlj = null;

		short[] t1_str_sizes = new short[1];
		t1_str_sizes[0] = nodeLabelLength;
		AttrType[] in1 = new AttrType[2];
		in1[0] = new AttrType(AttrType.attrString);
		in1[1] = new AttrType(AttrType.attrDesc);

		short[] t2_str_sizes = new short[3];
		t2_str_sizes[0] = 32;
		t2_str_sizes[1] = 32;
		t2_str_sizes[2] = 32;
		AttrType[] in2 = new AttrType[8];
		in2[0] = new AttrType(AttrType.attrInteger);
		in2[1] = new AttrType(AttrType.attrInteger);
		in2[2] = new AttrType(AttrType.attrInteger);
		in2[3] = new AttrType(AttrType.attrInteger);
		in2[4] = new AttrType(AttrType.attrString);
		in2[5] = new AttrType(AttrType.attrInteger);
		in2[6] = new AttrType(AttrType.attrString);
		in2[7] = new AttrType(AttrType.attrString);

		FldSpec[] inner_projlist = new FldSpec[8];
		RelSpec outer = new RelSpec(RelSpec.outer);
		inner_projlist[0] = new FldSpec(outer, 1);
		inner_projlist[1] = new FldSpec(outer, 2);
		inner_projlist[2] = new FldSpec(outer, 3);
		inner_projlist[3] = new FldSpec(outer, 4);
		inner_projlist[4] = new FldSpec(outer, 5);
		inner_projlist[5] = new FldSpec(outer, 6);
		inner_projlist[6] = new FldSpec(outer, 7);
		inner_projlist[7] = new FldSpec(outer, 8);

		FldSpec[] proj_list = new FldSpec[8];
		RelSpec inner_relation = new RelSpec(RelSpec.innerRel);
		RelSpec outer_relation = new RelSpec(RelSpec.outer);
		proj_list[0] = new FldSpec(inner_relation, 1);
		proj_list[1] = new FldSpec(inner_relation, 2);
		proj_list[2] = new FldSpec(inner_relation, 3);
		proj_list[3] = new FldSpec(inner_relation, 4);
		proj_list[4] = new FldSpec(inner_relation, 5);
		proj_list[5] = new FldSpec(inner_relation, 6);
		proj_list[6] = new FldSpec(inner_relation, 7);
		proj_list[7] = new FldSpec(inner_relation, 8);

		CondExpr[] right_filter = new CondExpr[2];
		right_filter[0] = new CondExpr();
		right_filter[0].type2 = new AttrType(AttrType.attrSymbol);
		if (attr[1].attrType == AttrType.attrString) {
			right_filter[0].op = new AttrOperator(AttrOperator.aopEQ);
			right_filter[0].type1 = new AttrType(AttrType.attrString);
			right_filter[0].operand2.symbol = new FldSpec(new RelSpec(
					RelSpec.outer), 5);
			right_filter[0].operand1.string = (String) expression[1];
		} else {
			right_filter[0].op = new AttrOperator(AttrOperator.aopGE);
			right_filter[0].type1 = new AttrType(AttrType.attrInteger);
			right_filter[0].operand2.symbol = new FldSpec(new RelSpec(
					RelSpec.outer), 6);
			right_filter[0].operand1.integer = (Integer) expression[1];
		}
		right_filter[1] = null;
		try {
			inlj = new IndexNestedLoopsJoins(in1, 2, 1, t1_str_sizes, in2, 8,
					7, t2_str_sizes, numBuf, am_outer, ehfName,
					indexEhfSourceNodeName, inner_projlist, null, right_filter,
					proj_list, 8);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		for (int i = 2; i < expression.length; i++) {

			CondExpr[] expr = new CondExpr[2];
			expr[0] = new CondExpr();
			expr[0].next = null;
			expr[0].type2 = new AttrType(AttrType.attrSymbol);
			if (attr[i].attrType == AttrType.attrString) {
				expr[0].op = new AttrOperator(AttrOperator.aopEQ);
				expr[0].type1 = new AttrType(AttrType.attrString);
				expr[0].operand2.symbol = new FldSpec(new RelSpec(
						RelSpec.innerRel), 5);
				expr[0].operand1.string = (String) expression[i];
			} else {
				expr[0].op = new AttrOperator(AttrOperator.aopGE);
				expr[0].type1 = new AttrType(AttrType.attrInteger);
				expr[0].operand2.symbol = new FldSpec(new RelSpec(
						RelSpec.innerRel), 6);
				expr[0].operand1.integer = (Integer) expression[i];
			}
			expr[1] = null;

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
			jtype[0] = new AttrType(AttrType.attrInteger); // SrcNID.pageid
			jtype[1] = new AttrType(AttrType.attrInteger); // SrcNID.slotno
			jtype[2] = new AttrType(AttrType.attrInteger); // DestNID.pageid
			jtype[3] = new AttrType(AttrType.attrInteger); // DestNID.slotno
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
			outputProjList[0] = new FldSpec(rel2, 1);
			outputProjList[1] = new FldSpec(rel2, 2);
			outputProjList[2] = new FldSpec(rel2, 3);
			outputProjList[3] = new FldSpec(rel2, 4);
			outputProjList[4] = new FldSpec(rel2, 5);
			outputProjList[5] = new FldSpec(rel2, 6);
			outputProjList[6] = new FldSpec(rel2, 7);
			outputProjList[7] = new FldSpec(rel2, 8);

			short s1_sizes[] = new short[3];
			s1_sizes[0] = nodeLabelLength;
			s1_sizes[1] = nodeLabelLength;
			s1_sizes[2] = nodeLabelLength;

			short s2_sizes[] = new short[3];
			s2_sizes[0] = nodeLabelLength;
			s2_sizes[1] = nodeLabelLength;
			s2_sizes[2] = nodeLabelLength;

			Iterator inj2 = null;

			try {
				inj2 = new IndexNestedLoopsJoins(attrType, 8, 8, s1_sizes,
						attrType, 8, 7, s2_sizes, numBuf, inlj, ehfName,
						indexEhfSourceNodeName, inputProjList, expr, null,
						outputProjList, outputProjList.length);
				inlj = inj2;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// ****************************//*
		Tuple tu;

		AttrType[] types = new AttrType[8];
		short[] strSizes = new short[3];
		strSizes[0] = 32;
		strSizes[1] = 32;
		strSizes[2] = 32;
		types[0] = new AttrType(AttrType.attrInteger); // SrcNID.pageid
		types[1] = new AttrType(AttrType.attrInteger); // SrcNID.slotno
		types[2] = new AttrType(AttrType.attrInteger); // DestNID.pageid
		types[3] = new AttrType(AttrType.attrInteger); // DestNID.slotno
		types[4] = new AttrType(AttrType.attrString); // EdgeLabel
		types[5] = new AttrType(AttrType.attrInteger); // EdgeWeight
		types[6] = new AttrType(AttrType.attrString); // SrcLabel
		types[7] = new AttrType(AttrType.attrString); // DestLabel

		Heapfile tailNodeFile = new Heapfile("TailNodeFileForPQ2");
		while ((tu = inlj.get_next()) != null) {
			tu.setHdr((short) 8, types, strSizes);
			RID rid = new RID();
			rid.pageNo.pid = tu.getIntFld(3);
			rid.slotNo = tu.getIntFld(4);
			Tuple tail = new Tuple();
			tail.setHdr((short) 1, new AttrType[] { new AttrType(
					AttrType.attrId) }, new short[] {});
			tail.setIDFld(1, rid);
			tailNodeFile.insertRecord(tail.getTupleByteArray());
		}
		inlj.close();
		am_outer.close();

		short[] str_sizes = new short[0];

		AttrType[] atrType = new AttrType[1];
		atrType[0] = new AttrType(AttrType.attrId);

		FldSpec[] projlist = new FldSpec[1];
		projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

		Iterator tail_iterator = new FileScan("TailNodeFileForPQ2", atrType,
				str_sizes, (short) 1, 1, projlist, null);

		return tail_iterator;

	}

	public Iterator pathExpress3_2(Object[] expression, String nhfName,
			String ehfName, String indexEhfSourceNodeName,
			String indexNodeLabelName, short numBuf, short nodeLabelLength)
			throws InvalidSlotNumberException, InvalidTupleSizeException,
			Exception {
		// Get a reference to the node heap file
		NodeHeapfile nodeHeapFile = new NodeHeapfile(nhfName);

		// Get the Node corresponding to the start NID in the path expression
		Node starNode = nodeHeapFile.getRecord((NID) expression[0]);
		starNode.setHdr();

		// Get the maximum number of edges in the pat
		int maxTotalEdgeWeight = (Integer) expression[1];

		// Delete TailWeightBoundNodeFile if it exists
		Heapfile tailNodeFile = new Heapfile("TailNodeFileForPQ3");

		// Iterator on TailWeightBoundNodeFile: Reads only the NID of all the
		// Nodes in TailWeightBoundNodeFile
		short[] str_sizes = new short[0];

		AttrType[] atrType = new AttrType[1];
		atrType[0] = new AttrType(AttrType.attrId);

		FldSpec[] tailNodeprojlist = new FldSpec[1];
		tailNodeprojlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

		// Declare a reference to TempTailBoundNodeFile
		EdgeHeapFile tempTailEdgeFile = null;

		// Iterator on TempTailBoundNodeFile: Reads the SourceNodeLabel and
		// DestinationNodeLabel of all the Tuples in TempTailBoundNodeFile
		short[] tempStrSizes = new short[3];
		tempStrSizes[0] = nodeLabelLength;
		tempStrSizes[1] = nodeLabelLength;
		tempStrSizes[2] = nodeLabelLength;

		AttrType[] tempTypes = new AttrType[8];
		tempTypes[0] = new AttrType(AttrType.attrInteger);
		tempTypes[1] = new AttrType(AttrType.attrInteger);
		tempTypes[2] = new AttrType(AttrType.attrInteger);
		tempTypes[3] = new AttrType(AttrType.attrInteger);
		tempTypes[4] = new AttrType(AttrType.attrString);
		tempTypes[5] = new AttrType(AttrType.attrInteger);
		tempTypes[6] = new AttrType(AttrType.attrString);
		tempTypes[7] = new AttrType(AttrType.attrString);

		BatchMapperClass bi = new BatchMapperClass();
		BTreeFile btfNodeLabel = new BTreeFile(indexNodeLabelName);

		Tuple tu, temptu;

		// Prepare an initial scan on the NodeHeapFile
		// The scan results will serve as records for the outer relation of the
		// join operation

		short[] outer_Iterator_str_sizes = new short[1];
		outer_Iterator_str_sizes[0] = nodeLabelLength;

		AttrType[] in1_outer_Iterator = new AttrType[2];
		in1_outer_Iterator[0] = new AttrType(AttrType.attrString);
		in1_outer_Iterator[1] = new AttrType(AttrType.attrDesc);

		FldSpec[] outer_Iterator_projlist = new FldSpec[2];
		outer_Iterator_projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		outer_Iterator_projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

		CondExpr[] out_filter_outer_Iterator = new CondExpr[2];
		out_filter_outer_Iterator[0] = new CondExpr();
		out_filter_outer_Iterator[0].op = new AttrOperator(AttrOperator.aopEQ);
		out_filter_outer_Iterator[0].type2 = new AttrType(AttrType.attrSymbol);
		out_filter_outer_Iterator[0].type1 = new AttrType(AttrType.attrString);
		out_filter_outer_Iterator[0].operand2.symbol = new FldSpec(new RelSpec(
				RelSpec.outer), 1);
		out_filter_outer_Iterator[0].operand1.string = starNode.getLabel();
		out_filter_outer_Iterator[1] = null;

		Iterator am_outer = new FileScan(nhfName, in1_outer_Iterator,
				outer_Iterator_str_sizes, (short) 2, 2,
				outer_Iterator_projlist, out_filter_outer_Iterator);

		IndexNestedLoopsJoins inlj = null;
		String tempTailEdgeFileName = null;

		short[] t2_str_sizes = new short[3];
		t2_str_sizes[0] = 32;
		t2_str_sizes[1] = 32;
		t2_str_sizes[2] = 32;
		AttrType[] in2 = new AttrType[8];
		in2[0] = new AttrType(AttrType.attrInteger);
		in2[1] = new AttrType(AttrType.attrInteger);
		in2[2] = new AttrType(AttrType.attrInteger);
		in2[3] = new AttrType(AttrType.attrInteger);
		in2[4] = new AttrType(AttrType.attrString);
		in2[5] = new AttrType(AttrType.attrInteger);
		in2[6] = new AttrType(AttrType.attrString);
		in2[7] = new AttrType(AttrType.attrString);

		FldSpec[] inner_projlist = new FldSpec[8];
		RelSpec outer = new RelSpec(RelSpec.outer);
		inner_projlist[0] = new FldSpec(outer, 1);
		inner_projlist[1] = new FldSpec(outer, 2);
		inner_projlist[2] = new FldSpec(outer, 3);
		inner_projlist[3] = new FldSpec(outer, 4);
		inner_projlist[4] = new FldSpec(outer, 5);
		inner_projlist[5] = new FldSpec(outer, 6);
		inner_projlist[6] = new FldSpec(outer, 7);
		inner_projlist[7] = new FldSpec(outer, 8);

		RelSpec inner_relation = new RelSpec(RelSpec.innerRel);
		RelSpec outer_relation = new RelSpec(RelSpec.outer);

		FldSpec[] proj_list = new FldSpec[8];
		proj_list[0] = new FldSpec(inner_relation, 1); // Source Node Page No.
		proj_list[1] = new FldSpec(inner_relation, 2); // Source Node Slot No.
		proj_list[2] = new FldSpec(inner_relation, 3); // Destination Node Page
														// No.
		proj_list[3] = new FldSpec(inner_relation, 4); // Destination Node Slot
														// No.
		proj_list[4] = new FldSpec(inner_relation, 5); // Edge Label
		proj_list[5] = new FldSpec(inner_relation, 6); // Edge Weight
		proj_list[6] = new FldSpec(inner_relation, 7); // Source Node Label
		proj_list[7] = new FldSpec(inner_relation, 8); // Destination Node Label

		FldSpec[] temp_proj_list = new FldSpec[8];
		temp_proj_list[0] = new FldSpec(outer_relation, 1); // Source Node Page
															// No.
		temp_proj_list[1] = new FldSpec(outer_relation, 2); // Source Node Slot
															// No.
		temp_proj_list[2] = new FldSpec(outer_relation, 3); // Destination Node
															// Page No.
		temp_proj_list[3] = new FldSpec(outer_relation, 4); // Destination Node
															// Slot No.
		temp_proj_list[4] = new FldSpec(outer_relation, 5); // Edge Label
		temp_proj_list[5] = new FldSpec(outer_relation, 6); // Edge Weight
		temp_proj_list[6] = new FldSpec(outer_relation, 7); // Source Node Label
		temp_proj_list[7] = new FldSpec(outer_relation, 8); // Destination Node
															// Label

		int i = 0;
		do {
			if (i != 0) {
				// System.out.println("i: "+i);
				short[] t1_str_sizes = new short[3];
				t1_str_sizes[0] = 32;
				t1_str_sizes[1] = 32;
				t1_str_sizes[2] = 32;

				AttrType[] in1 = new AttrType[8];
				in1[0] = new AttrType(AttrType.attrInteger);
				in1[1] = new AttrType(AttrType.attrInteger);
				in1[2] = new AttrType(AttrType.attrInteger);
				in1[3] = new AttrType(AttrType.attrInteger);
				in1[4] = new AttrType(AttrType.attrString);
				in1[5] = new AttrType(AttrType.attrInteger);
				in1[6] = new AttrType(AttrType.attrString);
				in1[7] = new AttrType(AttrType.attrString);

				out_filter_outer_Iterator = null;
				CondExpr[] rightFilter = new CondExpr[2];
				rightFilter[0] = new CondExpr();
				rightFilter[0].op = new AttrOperator(AttrOperator.aopGE);
				rightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
				rightFilter[0].type2 = new AttrType(AttrType.attrSymbol);
				rightFilter[0].operand1.symbol = new FldSpec(new RelSpec(
						RelSpec.outer), 6); // temporary file
				rightFilter[0].operand2.symbol = new FldSpec(new RelSpec(
						RelSpec.innerRel), 6); // edgeheap file
				rightFilter[1] = null;

				proj_list[0] = new FldSpec(inner_relation, 1); // Source Node
																// Page No.
				proj_list[1] = new FldSpec(inner_relation, 2); // Source Node
																// Slot No.
				proj_list[2] = new FldSpec(inner_relation, 3); // Destination
																// Node Page
																// No.
				proj_list[3] = new FldSpec(outer_relation, 6); // Destination
																// Node Slot
																// No.
				proj_list[4] = new FldSpec(inner_relation, 5); // Edge Label
				proj_list[5] = new FldSpec(inner_relation, 6); // Edge Weight
				proj_list[6] = new FldSpec(inner_relation, 7); // Source Node
																// Label
				proj_list[7] = new FldSpec(inner_relation, 8); // Destination
																// Node Label

				try {
					inlj = new IndexNestedLoopsJoins(in1, 8, 8, t1_str_sizes,
							in2, 8, 7, t2_str_sizes, numBuf, am_outer, ehfName,
							indexEhfSourceNodeName, inner_projlist,
							rightFilter, null, proj_list, 8);
				} catch (Exception e) {
					System.err
							.println("*** Error preparing for nested_loop_join");
					System.err.println("" + e);
					e.printStackTrace();
					Runtime.getRuntime().exit(1);

				}

				tempTailEdgeFileName = "TempTailEdgeFile" + i;
				tempTailEdgeFile = new EdgeHeapFile(tempTailEdgeFileName);
				tempTailEdgeFile.deleteFile();
				tempTailEdgeFile = new EdgeHeapFile(tempTailEdgeFileName);

				if ((temptu = inlj.get_next()) != null) {
					temptu.setHdr((short) 8, tempTypes, tempStrSizes);
					// System.out.println("Node Label: " + temptu.getStrFld(8));
					Tuple tail = new Tuple();
					Edge tempTail = new Edge();
					// System.err.println("Prior to Edge Updation for Node : "+temptu.getStrFld(8));
					// System.out.println("Source Node Label: " +
					// temptu.getStrFld(7)+" , Destination Node Label: "+temptu.getStrFld(8)+" , Edge Weight: "+temptu.getIntFld(6)
					// +" , Left Edge Weight: "+temptu.getIntFld(4));
					RID rid = (RID) (bi.getNidFromNodeLabel(
							temptu.getStrFld(8), nodeHeapFile, btfNodeLabel));

					/*
					 * if (rid == null) System.out.println("RID is null"); else
					 * System.out.println("RID Page No. : " + rid.pageNo +
					 * " , RID Slot No. : " + rid.slotNo);
					 */
					tail.setHdr((short) 1, new AttrType[] { new AttrType(
							AttrType.attrId) }, new short[] {});
					tail.setIDFld(1, rid);
					tailNodeFile.insertRecord(tail.getTupleByteArray());

					tempTail.setHdr();
					tempTail.setSourceLabel(temptu.getStrFld(7));
					tempTail.setDestLabel(temptu.getStrFld(8));
					tempTail.setLabel(temptu.getStrFld(5));
					tempTail.setWeight(temptu.getIntFld(4)
							- temptu.getIntFld(6));
					/*
					 * System.err.println("After Edge Updation for Node : "+temptu
					 * .getStrFld(8));
					 * System.out.println("Edge Source Node Label: "
					 * +tempTail.getStrFld(7)+
					 * " , Edge Destination Node Label: "+tempTail.getStrFld(8)+
					 * " , Revised Edge Weight: "+tempTail.getIntFld(6)+
					 * " , Left Edge Weight: "+tempTail.getIntFld(4));
					 */
					tempTailEdgeFile.insertEdge(tempTail.getTupleByteArray());

					while ((temptu = inlj.get_next()) != null) {
						temptu.setHdr((short) 8, tempTypes, tempStrSizes);
						// System.out.println("Node Label: " +
						// temptu.getStrFld(8));
						tail = new Tuple();
						tempTail = new Edge();

						/*
						 * System.err.println("Prior to Edge Updation for Node : "
						 * +temptu.getStrFld(8));
						 * System.out.println("Source Node Label: " +
						 * temptu.getStrFld(7)+
						 * " , Destination Node Label: "+temptu.getStrFld(8)+
						 * " , Edge Weight: "+temptu.getIntFld(6) +
						 * " , Left Edge Weight: "+temptu.getIntFld(4));
						 */
						rid = (RID) (bi
								.getNidFromNodeLabel(temptu.getStrFld(8),
										nodeHeapFile, btfNodeLabel));
						// System.out.println(rid.pageNo.pid+":"+rid.slotNo);

						/*
						 * if (rid == null) System.out.println("RID is null");
						 * else System.out.println("RID Page No. : " +
						 * rid.pageNo + " , RID Slot No. : " + rid.slotNo);
						 */
						tail.setHdr((short) 1, new AttrType[] { new AttrType(
								AttrType.attrId) }, new short[] {});
						tail.setIDFld(1, rid);
						tailNodeFile.insertRecord(tail.getTupleByteArray());

						tempTail.setHdr();
						tempTail.setSourceLabel(temptu.getStrFld(7));
						tempTail.setDestLabel(temptu.getStrFld(8));
						tempTail.setLabel(temptu.getStrFld(5));
						tempTail.setWeight(temptu.getIntFld(4)
								- temptu.getIntFld(6));
						/*
						 * System.err.println("After Edge Updation for Node : "+
						 * temptu.getStrFld(8));
						 * System.out.println("Edge Source Node Label: "
						 * +tempTail.getStrFld(7)+
						 * " , Edge Destination Node Label: "
						 * +tempTail.getStrFld(8)+
						 * " , Revised Edge Weight: "+tempTail.getIntFld(6)+
						 * " , Left Edge Weight: "+tempTail.getIntFld(4));
						 */
						tempTailEdgeFile.insertEdge(tempTail
								.getTupleByteArray());
					}
					i++;
				} else
					break;

			} else if (i == 0) {
				// System.out.println("i: " + i);

				short[] t1_str_sizes = new short[1];
				t1_str_sizes[0] = nodeLabelLength;

				AttrType[] in1 = new AttrType[2];
				in1[0] = new AttrType(AttrType.attrString);
				in1[1] = new AttrType(AttrType.attrDesc);

				CondExpr[] rightFilter = new CondExpr[2];
				rightFilter[0] = new CondExpr();
				rightFilter[0].op = new AttrOperator(AttrOperator.aopLE);
				rightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
				rightFilter[0].type2 = new AttrType(AttrType.attrInteger);
				rightFilter[0].operand1.symbol = new FldSpec(new RelSpec(
						RelSpec.outer), 6);
				rightFilter[0].operand2.integer = maxTotalEdgeWeight;
				rightFilter[1] = null;

				try {
					inlj = new IndexNestedLoopsJoins(in1, 2, 1, t1_str_sizes,
							in2, 8, 7, t2_str_sizes, numBuf, am_outer, ehfName,
							indexEhfSourceNodeName, inner_projlist, null,
							rightFilter, proj_list, 8);
				} catch (Exception e) {
					System.err
							.println("*** Error preparing for nested_loop_join");
					System.err.println("" + e);
					e.printStackTrace();
					Runtime.getRuntime().exit(1);
				}

				tempTailEdgeFileName = "TempTailEdgeFile" + i;
				tempTailEdgeFile = new EdgeHeapFile(tempTailEdgeFileName);
				tempTailEdgeFile.deleteFile();
				tempTailEdgeFile = new EdgeHeapFile(tempTailEdgeFileName);

				if ((temptu = inlj.get_next()) != null) {
					temptu.setHdr((short) 8, tempTypes, tempStrSizes);
					// System.out.println("Node Label: " + temptu.getStrFld(8));
					Tuple tail = new Tuple();
					Edge tempTail = new Edge();

					/*
					 * System.err.println("Prior to Edge Updation for Node : "+
					 * temptu.getStrFld(8));
					 * System.out.println("Source Node Label: " +
					 * temptu.getStrFld(7)+
					 * " , Destination Node Label: "+temptu.getStrFld(8)+
					 * " , Edge Weight: "+temptu.getIntFld(6));
					 */
					RID rid = (RID) (bi.getNidFromNodeLabel(
							temptu.getStrFld(8), nodeHeapFile, btfNodeLabel));
					// System.out.println(rid.pageNo.pid+":"+rid.slotNo);

					/*
					 * if (rid == null) System.out.println("RID is null"); else
					 * System.out.println("RID Page No. : " + rid.pageNo +
					 * " , RID Slot No. : " + rid.slotNo);
					 */
					tail.setHdr((short) 1, new AttrType[] { new AttrType(
							AttrType.attrId) }, new short[] {});
					tail.setIDFld(1, rid);
					tailNodeFile.insertRecord(tail.getTupleByteArray());

					tempTail.setHdr();
					tempTail.setSourceLabel(temptu.getStrFld(7));
					tempTail.setDestLabel(temptu.getStrFld(8));
					tempTail.setLabel(temptu.getStrFld(5));
					tempTail.setWeight(maxTotalEdgeWeight - temptu.getIntFld(6));
					// tempTail.setIntFld(4, maxTotalEdgeWeight -
					// temptu.getIntFld(6));
					/*
					 * System.err.println("After Edge Updation for Node : "+temptu
					 * .getStrFld(8));
					 * System.out.println("Edge Source Node Label: "
					 * +tempTail.getStrFld(7)+
					 * " , Edge Destination Node Label: "+tempTail.getStrFld(8)+
					 * " , Revised Edge Weight: "+tempTail.getIntFld(6)+
					 * " , Left Edge Weight: "+tempTail.getIntFld(4));
					 */
					tempTailEdgeFile.insertEdge(tempTail.getTupleByteArray());

					while ((temptu = inlj.get_next()) != null) {
						temptu.setHdr((short) 8, tempTypes, tempStrSizes);
						// System.out.println("Node Label: " +
						// temptu.getStrFld(8));
						tail = new Tuple();
						tempTail = new Edge();

						/*
						 * System.err.println("Prior to Edge Updation for Node : "
						 * +temptu.getStrFld(8));
						 * System.out.println("Source Node Label: " +
						 * temptu.getStrFld(7)+
						 * " , Destination Node Label: "+temptu.getStrFld(8)+
						 * " , Edge Weight: "+temptu.getIntFld(6));
						 */
						rid = (RID) (bi
								.getNidFromNodeLabel(temptu.getStrFld(8),
										nodeHeapFile, btfNodeLabel));
						// System.out.println(rid.pageNo.pid+":"+rid.slotNo);

						/*
						 * if (rid == null) System.out.println("RID is null");
						 * else System.out.println("RID Page No. : " +
						 * rid.pageNo + " , RID Slot No. : " + rid.slotNo);
						 */
						tail.setHdr((short) 1, new AttrType[] { new AttrType(
								AttrType.attrId) }, new short[] {});
						tail.setIDFld(1, rid);
						tailNodeFile.insertRecord(tail.getTupleByteArray());

						tempTail.setHdr();
						tempTail.setSourceLabel(temptu.getStrFld(7));
						tempTail.setDestLabel(temptu.getStrFld(8));
						tempTail.setLabel(temptu.getStrFld(5));
						tempTail.setWeight(maxTotalEdgeWeight
								- temptu.getIntFld(6));
						// tempTail.setIntFld(4, maxTotalEdgeWeight -
						// temptu.getIntFld(6));
						/*
						 * System.err.println("After Edge Updation for Node : "+
						 * temptu.getStrFld(8));
						 * System.out.println("Edge Source Node Label: "
						 * +tempTail.getStrFld(7)+
						 * " , Edge Destination Node Label: "
						 * +tempTail.getStrFld(8)+
						 * " , Revised Edge Weight: "+tempTail.getIntFld(6)+
						 * " , Left Edge Weight: "+tempTail.getIntFld(4));
						 */
						tempTailEdgeFile.insertEdge(tempTail
								.getTupleByteArray());
					}
					i++;
				} else
					break;

			}

			inlj.close();
			am_outer.close();
			proj_list[0] = new FldSpec(outer_relation, 1); // Node Label
			proj_list[1] = new FldSpec(outer_relation, 2); // Descriptor
			am_outer = new EFileScan(tempTailEdgeFileName, tempTypes,
					tempStrSizes, (short) 8, 8, temp_proj_list, null);
			/*
			 * Tuple temp; while((temp = am_outer.get_next()) != null){
			 * temp.setHdr((short)8, tempTypes, tempStrSizes);
			 * System.out.println("Edge Source Node Label: "+temp.getStrFld(7)+
			 * " , Edge Destination Node Label: "+temp.getStrFld(8)
			 * +" , Revised Edge Weight: "+temp.getIntFld(4)
			 * +" , Edge Weight: "+temp.getIntFld(6)); }
			 */

		} while (true);
		btfNodeLabel.close();
		inlj.close();
		am_outer.close();

		for (int j = 0; j <= i; j++) {

			tempTailEdgeFileName = "TempTailEdgeFile" + i;
			tempTailEdgeFile = new EdgeHeapFile(tempTailEdgeFileName);
			tempTailEdgeFile.deleteFile();

		}
		Iterator tail_iterator = new FileScan("TailNodeFileForPQ3", atrType,
				str_sizes, (short) 1, 1, tailNodeprojlist, null);
		/*Tuple t;
		while ((t = tail_iterator.get_next()) != null) {
			t.setHdr((short) 1, atrType, str_sizes);
			RID rid = t.getIDFld(1);
			System.out.println(rid.pageNo.pid + ":" + rid.slotNo);
			t.print(atrType);
		}*/

		return tail_iterator;
	}

	public Iterator pathExpress3_1(Object[] expression, String nhfName,
			String ehfName, String indexEhfSourceNodeName,
			String indexNodeLabelName, short numBuf, short nodeLabelLength)
			throws InvalidSlotNumberException, InvalidTupleSizeException,
			Exception {
		// Get a reference to the node heap file
		NodeHeapfile nodeHeapFile = new NodeHeapfile(nhfName);

		// Get the Node corresponding to the start NID in the path expression
		Node starNode = nodeHeapFile.getRecord((NID) expression[0]);
		starNode.setHdr();

		// Get the maximum number of edges in the pat
		int maxNumberOfEdges = (Integer) expression[1];

		// Delete TailBoundEdgeNodeFile if it exists
		Heapfile tailNodeFile = new Heapfile("TailNodeFileForPQ3");

		// Iterator on TailBoundEdgeNodeFile: Reads only the NID of all the
		// Nodes in TailBoundEdgeNodeFile
		short[] str_sizes = new short[0];

		AttrType[] atrType = new AttrType[1];
		atrType[0] = new AttrType(AttrType.attrId);

		FldSpec[] tailNodeprojlist = new FldSpec[1];
		tailNodeprojlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

		// Declare a reference to TempTailBoundNodeFile
		NodeHeapfile tempTailNodeFile;
		String tempTailNodeFileName;
		// Iterator on TempTailBoundNodeFile: Reads the NodeLabel and Descriptor
		// of all the Tuples in TempTailBoundNodeFile
		short[] tempStrSizes = new short[1];
		tempStrSizes[0] = nodeLabelLength;

		AttrType[] tempTypes = new AttrType[2];
		tempTypes[0] = new AttrType(AttrType.attrString); // Node Label
		tempTypes[1] = new AttrType(AttrType.attrDesc); // Descriptor

		BatchMapperClass bi = new BatchMapperClass();
		BTreeFile btfNodeLabel = new BTreeFile(indexNodeLabelName);

		Tuple tu, temptu;

		// Prepare an initial scan on the NodeHeapFile
		// The scan results will serve as records for the outer relation of the
		// join operation

		short[] outer_Iterator_str_sizes = new short[1];
		outer_Iterator_str_sizes[0] = nodeLabelLength;

		AttrType[] in1_outer_Iterator = new AttrType[2];
		in1_outer_Iterator[0] = new AttrType(AttrType.attrString);
		in1_outer_Iterator[1] = new AttrType(AttrType.attrDesc);

		FldSpec[] outer_Iterator_projlist = new FldSpec[2];
		outer_Iterator_projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		outer_Iterator_projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);

		CondExpr[] out_filter_outer_Iterator = new CondExpr[2];
		out_filter_outer_Iterator[0] = new CondExpr();
		out_filter_outer_Iterator[0].op = new AttrOperator(AttrOperator.aopEQ);
		out_filter_outer_Iterator[0].type2 = new AttrType(AttrType.attrSymbol);
		out_filter_outer_Iterator[0].type1 = new AttrType(AttrType.attrString);
		out_filter_outer_Iterator[0].operand2.symbol = new FldSpec(new RelSpec(
				RelSpec.outer), 1);
		out_filter_outer_Iterator[0].operand1.string = starNode.getLabel();
		out_filter_outer_Iterator[1] = null;

		Iterator am_outer = new FileScan(nhfName, in1_outer_Iterator,
				outer_Iterator_str_sizes, (short) 2, 2,
				outer_Iterator_projlist, out_filter_outer_Iterator);

		IndexNestedLoopsJoins inlj = null;
		short[] t1_str_sizes, t2_str_sizes;
		AttrType[] in1;
		for (int i = 0; i < maxNumberOfEdges; i++) {
			// System.out.println("i: " + i);

			// Iterator Settings for EdgeHeapFile
			t2_str_sizes = new short[3];
			t2_str_sizes[0] = 32;
			t2_str_sizes[1] = nodeLabelLength;
			t2_str_sizes[2] = nodeLabelLength;

			AttrType[] in2 = new AttrType[8];
			in2[0] = new AttrType(AttrType.attrInteger); // Source Node Page No.
			in2[1] = new AttrType(AttrType.attrInteger); // Source Node Slot No.
			in2[2] = new AttrType(AttrType.attrInteger); // Destination Node
															// Page No.
			in2[3] = new AttrType(AttrType.attrInteger); // Destination Node
															// Slot No.
			in2[4] = new AttrType(AttrType.attrString); // Edge Label
			in2[5] = new AttrType(AttrType.attrInteger); // Edge Weight
			in2[6] = new AttrType(AttrType.attrString); // Source Node Label
			in2[7] = new AttrType(AttrType.attrString); // Destination Node
														// Label

			FldSpec[] inner_projlist = new FldSpec[8];
			RelSpec outer = new RelSpec(RelSpec.outer);
			inner_projlist[0] = new FldSpec(outer, 1); // Source Node Page No.
			inner_projlist[1] = new FldSpec(outer, 2); // Source Node Slot No.
			inner_projlist[2] = new FldSpec(outer, 3); // Destination Node Page
														// No.
			inner_projlist[3] = new FldSpec(outer, 4); // Destination Node Slot
														// No.
			inner_projlist[4] = new FldSpec(outer, 5); // Edge Label
			inner_projlist[5] = new FldSpec(outer, 6); // Edge Weight
			inner_projlist[6] = new FldSpec(outer, 7); // Source Node Label
			inner_projlist[7] = new FldSpec(outer, 8); // Destination Node Label

			RelSpec inner_relation = new RelSpec(RelSpec.innerRel);
			RelSpec outer_relation = new RelSpec(RelSpec.outer);

			// Projections after the first Join Operation
			FldSpec[] proj_list = new FldSpec[8];
			proj_list[0] = new FldSpec(inner_relation, 1); // Source Node Page
															// No.
			proj_list[1] = new FldSpec(inner_relation, 2); // Source Node Slot
															// No.
			proj_list[2] = new FldSpec(inner_relation, 3); // Destination Node
															// Page No.
			proj_list[3] = new FldSpec(inner_relation, 4); // Destination Node
															// Slot No.
			proj_list[4] = new FldSpec(inner_relation, 5); // Edge Weight
			proj_list[5] = new FldSpec(inner_relation, 6); // Edge Weight
			proj_list[6] = new FldSpec(inner_relation, 7); // Source Node Label
			proj_list[7] = new FldSpec(inner_relation, 8); // Destination Node
															// Label

			// In the initial run : LHS -> Iterator over Nodes , RHS -> Iterator
			// over Edges

			try {
				in1 = new AttrType[2];
				in1[0] = new AttrType(AttrType.attrString);
				in1[1] = new AttrType(AttrType.attrDesc);

				t1_str_sizes = new short[1];
				t1_str_sizes[0] = nodeLabelLength;

				inlj = new IndexNestedLoopsJoins(in1, 2, 1, t1_str_sizes, in2,
						8, 7, t2_str_sizes, numBuf, am_outer, ehfName,
						indexEhfSourceNodeName, inner_projlist, null, null,
						proj_list, 8);

			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			in1 = new AttrType[8];
			in1[0] = new AttrType(AttrType.attrInteger);
			in1[1] = new AttrType(AttrType.attrInteger);
			in1[2] = new AttrType(AttrType.attrInteger);
			in1[3] = new AttrType(AttrType.attrInteger);
			in1[4] = new AttrType(AttrType.attrString);
			in1[5] = new AttrType(AttrType.attrInteger);
			in1[6] = new AttrType(AttrType.attrString);
			in1[7] = new AttrType(AttrType.attrString);

			t1_str_sizes = new short[3];
			t1_str_sizes[0] = nodeLabelLength;
			t1_str_sizes[1] = nodeLabelLength;
			t1_str_sizes[2] = nodeLabelLength;

			t2_str_sizes = new short[1];
			t2_str_sizes[0] = nodeLabelLength;
			in2 = new AttrType[2];
			in2[0] = new AttrType(AttrType.attrString);
			in2[1] = new AttrType(AttrType.attrDesc);

			inner_projlist = new FldSpec[2];
			inner_projlist[0] = new FldSpec(outer_relation, 1);
			inner_projlist[1] = new FldSpec(outer_relation, 2);

			// Projections after the second Join Operation
			proj_list[0] = new FldSpec(inner_relation, 1); // Node Label
			proj_list[1] = new FldSpec(inner_relation, 2); // Descriptor

			try {
				am_outer = new IndexNestedLoopsJoins(in1, 8, 8, t1_str_sizes,
						in2, 2, 1, t2_str_sizes, numBuf, inlj, nhfName,
						indexNodeLabelName, inner_projlist, null, null,
						proj_list, 2);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			tempTailNodeFileName = "TempTailNodeFile" + i;
			tempTailNodeFile = new NodeHeapfile(tempTailNodeFileName);
			tempTailNodeFile.deleteFile();
			tempTailNodeFile = new NodeHeapfile(tempTailNodeFileName);

			while ((temptu = am_outer.get_next()) != null) {
				temptu.setHdr((short) 2, tempTypes, tempStrSizes);
				Tuple tail = new Tuple();
				Node tempTail = new Node();

				// System.out.println("Label: " + temptu.getStrFld(1));
				RID rid = (RID) (bi.getNidFromNodeLabel(temptu.getStrFld(1),
						nodeHeapFile, btfNodeLabel));

				/*
				 * if (rid == null) System.out.println("RID is null"); else
				 * System.out.println("RID Page No. : " + rid.pageNo +
				 * " , RID Slot No. : " + rid.slotNo);
				 */
				tail.setHdr((short) 1, new AttrType[] { new AttrType(
						AttrType.attrId) }, new short[] {});
				tail.setIDFld(1, rid);
				tailNodeFile.insertRecord(tail.getTupleByteArray());

				tempTail.setHdr();
				tempTail.setLabel(temptu.getStrFld(1));
				tempTail.setDesc(temptu.getDescFld(2));

				tempTailNodeFile.insertNode(temptu.getTupleByteArray());
			}

			// btfNodeLabel.close();
			inlj.close();
			am_outer.close();

			proj_list[0] = new FldSpec(outer_relation, 1); // Node Label
			proj_list[1] = new FldSpec(outer_relation, 2); // Descriptor
			am_outer = new NFileScan(tempTailNodeFileName, tempTypes,
					tempStrSizes, (short) 2, 2, proj_list, null);

		}
		// ****************************//*

		while ((tu = am_outer.get_next()) != null) {
			tu.setHdr((short) 2, tempTypes, tempStrSizes);
			Tuple tail = new Tuple();
			RID rid = (RID) (bi.getNidFromNodeLabel(tu.getStrFld(1),
					nodeHeapFile, btfNodeLabel));
			tail.setHdr((short) 1, new AttrType[] { new AttrType(
					AttrType.attrId) }, new short[] {});
			tail.setIDFld(1, rid);
			tailNodeFile.insertRecord(tail.getTupleByteArray());
		}
		// ****************************//*
		btfNodeLabel.close();
		inlj.close();
		am_outer.close();
		for (int i = 0; i < maxNumberOfEdges; i++) {

			String tempTailEdgeFileName = "TempTailEdgeFile" + i;
			EdgeHeapFile tempTailEdgeFile = new EdgeHeapFile(tempTailEdgeFileName);
			tempTailEdgeFile.deleteFile();

		}
		Iterator tail_iterator = new FileScan("TailNodeFileForPQ3", atrType,
				str_sizes, (short) 1, 1, tailNodeprojlist, null);
		/*Tuple t;
		while ((t = tail_iterator.get_next()) != null) {
			t.setHdr((short) 1, atrType, str_sizes);
			RID rid = t.getIDFld(1);
			t.print(atrType);
		}*/

		return tail_iterator;
	}

}
