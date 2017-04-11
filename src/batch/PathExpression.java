package batch;

import java.io.IOException;

import nodeheap.NScan;
import nodeheap.Node;
import nodeheap.NodeHeapfile;
import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.NID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.IndexNestedLoopsJoins;
import iterator.Iterator;
import iterator.RelSpec;

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

		for (int i = 0; i < expression.length-1; i++) {

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

			IndexNestedLoopsJoins inlj = null;

			if (i != 0) {
				out_filter_outer_Iterator = null;
			}

			try {
				inlj = new IndexNestedLoopsJoins(in1, 2, 1, t1_str_sizes, in2,
						8, 7, t2_str_sizes, numBuf, am_outer, ehfName,
						indexEhfSourceNodeName, inner_projlist,
						out_filter_outer_Iterator, null, proj_list, 6);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			am_outer.close();
			
			t1_str_sizes = new short[2];
			t1_str_sizes[0] = nodeLabelLength;
			t1_str_sizes[1] = nodeLabelLength;
			inner_projlist = new FldSpec[6];
			inner_projlist[0] = new FldSpec(outer, 1);
			inner_projlist[1] = new FldSpec(outer, 2);
			inner_projlist[2] = new FldSpec(outer, 3);
			inner_projlist[3] = new FldSpec(outer, 4);
			inner_projlist[4] = new FldSpec(outer, 5);
			inner_projlist[5] = new FldSpec(outer, 6);
			
			t2_str_sizes = new short[2];
			t2_str_sizes[0] = nodeLabelLength;
			in2 = new AttrType[2];
			in2[0] = new AttrType(AttrType.attrString);
			in2[1] = new AttrType(AttrType.attrDesc);
			CondExpr[] rightFilter = new CondExpr[2];
			rightFilter[0] = new CondExpr();
			rightFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
			if(attr[i+1].attrType == AttrType.attrString){
				rightFilter[0].type2 = new AttrType(AttrType.attrSymbol);
				rightFilter[0].type1 = new AttrType(AttrType.attrString);
				rightFilter[0].operand2.symbol = new FldSpec(new RelSpec(
						RelSpec.outer), 2);
				rightFilter[0].operand1.string = (String)expression[i+1];
			}else{
				rightFilter[0].type2 = new AttrType(AttrType.attrSymbol);
				rightFilter[0].type1 = new AttrType(AttrType.attrDesc);
				rightFilter[0].operand2.symbol = new FldSpec(new RelSpec(
						RelSpec.outer), 2);
				rightFilter[0].operand1.attrDesc = (Descriptor)expression[i+1];
			}
			rightFilter[1] = null;
			try {
				am_outer = new IndexNestedLoopsJoins(in1, 6, 6, t1_str_sizes,
						in2, 2, 1, t2_str_sizes, numBuf, inlj, nhfName,
						indexNodeLabelName, inner_projlist, null,
						rightFilter, proj_list, 2);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			inlj.close();
		}
		return am_outer;
	}
}
