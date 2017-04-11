package tests;


import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.EID;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.IndexNestedLoopsJoins;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NFileScan;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.SortException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import nodeheap.NodeHeapfile;
import btree.BTreeFile;
import btree.KeyClass;
import btree.StringKey;
import bufmgr.PageNotReadException;
import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.EdgeHeapFile;

public class IndexNestedJoinTest {
	public void node_edge_source(EdgeHeapFile ehf, NodeHeapfile nhf,
			short nodeLabelLength, short numBuf) throws JoinsException,
			IndexException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

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
		Iterator am_outer = new NFileScan(nodeHeapFileName, attrType,
				stringSize, (short) 2, 2, projlist, null);
		/*
		 * Tuple tu; while((tu = am_outer.get_next() )!= null){
		 * System.out.println(tu.getStrFld(1)); }
		 */
		BTreeFile btf_edge_source_label = new BTreeFile("IndexEdgeSouceLabel",
				AttrType.attrString, 32, 0);
		EID eid = new EID();
		Edge edge;
		try {

			EScan escan = ehf.openScan();
			edge = escan.getNext(eid);
			KeyClass key;
			while (edge != null) {
				edge.setHdr();
				key = new StringKey(edge.getSourceLabel());
				btf_edge_source_label.insert(key, eid);
				edge = escan.getNext(eid);
			}
			escan.closescan();
		} catch (Exception e) {
			e.printStackTrace();
		}

		AttrType[] in1 = new AttrType[2];
		short[] t1_str_sizes = new short[1];
		stringSize[0] = nodeLabelLength;
		in1[0] = new AttrType(AttrType.attrString);
		in1[1] = new AttrType(AttrType.attrDesc);

		AttrType[] in2 = new AttrType[8];
		short[] t2_str_sizes = new short[3];
		t2_str_sizes[0] = 32;
		t2_str_sizes[1] = 32;
		t2_str_sizes[2] = 32;
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

		FldSpec[] proj_list = new FldSpec[10];
		RelSpec inner_relation = new RelSpec(RelSpec.innerRel);
		RelSpec outer_relation = new RelSpec(RelSpec.outer);
		proj_list[0] = new FldSpec(outer_relation, 1);
		proj_list[1] = new FldSpec(outer_relation, 2);
		proj_list[2] = new FldSpec(inner_relation, 1);
		proj_list[3] = new FldSpec(inner_relation, 2);
		proj_list[4] = new FldSpec(inner_relation, 3);
		proj_list[5] = new FldSpec(inner_relation, 4);
		proj_list[6] = new FldSpec(inner_relation, 5);
		proj_list[7] = new FldSpec(inner_relation, 6);
		proj_list[8] = new FldSpec(inner_relation, 7);
		proj_list[9] = new FldSpec(inner_relation, 8);

		/*Descriptor desc = new Descriptor();
		desc.set(33, 38, 17, 34, 39);
		CondExpr[] out_filter = new CondExpr[2];
		out_filter[0] = new CondExpr();
		out_filter[0].op = new AttrOperator(AttrOperator.aopEQ);
		out_filter[0].type2 = new AttrType(AttrType.attrSymbol);
		out_filter[0].type1 = new AttrType(AttrType.attrDesc);
		out_filter[0].operand2.symbol = new FldSpec(new RelSpec(
				RelSpec.outer), 2);
		out_filter[0].operand1.attrDesc = desc;
		out_filter[1] = null;*/
		
		IndexNestedLoopsJoins inlj = null;
		try {
			inlj = new IndexNestedLoopsJoins(in1, 2, 1, t1_str_sizes, in2, 8,
					7, t2_str_sizes, numBuf, am_outer, ehf.get_fileName(),
					btf_edge_source_label.get_fileName(), inner_projlist, out_filter,
					null, proj_list, 10);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		AttrType[] op = new AttrType[10];
		op[0] = new AttrType(AttrType.attrString);
		op[1] = new AttrType(AttrType.attrDesc);
		op[2] = new AttrType(AttrType.attrInteger);
		op[3] = new AttrType(AttrType.attrInteger);
		op[4] = new AttrType(AttrType.attrInteger);
		op[5] = new AttrType(AttrType.attrInteger);
		op[6] = new AttrType(AttrType.attrString);
		op[7] = new AttrType(AttrType.attrInteger);
		op[8] = new AttrType(AttrType.attrString);
		op[9] = new AttrType(AttrType.attrString);
		short[] ot_str_sizes = new short[4];
		ot_str_sizes[0] = 32;
		ot_str_sizes[1] = 32;
		ot_str_sizes[2] = 32;
		ot_str_sizes[3] = 32;

		Tuple t;
		t = inlj.get_next();
		while (t != null) {
			t.setHdr((short) 10, op, ot_str_sizes);
			t.print(op);
			t = inlj.get_next();
		}
		inlj.close();
		am_outer.close();
		btf_edge_source_label.close();
		btf_edge_source_label.destroyFile();
	}

	public void node_edge_dest(EdgeHeapFile ehf, NodeHeapfile nhf,
			short nodeLabelLength, short numBuf) throws JoinsException,
			IndexException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

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
		Iterator am_outer = new NFileScan(nodeHeapFileName, attrType,
				stringSize, (short) 2, 2, projlist, null);
		/*
		 * Tuple tu; while((tu = am_outer.get_next() )!= null){
		 * System.out.println(tu.getStrFld(1)); }
		 */
		BTreeFile btf_edge_dest_label = new BTreeFile("IndexEdgeSouceLabel",
				AttrType.attrString, 32, 0);
		EID eid = new EID();
		Edge edge;
		try {

			EScan escan = ehf.openScan();
			edge = escan.getNext(eid);
			KeyClass key;
			while (edge != null) {
				edge.setHdr();
				key = new StringKey(edge.getDestLabel());
				btf_edge_dest_label.insert(key, eid);
				edge = escan.getNext(eid);
			}
			escan.closescan();
		} catch (Exception e) {
			e.printStackTrace();
		}

		AttrType[] in1 = new AttrType[2];
		short[] t1_str_sizes = new short[1];
		stringSize[0] = nodeLabelLength;
		in1[0] = new AttrType(AttrType.attrString);
		in1[1] = new AttrType(AttrType.attrDesc);

		AttrType[] in2 = new AttrType[8];
		short[] t2_str_sizes = new short[3];
		t2_str_sizes[0] = 32;
		t2_str_sizes[1] = 32;
		t2_str_sizes[2] = 32;
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

		FldSpec[] proj_list = new FldSpec[10];
		RelSpec inner_relation = new RelSpec(RelSpec.innerRel);
		RelSpec outer_relation = new RelSpec(RelSpec.outer);
		proj_list[0] = new FldSpec(outer_relation, 1);
		proj_list[1] = new FldSpec(outer_relation, 2);
		proj_list[2] = new FldSpec(inner_relation, 1);
		proj_list[3] = new FldSpec(inner_relation, 2);
		proj_list[4] = new FldSpec(inner_relation, 3);
		proj_list[5] = new FldSpec(inner_relation, 4);
		proj_list[6] = new FldSpec(inner_relation, 5);
		proj_list[7] = new FldSpec(inner_relation, 6);
		proj_list[8] = new FldSpec(inner_relation, 7);
		proj_list[9] = new FldSpec(inner_relation, 8);

		IndexNestedLoopsJoins inlj = null;
		try {
			inlj = new IndexNestedLoopsJoins(in1, 2, 1, t1_str_sizes, in2, 8,
					8, t2_str_sizes, numBuf, am_outer, ehf.get_fileName(),
					btf_edge_dest_label.get_fileName(), inner_projlist, null,
					null, proj_list, 10);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		AttrType[] op = new AttrType[10];
		op[0] = new AttrType(AttrType.attrString);
		op[1] = new AttrType(AttrType.attrDesc);
		op[2] = new AttrType(AttrType.attrInteger);
		op[3] = new AttrType(AttrType.attrInteger);
		op[4] = new AttrType(AttrType.attrInteger);
		op[5] = new AttrType(AttrType.attrInteger);
		op[6] = new AttrType(AttrType.attrString);
		op[7] = new AttrType(AttrType.attrInteger);
		op[8] = new AttrType(AttrType.attrString);
		op[9] = new AttrType(AttrType.attrString);
		short[] ot_str_sizes = new short[4];
		ot_str_sizes[0] = 32;
		ot_str_sizes[1] = 32;
		ot_str_sizes[2] = 32;
		ot_str_sizes[3] = 32;

		Tuple t;
		t = inlj.get_next();
		while (t != null) {
			t.setHdr((short) 10, op, ot_str_sizes);
			t.print(op);
			t = inlj.get_next();
		}
		inlj.close();
		am_outer.close();
		btf_edge_dest_label.close();
		btf_edge_dest_label.destroyFile();
	}

	public void edge_node_source(EdgeHeapFile ehf, NodeHeapfile nhf, BTreeFile btf_node_label,
			short nodeLabelLength,short numBuf) throws JoinsException,
			IndexException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

		AttrType[] attrType = new AttrType[8];
		short[] stringSize = new short[3];
		stringSize[0] = nodeLabelLength;
		stringSize[1] = nodeLabelLength;
		stringSize[2] = nodeLabelLength;
		attrType[0] = new AttrType(AttrType.attrInteger);
		attrType[1] = new AttrType(AttrType.attrInteger);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrInteger);
		attrType[4] = new AttrType(AttrType.attrString);
		attrType[5] = new AttrType(AttrType.attrInteger);
		attrType[6] = new AttrType(AttrType.attrString);
		attrType[7] = new AttrType(AttrType.attrString);
		FldSpec[] projlist = new FldSpec[8];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);
		projlist[3] = new FldSpec(rel, 4);
		projlist[4] = new FldSpec(rel, 5);
		projlist[5] = new FldSpec(rel, 6);
		projlist[6] = new FldSpec(rel, 7);
		projlist[7] = new FldSpec(rel, 8);
		Iterator am_outer = new NFileScan(ehf.get_fileName(), attrType,
				stringSize, (short) 8, 8, projlist, null);
		/*
		 * Tuple tu; while((tu = am_outer.get_next() )!= null){
		 * System.out.println(tu.getStrFld(1)); }
		 */

		AttrType[] in1 = new AttrType[8];
		short[] t1_str_sizes = new short[3];
		t1_str_sizes[0] = 32;
		t1_str_sizes[1] = 32;
		t1_str_sizes[2] = 32;
		in1[0] = new AttrType(AttrType.attrInteger);
		in1[1] = new AttrType(AttrType.attrInteger);
		in1[2] = new AttrType(AttrType.attrInteger);
		in1[3] = new AttrType(AttrType.attrInteger);
		in1[4] = new AttrType(AttrType.attrString);
		in1[5] = new AttrType(AttrType.attrInteger);
		in1[6] = new AttrType(AttrType.attrString);
		in1[7] = new AttrType(AttrType.attrString);
		
		AttrType[] in2 = new AttrType[2];
		short[] t2_str_sizes = new short[1];
		t2_str_sizes[0] = nodeLabelLength;
		in2[0] = new AttrType(AttrType.attrString);
		in2[1] = new AttrType(AttrType.attrDesc);
		FldSpec[] inner_projlist = new FldSpec[2];
		RelSpec outer = new RelSpec(RelSpec.outer);
		inner_projlist[0] = new FldSpec(outer, 1);
		inner_projlist[1] = new FldSpec(outer, 2);
		
		FldSpec[] proj_list = new FldSpec[10];
		RelSpec inner_relation = new RelSpec(RelSpec.innerRel);
		RelSpec outer_relation = new RelSpec(RelSpec.outer);
		proj_list[0] = new FldSpec(outer_relation, 1);
		proj_list[1] = new FldSpec(outer_relation, 2);
		proj_list[2] = new FldSpec(outer_relation, 3);
		proj_list[3] = new FldSpec(outer_relation, 4);
		proj_list[4] = new FldSpec(outer_relation, 5);
		proj_list[5] = new FldSpec(outer_relation, 6);
		proj_list[6] = new FldSpec(outer_relation, 7);
		proj_list[7] = new FldSpec(outer_relation, 8);
		proj_list[8] = new FldSpec(inner_relation, 1);
		proj_list[9] = new FldSpec(inner_relation, 2);

		IndexNestedLoopsJoins inlj = null;
		try {
			inlj = new IndexNestedLoopsJoins(in1, 8, 7, t1_str_sizes, in2, 2,
					1, t2_str_sizes, numBuf, am_outer, nhf.get_fileName(),
					btf_node_label.get_fileName(), inner_projlist, null,
					null, proj_list, 10);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		AttrType[] op = new AttrType[10];
		op[0] = new AttrType(AttrType.attrInteger);
		op[1] = new AttrType(AttrType.attrInteger);
		op[2] = new AttrType(AttrType.attrInteger);
		op[3] = new AttrType(AttrType.attrInteger);
		op[4] = new AttrType(AttrType.attrString);
		op[5] = new AttrType(AttrType.attrInteger);
		op[6] = new AttrType(AttrType.attrString);
		op[7] = new AttrType(AttrType.attrString);
		op[8] = new AttrType(AttrType.attrString);
		op[9] = new AttrType(AttrType.attrDesc);
		short[] ot_str_sizes = new short[4];
		ot_str_sizes[0] = 32;
		ot_str_sizes[1] = 32;
		ot_str_sizes[2] = 32;
		ot_str_sizes[3] = 32;

		Tuple t;
		t = inlj.get_next();
		while (t != null) {
			t.setHdr((short) 10, op, ot_str_sizes);
			t.print(op);
			t = inlj.get_next();
		}
		inlj.close();
		am_outer.close();

	}

	public void edge_node_dest(EdgeHeapFile ehf, NodeHeapfile nhf, BTreeFile btf_node_label,
			short nodeLabelLength,short numBuf) throws JoinsException,
			IndexException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

		AttrType[] attrType = new AttrType[8];
		short[] stringSize = new short[3];
		stringSize[0] = nodeLabelLength;
		stringSize[1] = nodeLabelLength;
		stringSize[2] = nodeLabelLength;
		attrType[0] = new AttrType(AttrType.attrInteger);
		attrType[1] = new AttrType(AttrType.attrInteger);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrInteger);
		attrType[4] = new AttrType(AttrType.attrString);
		attrType[5] = new AttrType(AttrType.attrInteger);
		attrType[6] = new AttrType(AttrType.attrString);
		attrType[7] = new AttrType(AttrType.attrString);
		FldSpec[] projlist = new FldSpec[8];
		RelSpec rel = new RelSpec(RelSpec.outer);
		projlist[0] = new FldSpec(rel, 1);
		projlist[1] = new FldSpec(rel, 2);
		projlist[2] = new FldSpec(rel, 3);
		projlist[3] = new FldSpec(rel, 4);
		projlist[4] = new FldSpec(rel, 5);
		projlist[5] = new FldSpec(rel, 6);
		projlist[6] = new FldSpec(rel, 7);
		projlist[7] = new FldSpec(rel, 8);
		Iterator am_outer = new NFileScan(ehf.get_fileName(), attrType,
				stringSize, (short) 8, 8, projlist, null);
		/*
		 * Tuple tu; while((tu = am_outer.get_next() )!= null){
		 * System.out.println(tu.getStrFld(1)); }
		 */

		AttrType[] in1 = new AttrType[8];
		short[] t1_str_sizes = new short[3];
		t1_str_sizes[0] = 32;
		t1_str_sizes[1] = 32;
		t1_str_sizes[2] = 32;
		in1[0] = new AttrType(AttrType.attrInteger);
		in1[1] = new AttrType(AttrType.attrInteger);
		in1[2] = new AttrType(AttrType.attrInteger);
		in1[3] = new AttrType(AttrType.attrInteger);
		in1[4] = new AttrType(AttrType.attrString);
		in1[5] = new AttrType(AttrType.attrInteger);
		in1[6] = new AttrType(AttrType.attrString);
		in1[7] = new AttrType(AttrType.attrString);
		
		AttrType[] in2 = new AttrType[2];
		short[] t2_str_sizes = new short[1];
		t2_str_sizes[0] = nodeLabelLength;
		in2[0] = new AttrType(AttrType.attrString);
		in2[1] = new AttrType(AttrType.attrDesc);
		FldSpec[] inner_projlist = new FldSpec[2];
		RelSpec outer = new RelSpec(RelSpec.outer);
		inner_projlist[0] = new FldSpec(outer, 1);
		inner_projlist[1] = new FldSpec(outer, 2);
		
		FldSpec[] proj_list = new FldSpec[10];
		RelSpec inner_relation = new RelSpec(RelSpec.innerRel);
		RelSpec outer_relation = new RelSpec(RelSpec.outer);
		proj_list[0] = new FldSpec(outer_relation, 1);
		proj_list[1] = new FldSpec(outer_relation, 2);
		proj_list[2] = new FldSpec(outer_relation, 3);
		proj_list[3] = new FldSpec(outer_relation, 4);
		proj_list[4] = new FldSpec(outer_relation, 5);
		proj_list[5] = new FldSpec(outer_relation, 6);
		proj_list[6] = new FldSpec(outer_relation, 7);
		proj_list[7] = new FldSpec(outer_relation, 8);
		proj_list[8] = new FldSpec(inner_relation, 1);
		proj_list[9] = new FldSpec(inner_relation, 2);

		IndexNestedLoopsJoins inlj = null;
		try {
			inlj = new IndexNestedLoopsJoins(in1, 8, 8, t1_str_sizes, in2, 2,
					1, t2_str_sizes, numBuf, am_outer, nhf.get_fileName(),
					btf_node_label.get_fileName(), inner_projlist, null,
					null, proj_list, 10);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		AttrType[] op = new AttrType[10];
		op[0] = new AttrType(AttrType.attrInteger);
		op[1] = new AttrType(AttrType.attrInteger);
		op[2] = new AttrType(AttrType.attrInteger);
		op[3] = new AttrType(AttrType.attrInteger);
		op[4] = new AttrType(AttrType.attrString);
		op[5] = new AttrType(AttrType.attrInteger);
		op[6] = new AttrType(AttrType.attrString);
		op[7] = new AttrType(AttrType.attrString);
		op[8] = new AttrType(AttrType.attrString);
		op[9] = new AttrType(AttrType.attrDesc);
		short[] ot_str_sizes = new short[4];
		ot_str_sizes[0] = 32;
		ot_str_sizes[1] = 32;
		ot_str_sizes[2] = 32;
		ot_str_sizes[3] = 32;

		Tuple t;
		t = inlj.get_next();
		while (t != null) {
			t.setHdr((short) 10, op, ot_str_sizes);
			t.print(op);
			t = inlj.get_next();
		}
		inlj.close();
		am_outer.close();
	}

}
