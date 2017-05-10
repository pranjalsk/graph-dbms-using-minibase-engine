package iterator;

import java.io.IOException;

import bufmgr.PageNotReadException;
import edgeheap.EScan;
import edgeheap.Edge;
import edgeheap.EdgeHeapFile;
import global.AttrType;
import global.EID;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;

public class EFileScan extends Iterator {
	private AttrType[] _in1;
	private short in1_len;
	private short[] s_sizes;
	private EdgeHeapFile ehf;
	private EScan escan;
	private Edge edge1;
	private Edge Jedge;
	private int t1_size;
	private int nOutFlds;
	private CondExpr[] OutputFilter;
	public FldSpec[] perm_mat;

	/**
	 * constructor
	 * 
	 * @param file_name
	 *            heapfile to be opened
	 * @param in1[]
	 *            array showing what the attributes of the input fields are.
	 * @param s1_sizes[]
	 *            shows the length of the string fields.
	 * @param len_in1
	 *            number of attributes in the input tuple
	 * @param n_out_flds
	 *            number of fields in the out tuple
	 * @param proj_list
	 *            shows what input fields go where in the output tuple
	 * @param outFilter
	 *            select expressions
	 * @exception IOException
	 *                some I/O fault
	 * @exception FileScanException
	 *                exception from this class
	 * @exception TupleUtilsException
	 *                exception from this class
	 * @exception InvalidRelation
	 *                invalid relation
	 */
	public EFileScan(String file_name, AttrType in1[], short s1_sizes[], short len_in1, int n_out_flds,
			FldSpec[] proj_list, CondExpr[] outFilter)
			throws IOException, FileScanException, TupleUtilsException, InvalidRelation {
		_in1 = in1;
		in1_len = len_in1;
		s_sizes = s1_sizes;

		Jedge = new Edge();
		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] ts_size;
		ts_size = TupleUtils.setup_op_tuple(Jedge, Jtypes, in1, len_in1, s1_sizes, proj_list, n_out_flds);

		OutputFilter = outFilter;
		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		edge1 = new Edge();

		try {
			edge1.setHdr(in1_len, _in1, s1_sizes);
		} catch (Exception e) {
			throw new FileScanException(e, "setHdr() failed");
		}
		t1_size = edge1.size();

		try {
			ehf = new EdgeHeapFile(file_name);

		} catch (Exception e) {
			throw new FileScanException(e, "Create new heapfile failed");
		}

		try {
			escan = ehf.openScan();
		} catch (Exception e) {
			throw new FileScanException(e, "openScan() failed");
		}
	}

	/**
	 * @return shows what input fields go where in the output tuple
	 */
	public FldSpec[] show() {
		return perm_mat;
	}

	/**
	 * @return the result tuple
	 * @exception JoinsException
	 *                some join exception
	 * @exception IOException
	 *                I/O errors
	 * @exception InvalidTupleSizeException
	 *                invalid tuple size
	 * @exception InvalidTypeException
	 *                tuple type not valid
	 * @exception PageNotReadException
	 *                exception from lower layer
	 * @exception PredEvalException
	 *                exception from PredEval class
	 * @exception UnknowAttrType
	 *                attribute type unknown
	 * @exception FieldNumberOutOfBoundException
	 *                array out of bounds
	 * @exception WrongPermat
	 *                exception for wrong FldSpec argument
	 */
	public Edge get_next() throws JoinsException, IOException, InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, PredEvalException, UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat {
		EID eid = new EID();

		while (true) {
			if ((edge1 = escan.getNext(eid)) == null) {
				return null;
			}
			edge1.setHdr(in1_len, _in1, s_sizes);
			if (PredEval.Eval(OutputFilter, edge1, null, _in1, null) == true) {
				Projection.Project(edge1, _in1, Jedge, perm_mat, nOutFlds);
				return Jedge;
			}
		}
	}

	/**
	 * implement the abstract method close() from super class Iterator to finish
	 * cleaning up
	 */
	public void close() {

		if (!closeFlag) {
			escan.closescan();
			closeFlag = true;
		}
	}

}
