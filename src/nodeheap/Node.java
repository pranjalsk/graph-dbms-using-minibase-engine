package nodeheap;

import java.io.IOException;

import global.AttrType;
import global.Descriptor;
import heap.Tuple;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.FieldNumberOutOfBoundException;

public class Node extends Tuple {
		
	//fldCnt needs to be set	
	public Node() {
		super();
		this.setFldCnt(2);
	}

	public Node(byte[] aNode, int node_offset) {
		super( aNode,  node_offset, 54);
		this.setFldCnt(2);
	}
	public Node(int size) {
		super(size);
		this.setFldCnt(2);
	}

	public Node(Node fromNode) {
		data = fromNode.getNodeByteArray();
		tuple_length = fromNode.getLength();
		tuple_offset = 0;
		fldCnt = fromNode.noOfFlds();
		fldOffset = fromNode.copyFldOffset();

	}

	public String getLabel() throws FieldNumberOutOfBoundException, IOException{
		return super.getStrFld(1);
	}
	
	public Descriptor getDesc() throws FieldNumberOutOfBoundException, IOException{
		return super.getDescFld(2);
	}
	//Need to pass 1 here vs 0 since 0 is excluded in Tuple.java
	public Node setLabel(String label) throws FieldNumberOutOfBoundException, IOException{
		return (Node)super.setStrFld(1, label);
	}
	
	public Node setDesc(Descriptor desc) throws FieldNumberOutOfBoundException, IOException{
		return (Node)super.setDescFld(2, desc);
	}
	
	public byte[] getNodeByteArray() {
		return getTupleByteArray();
	}
	
	public void print() throws IOException{
		AttrType[] type = {new AttrType(0),new AttrType(5)};
		super.print(type);
	}
	
	public short size(){
		return super.size();
	}
	
	public void nodeCopy(Node fromNode){
		byte[] temparray = fromNode.getNodeByteArray();
		System.arraycopy(temparray, 0, data, tuple_offset, tuple_length);
	}
	
	public void nodeInit(byte[] aNode, int node_offset){
		super.tupleInit(aNode, node_offset, 54);
	}
	
	public void nodeSet(byte[] fromnode, int offset){
		super.tupleSet(fromnode, offset, 54);
	}
	/**
	 * setHdr will set the header of this Node.
	 *
	 * @param numFlds
	 *            number of fields
	 * @param types[]
	 *            contains the types that will be in this node
	 * @param strSizes[]
	 *            contains the sizes of the string
	 * 
	 * @exception IOException
	 *                I/O errors
	 * @exception InvalidTypeException
	 *                Invalid tupe type
	 * @exception InvalidTupleSizeException
	 *                Tuple size too big
	 *
	 */
	 
	public void setHdr() throws InvalidTypeException, InvalidTupleSizeException, IOException{
		AttrType[] types = {new AttrType(0),new AttrType(5)};
		super.setHdr((short)2, types, new short[]{34});
	}

}
