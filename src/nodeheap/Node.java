package nodeheap;

import java.io.IOException;

import global.AttrType;
import global.Descriptor;
import heap.Tuple;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.FieldNumberOutOfBoundException;

/**
 * Stores the node attributes
 */
public class Node extends Tuple {
		
	/**
	 * constructor for node
	 */	
	public Node() {
		super();
		this.setFldCnt(2);		//fldCnt needs to be set
	}

	/**
	 * copy constructor using bytearray and offset
	 * @param aNode
	 * @param node_offset
	 */
	public Node(byte[] aNode, int node_offset) {

		super( aNode,  node_offset, 62);

		this.setFldCnt(2);
	}
	
	/**
	 * constructor for node with given size
	 * @param size
	 */
	public Node(int size) {
		super(size);
		this.setFldCnt(2);

	}

	/**
	 * copy constructor using other node
	 * @param fromNode
	 */
	public Node(Node fromNode) {
		data = fromNode.getNodeByteArray();
		tuple_length = fromNode.getLength();
		tuple_offset = 0;
		fldCnt = fromNode.noOfFlds();
		fldOffset = fromNode.copyFldOffset();

	}

	/**
	 * copy construcor using byte array node offset and size
	 * @param aNode
	 * @param node_offset
	 * @param size
	 */
	public Node(byte[] aNode, int node_offset, int size) {
		super( aNode,  node_offset, size);
	}

	/**
	 * returning the nodelabel
	 * @return
	 * @throws FieldNumberOutOfBoundException
	 * @throws IOException
	 */
	public String getLabel() throws FieldNumberOutOfBoundException, IOException{
		return super.getStrFld(1);
	}
	
	/**
	 * returning the descriptor
	 * @return
	 * @throws FieldNumberOutOfBoundException
	 * @throws IOException
	 */
	public Descriptor getDesc() throws FieldNumberOutOfBoundException, IOException{
		return super.getDescFld(2);
	}
	//Need to pass 1 here vs 0 since 0 is excluded in Tuple.java
	/**
	 * set the label for particular attribute(1)
	 * @param label
	 * @return
	 * @throws FieldNumberOutOfBoundException
	 * @throws IOException
	 */
	public Node setLabel(String label) throws FieldNumberOutOfBoundException, IOException{
		return (Node)super.setStrFld(1, label);
	}
	
	/**
	 * set the descriptor for particular attribute(2)
	 * @param desc
	 * @return
	 * @throws FieldNumberOutOfBoundException
	 * @throws IOException
	 */
	public Node setDesc(Descriptor desc) throws FieldNumberOutOfBoundException, IOException{
		return (Node)super.setDescFld(2, desc);
	}
	
	/**
	 * returns the node in bytes format
	 * @return
	 */
	public byte[] getNodeByteArray() {
		return getTupleByteArray();
	}
	
	/**
	 * prints the attributes of the node
	 * @throws IOException
	 */
	public void print() throws IOException{
		AttrType[] type = {new AttrType(0),new AttrType(5)};
		super.print(type);
	}
	/**
	 * returns the size of the node in bytes
	 */
	public short size(){
		return super.size();
	}
	
	/**
	 * creates a copy of the given node
	 * @param fromNode
	 */
	public void nodeCopy(Node fromNode){
		byte[] temparray = fromNode.getNodeByteArray();
		System.arraycopy(temparray, 0, data, tuple_offset, tuple_length);
	}
	
	/**
	 * initializes the node with given parametes
	 * @param aNode
	 * @param node_offset
	 */
	public void nodeInit(byte[] aNode, int node_offset){
		super.tupleInit(aNode, node_offset, 62);
	}
	
	/**
	 * sets the node from given node
	 * @param fromnode
	 * @param offset
	 */
	public void nodeSet(byte[] fromnode, int offset){
		super.tupleSet(fromnode, offset, 62);
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
		super.setHdr((short)2, types,new short [] {32});
	}

}
