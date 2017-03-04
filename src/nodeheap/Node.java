package nodeheap;

import java.io.IOException;

import global.AttrType;
import global.Descriptor;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;

public class Node extends Tuple {
		
		
	public Node() {
		super();
	}

	public Node(byte[] aNode, int node_offset) {
		super( aNode,  node_offset, 54);
	}

	public Node(Node fromNode) {
		super((Tuple)fromNode);
	}

	public String getLabel() throws FieldNumberOutOfBoundException, IOException{
		return super.getStrFld(1);
	}
	
	public Descriptor getDesc() throws FieldNumberOutOfBoundException, IOException{
		return super.getDescFld(2);
	}
	
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
		super.tupleCopy((Tuple)fromNode);
	}
	
	public void nodeInit(byte[] aNode, int node_offset){
		super.tupleInit(aNode, node_offset, 54);
	}
	
	public void nodeSet(byte[] fromnode, int offset){
		super.tupleSet(fromnode, offset, 54);
	}
	
	public void setHdr() throws InvalidTypeException, InvalidTupleSizeException, IOException{
		AttrType[] types = {new AttrType(0),new AttrType(5)};
		super.setHdr((short)2, types, new short[]{34});
	}

}
