package edgeheap;

import java.io.IOException;

import heap.FieldNumberOutOfBoundException;
import nodeheap.Node;
import heap.Tuple;

import global.AttrType;
import global.Convert;
import global.Descriptor;
import global.GlobalConst;
import global.NID;
import heap.*;

public class Edge extends Tuple {

	NID source;
	NID destination;
	
	public Edge() {
		super();
	}

	public Edge(byte[] aEdge, int edge_offset) {
		super( aEdge,  edge_offset, 54);
	}
	
	public Edge(Node fromEdge) {
		super((Tuple)fromEdge);
	}		
	
	public NID getSource() throws FieldNumberOutOfBoundException, IOException{
		return getNIDFld(4);
	}

	public Edge setSource(NID sourceID) throws FieldNumberOutOfBoundException, IOException{
		
		return (Edge)setNIDFld(4, sourceID);	
	}
	
	public NID getDestination() throws FieldNumberOutOfBoundException, IOException{
		return getNIDFld(6);
	}

	public Edge setDestination(NID DestinationID) throws FieldNumberOutOfBoundException, IOException{
		
		return (Edge)setNIDFld(6, DestinationID);	
	}
	
	public String getLabel() throws FieldNumberOutOfBoundException, IOException{
		return super.getStrFld(1);
	}
	
	public int getWeight() throws FieldNumberOutOfBoundException, IOException{
		return super.getIntFld(2);
	}
	
	public Edge setLabel(String label) throws FieldNumberOutOfBoundException, IOException{
		return (Edge)super.setStrFld(1, label);
	}
	
	public Edge setWeight(int weight) throws FieldNumberOutOfBoundException, IOException{
		return (Edge)super.setIntFld(2, weight);
	}
	
	public Tuple setNIDFld(int fldNo, NID val) throws IOException, FieldNumberOutOfBoundException {
		if ((fldNo > 0) && (fldNo <= fldCnt)) {
			Convert.setIntValue(val.pageNo.pid, fldOffset[fldNo - 2], data);
			Convert.setIntValue(val.slotNo, fldOffset[fldNo - 1], data);
			return this;
		} else
			throw new FieldNumberOutOfBoundException(null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
	}
	
	public NID getNIDFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {
		NID val = null;
		if ((fldNo > 0) && (fldNo <= fldCnt)) {
			val.pageNo.pid = Convert.getIntValue(fldOffset[fldNo - 2], data);
			val.slotNo = Convert.getIntValue(fldOffset[fldNo - 1], data);
			return val;
		} else
			throw new FieldNumberOutOfBoundException(null, "TUPLE:TUPLE_FLDNO_OUT_OF_BOUND");
	}
	
	public byte[] getEdgeByteArray() {
		return getTupleByteArray();
	}
	
	public void print() throws IOException{
		AttrType[] type = {new AttrType(0),new AttrType(5)};
		super.print(type);
	}
	
	public short size(){
		return super.size();
	}
	
	public void edgeCopy(Edge fromEdge){
		super.tupleCopy((Tuple)fromEdge);
	}
	
	public void edgeInit(byte[] aEdge, int edge_offset){
		super.tupleInit(aEdge, edge_offset, 54);
	}
	
	public void edgeSet(byte[] fromEdge, int offset){
		super.tupleSet(fromEdge, offset, 54);
	}
	
	public void setHdr() throws InvalidTypeException, InvalidTupleSizeException, IOException{
		AttrType[] types = {new AttrType(0),new AttrType(5)};
		super.setHdr((short)2, types, new short[]{34});
	}
	
}
