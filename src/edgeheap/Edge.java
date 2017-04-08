package edgeheap;

import java.io.IOException;

import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import nodeheap.Node;
import heap.Tuple;

import global.AttrType;
import global.Convert;
import global.Descriptor;
import global.GlobalConst;
import global.NID;
import global.PageId;
import heap.*;

public class Edge extends Tuple {
	
	public Edge() {
		super();
		this.setFldCnt(8);
	}
	
	public Edge(byte[] aEdge, int edge_offset, int size) {
		super( aEdge,  edge_offset, size);
		this.setFldCnt(8);
	}
	
	public Edge(byte[] aEdge, int edge_offset) {
		super( aEdge,  edge_offset, 142);
	}
	
	public Edge(int size) {
		super(size);
		this.setFldCnt(8);

	}
	
	public Edge(Edge fromEdge) {
		data = fromEdge.getEdgeByteArray();
		tuple_length = fromEdge.getLength();
		tuple_offset = 0;
		fldCnt = fromEdge.noOfFlds();
		fldOffset = fromEdge.copyFldOffset();
	}		
	
	public NID getSource() throws FieldNumberOutOfBoundException, IOException{
		return getNIDFld(2);
	}

	public Edge setSource(NID sourceID) throws FieldNumberOutOfBoundException, IOException{
		
		return (Edge)setNIDFld(2, sourceID);	
	}
	
	public NID getDestination() throws FieldNumberOutOfBoundException, IOException{
		return getNIDFld(4);
	}

	public Edge setDestination(NID DestinationID) throws FieldNumberOutOfBoundException, IOException{
		
		return (Edge)setNIDFld(4, DestinationID);	
	}
	
	public String getLabel() throws FieldNumberOutOfBoundException, IOException{
		return super.getStrFld(5);
	}
	
	public int getWeight() throws FieldNumberOutOfBoundException, IOException{
		return super.getIntFld(6);
	}
	
	public String getSourceLabel() throws FieldNumberOutOfBoundException, IOException{
		return super.getStrFld(7);
	}
	
	public String getDestLabel() throws FieldNumberOutOfBoundException, IOException{
		return super.getStrFld(8);
	}
	
	public Edge setLabel(String label) throws FieldNumberOutOfBoundException, IOException{
		return (Edge)super.setStrFld(5, label);
	}
	
	public Edge setWeight(int weight) throws FieldNumberOutOfBoundException, IOException{
		return (Edge)super.setIntFld(6, weight);
	}
	
	public Edge setSourceLabel(String srcLabel) throws FieldNumberOutOfBoundException, IOException{
		return (Edge)super.setStrFld(7, srcLabel);
	}
		
	public Edge setDestLabel(String destLabel) throws FieldNumberOutOfBoundException, IOException{
		return (Edge)super.setStrFld(8, destLabel);
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
		NID val = new NID();
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
		AttrType[] type = {new AttrType(1),new AttrType(1),new AttrType(1),new AttrType(1),
				new AttrType(0),new AttrType(1),new AttrType(0),new AttrType(0)};
		super.print(type);
	}
	
	public short size(){
		return super.size();
	}
	
	public void edgeCopy(Edge fromEdge){
		super.tupleCopy((Tuple)fromEdge);
	}
	
	public void edgeInit(byte[] aEdge, int edge_offset){
		super.tupleInit(aEdge, edge_offset, 142);
	}
	
	public void edgeSet(byte[] fromEdge, int offset){
		super.tupleSet(fromEdge, offset, 142);
	}
	
	public void setHdr() throws InvalidTypeException, InvalidTupleSizeException, IOException{
		AttrType[] types = {new AttrType(1),new AttrType(1),new AttrType(1),new AttrType(1),
				new AttrType(0),new AttrType(1),new AttrType(0),new AttrType(0)};
		super.setHdr((short)8, types, new short[]{32,32,32}); 
	}
	
}
