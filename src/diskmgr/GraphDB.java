package diskmgr;

import edgeheap.*;
import global.*;
import nodeheap.*;
import heap.*;

import java.io.IOException;
import java.util.HashSet;

import zindex.DescriptorKey;
import zindex.ZTreeFile;

import btree.*;


public class GraphDB extends DB {

	// Declare all 6 type of files 
	public NodeHeapfile nhf;
	public EdgeHeapFile ehf;
	public BTreeFile btf_node_label;
	public BTreeFile btf_edge_label;
	public BTreeFile btf_edge_weight;
	public ZTreeFile ztf_node_desc;
	public int type;
	
	/**
	 *  Creates a new database of the db_name passed as argument 
	 * @param db_name Name of the database
	 */
	
	
	/**
	 * Constructor of graphDB initializes heap files and index files
	 * @param type
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws IOException
	 * @throws GetFileEntryException
	 * @throws ConstructPageException
	 * @throws AddFileEntryException
	 * @throws KeyTooLongException
	 * @throws KeyNotMatchException
	 * @throws LeafInsertRecException
	 * @throws IndexInsertRecException
	 * @throws UnpinPageException
	 * @throws PinPageException
	 * @throws NodeNotMatchException
	 * @throws ConvertException
	 * @throws DeleteRecException
	 * @throws IndexSearchException
	 * @throws IteratorException
	 * @throws LeafDeleteException
	 * @throws InsertException
	 * @throws InvalidTupleSizeException
	 * @throws heap.FieldNumberOutOfBoundException
	 */
	public GraphDB(int type, String graphDBName) throws HFException, HFBufMgrException,
			HFDiskMgrException, IOException, GetFileEntryException,
			ConstructPageException, AddFileEntryException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, InvalidTupleSizeException, heap.FieldNumberOutOfBoundException {

		this.type = type;
		
		int keyTypeString = AttrType.attrString;
		int keyTypeInt = AttrType.attrInteger;
		
		nhf = new NodeHeapfile("NodeHeapFile_"+graphDBName);
		ehf = new EdgeHeapFile("EdgeHeapFile_"+graphDBName);
		btf_node_label = new BTreeFile("IndNodeLabel_"+graphDBName, keyTypeString, 32, 0);
		btf_edge_label = new BTreeFile("IndEdgeLabel_"+graphDBName, keyTypeString, 32, 0);
		btf_edge_weight = new BTreeFile("IndEdgeWeight_"+graphDBName, keyTypeInt, 4, 0);
		ztf_node_desc = new ZTreeFile("IndZtree_"+graphDBName);
		
	}

	//Methods to create index files
	/**
	 * creates a Z tree index file on the descriptor field of node
	 */
	public void createZTFNodeDesc(NodeHeapfile nhf, ZTreeFile ztf_node_desc) {
		try {
			NID nid = new NID();
			Descriptor desc = null;
			Node newNode = null;
			NScan newNscan = nhf.openScan();
			boolean done = false;
			
			while(!done){
				newNode = newNscan.getNext(nid);
				if (newNode==null) {
					done = true;
					break;
				}
				newNode.setHdr();
				desc = newNode.getDesc();
				KeyClass data = new DescriptorKey(desc);
				ztf_node_desc.insert(data, (RID) nid);
			}
			newNscan.closescan();
		} catch (Exception e) {
			System.err.println("Empty node heap file");
			e.printStackTrace();
		}		
	}
	
	/**
	 * creates a B-tree index file on node label attribute
	 * @throws KeyTooLongException
	 * @throws KeyNotMatchException
	 * @throws LeafInsertRecException
	 * @throws IndexInsertRecException
	 * @throws ConstructPageException
	 * @throws UnpinPageException
	 * @throws PinPageException
	 * @throws NodeNotMatchException
	 * @throws ConvertException
	 * @throws DeleteRecException
	 * @throws IndexSearchException
	 * @throws IteratorException
	 * @throws LeafDeleteException
	 * @throws InsertException
	 * @throws IOException
	 * @throws InvalidTupleSizeException
	 * @throws heap.FieldNumberOutOfBoundException
	 */
	public void createBTNodeLabel(NodeHeapfile nhf, BTreeFile btf_node_label) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException, InvalidTupleSizeException, heap.FieldNumberOutOfBoundException{
		try {
			NID nid = new NID();
			String key = null;
			Node newNode = null;
			NScan newNscan = nhf.openScan();
			boolean done = false;
			
			while(!done){
				newNode = newNscan.getNext(nid);
				if (newNode==null) {
					done = true;
					break;
				}
				newNode.setHdr();
				key = newNode.getLabel();
				btf_node_label.insert(new StringKey(key), (RID) nid);			
			}
			newNscan.closescan();
		} catch (Exception e) {
			System.err.println("Empty node heap file");
			e.printStackTrace();
		}
	}
	
	/**
	 * creates a B-tree on the edge label field
	 * @throws KeyTooLongException
	 * @throws KeyNotMatchException
	 * @throws LeafInsertRecException
	 * @throws IndexInsertRecException
	 * @throws ConstructPageException
	 * @throws UnpinPageException
	 * @throws PinPageException
	 * @throws NodeNotMatchException
	 * @throws ConvertException
	 * @throws DeleteRecException
	 * @throws IndexSearchException
	 * @throws IteratorException
	 * @throws LeafDeleteException
	 * @throws InsertException
	 * @throws IOException
	 * @throws InvalidTupleSizeException
	 * @throws heap.FieldNumberOutOfBoundException
	 */
	public void createBTEdgeLabel(EdgeHeapFile ehf, BTreeFile btf_edge_label) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException, InvalidTupleSizeException, heap.FieldNumberOutOfBoundException{
		try {
			EID eid = new EID();
			String key = null;
			Edge newEdge = null;
			EScan newEscan = ehf.openScan();
			boolean done = false;
			
			while(!done){
				newEdge = newEscan.getNext(eid);
				if (newEdge == null) {
					done = true;
					break;
				}
				newEdge.setHdr();
				key = newEdge.getLabel();
				btf_edge_label.insert(new StringKey(key), (RID) eid);			
			}
			newEscan.closescan();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * creates B-Tree index file on edge weight field
	 * @throws KeyTooLongException
	 * @throws KeyNotMatchException
	 * @throws LeafInsertRecException
	 * @throws IndexInsertRecException
	 * @throws ConstructPageException
	 * @throws UnpinPageException
	 * @throws PinPageException
	 * @throws NodeNotMatchException
	 * @throws ConvertException
	 * @throws DeleteRecException
	 * @throws IndexSearchException
	 * @throws IteratorException
	 * @throws LeafDeleteException
	 * @throws InsertException
	 * @throws IOException
	 * @throws InvalidTupleSizeException
	 * @throws heap.FieldNumberOutOfBoundException
	 */
	public void createBTEdgeWeight(EdgeHeapFile ehf, BTreeFile btf_edge_weight) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException, InvalidTupleSizeException, heap.FieldNumberOutOfBoundException{
		try {
			EID eid = new EID();
			int key;
			Edge newEdge = null;
			EScan newEscan = ehf.openScan();
			boolean done = false;
			
			while(!done){
				newEdge = newEscan.getNext(eid);
				if (newEdge == null) {
					done = true;
					break;
				}
				newEdge.setHdr();
				key = newEdge.getWeight();
				btf_edge_weight.insert(new IntegerKey(key), (RID) eid);			
			}
			newEscan.closescan();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//Statistical information about heap files methods
	
	/**
	 * returns the node count in the node heap file
	 * @return NodeCount
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws IOException
	 */
	public int getNodeCnt(NodeHeapfile nhf) throws InvalidSlotNumberException,
			InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException,
			IOException {
		return nhf.getNodeCnt();
	}

	/**
	 * returns the edge count in the edge heap file
	 * @return EdgeCount
	 */
	public int getEdgeCnt(EdgeHeapFile ehf) throws InvalidSlotNumberException,
			InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException,
			IOException {
		return ehf.getEdgeCnt();
	}

	/**
	 * return the distinct sources in the edge heap file
	 * @return distinct SourceCount
	 */
	public int getSourceCnt(EdgeHeapFile ehf) {
		try {
			HashSet<NID> hashSet = new HashSet<NID>();
			EID newEid = new EID();
			EScan newEscan = ehf.openScan();
			Edge newEdge = new Edge();
			boolean done = false;

			while (!done) {
				newEdge = newEscan.getNext(newEid);
				if (newEdge == null) {
					done = true;
					break;
				}
				newEdge.setHdr();
				NID srcNID = newEdge.getSource();
				if (!hashSet.contains(srcNID)) {
					hashSet.add(srcNID);
				}
			}
			newEscan.closescan();
			return hashSet.size();
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}

	}

	/**
	 * return the distinct destination in the edge heap file
	 * @return distinct DestinationCount
	 */
	public int getDestinationCnt(EdgeHeapFile ehf){
		try {
			HashSet<NID> hashSet = new HashSet<NID>();
			EID newEid = new EID();
			EScan newEscan = ehf.openScan();
			Edge newEdge = new Edge();
			boolean done = false;

			while (!done) {
				newEdge = newEscan.getNext(newEid);
				if (newEdge == null) {
					done =true;
					break;
				}
				newEdge.setHdr();
				NID destNID = newEdge.getDestination();
				if (!hashSet.contains(destNID)) {
					hashSet.add(destNID);
				}
			}
			newEscan.closescan();
			return hashSet.size();
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

	/**
	 * returns the total number of distinct labels in node heap file and edge heap file
	 * @return total LabelCount
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws IOException
	 */
	public int getLabelCnt(NodeHeapfile nhf, EdgeHeapFile ehf) throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException, IOException{
		
		int edgeLabelCnt = 0;
		try {
			HashSet<String> hashSet = new HashSet<String>();
			EID newEid = new EID();
			EScan newEscan = ehf.openScan();
			Edge newEdge = new Edge();
			boolean done = false;

			while (!done) {
				newEdge = newEscan.getNext(newEid);
				if (newEdge == null) {
					done = true;
					break;
				}
				newEdge.setHdr();
				String edgeLbl = newEdge.getLabel();
								
				if (!hashSet.contains(edgeLbl)) {
					hashSet.add(edgeLbl);
				}
			}
			newEscan.closescan();
			edgeLabelCnt = hashSet.size();
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
		
		int nodeLabelCnt = nhf.getNodeCnt();
		
		return (edgeLabelCnt + nodeLabelCnt);		
	}

}
