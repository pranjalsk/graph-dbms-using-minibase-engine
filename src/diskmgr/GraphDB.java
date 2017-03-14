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
	public BTreeFile btf_node;
	public BTreeFile btf_edge_label;
	public BTreeFile btf_edge_weight;
	public ZTreeFile ztf_node_desc;
	
	public int type;
	public static String dbpath;
	public static String logpath;
	public static String graphDBName;
	
    public static String getGraphDBName() {
		return graphDBName;
	}

	public static void setGraphDBName(String graphDBName) {
		GraphDB.graphDBName = graphDBName;
	}
	
	/**
	 *  Creates a new database of the db_name passed as argument 
	 * @param db_name Name of the database
	 */
	public static void initGraphDB(String db_name) {
        graphDBName= db_name;
		dbpath = "/tmp/" + db_name + System.getProperty("user.name") + ".minibase-db";
		logpath = "/tmp/" + db_name + System.getProperty("user.name") + ".minibase-log";
		
		@SuppressWarnings("unused")
		SystemDefs sysdef = new SystemDefs(dbpath, 300, 100, "Clock"); //Number of pages=300, Buffer pool=100 pages
		
		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent. We assume
		// user are on UNIX system here
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("IO error: " + e);
		}

		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("IO error: " + e);
		}

	}
	
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
	public GraphDB(int type) throws HFException, HFBufMgrException,
			HFDiskMgrException, IOException, GetFileEntryException,
			ConstructPageException, AddFileEntryException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, InvalidTupleSizeException, heap.FieldNumberOutOfBoundException {

		this.type = type;
		
		int keyTypeString = AttrType.attrString;
		int keyTypeInt = AttrType.attrInteger;
		
		nhf = new NodeHeapfile("NodeHeapFile"+ graphDBName);
		//System.out.println("heap file created");
		ehf = new EdgeHeapFile("EdgeHeapFile"+ graphDBName);
		//System.out.println("edge heap file created");
		
		btf_node = new BTreeFile("IndexNodeLabel", keyTypeString, 32, 1);
		btf_edge_label = new BTreeFile("IndexEdgeLabel", keyTypeString, 32, 1);
		btf_edge_weight = new BTreeFile("IndexEdgeWeight", keyTypeInt, 4, 1);
		ztf_node_desc = new ZTreeFile();
		
	}

	//Methods to create index files
	/**
	 * creates a Z tree index file on the descriptor field of node
	 */
	public void createZTFNodeDesc() {
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
	public void createBTNodeLabel() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException, InvalidTupleSizeException, heap.FieldNumberOutOfBoundException{
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
				btf_node.insert(new StringKey(key), (RID) nid);			
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
	public void createBTEdgeLabel() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException, InvalidTupleSizeException, heap.FieldNumberOutOfBoundException{
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
	public void createBTEdgeWeight() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException, IOException, InvalidTupleSizeException, heap.FieldNumberOutOfBoundException{
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
	public int getNodeCnt() throws InvalidSlotNumberException,
			InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException,
			IOException {
		return nhf.getNodeCnt();
	}

	/**
	 * returns the edge count in the edge heap file
	 * @return EdgeCount
	 */
	public int getEdgeCnt() throws InvalidSlotNumberException,
			InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException,
			IOException {
		return ehf.getEdgeCnt();
	}

	/**
	 * return the distinct sources in the edge heap file
	 * @return distinct SourceCount
	 */
	public int getSourceCnt() {
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
	public int getDestinationCnt(){
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
	public int getLabelCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException, HFBufMgrException, IOException{
		
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
			edgeLabelCnt = hashSet.size();
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
		
		int nodeLabelCnt = nhf.getNodeCnt();
		
		return (edgeLabelCnt + nodeLabelCnt);		
	}

}
