package batch;

import global.AttrType;
import global.Descriptor;
import global.NID;
import global.PageId;
import global.RID;
import heap.Heapfile;
import heap.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.RelSpec;
import zindex.ZTreeFile;
import btree.BTreeFile;
import nodeheap.NodeHeapfile;

public class PathExpressionParser {

	Iterator niditer;
	Object[] objExpList;
	AttrType[] attrTypeList;
	
	/**
	 * @param inputPathExpression
	 * @param btf_node
	 * @param nhf
	 * @param ztf_desc
	 * @return
	 * @throws Exception
	 */
	// Returns an iterator over all possible NIDs of the head node, an array of objects & its types for Query1
	public int pathExpressionQuery1Parser(String inputPathExpression,
			BTreeFile btf_node, NodeHeapfile nhf, ZTreeFile ztf_desc)
			throws Exception {
		int [] type = new int[1];
		
		List<String[]> pathExpression = splitPathExpression(inputPathExpression, type);

		// Finding the list of NIDs corresponding to the headnode
		BatchMapperClass batchinsert = new BatchMapperClass();
		String headnode = pathExpression.get(0)[1];

		/* Check if the NN is a string label or descriptor
		 * Set iterator over the NIDs returned by the BatchMapper class
		 */
		if (headnode.matches("\\d+\\s?\\d+\\s?\\d+\\s?\\d+\\s?\\d+")) {
			niditer = batchinsert.getNidFromDescriptor(headnode, nhf, ztf_desc); 
		}
		else {
			NID newnid = batchinsert.getNidFromNodeLabel(headnode, nhf, btf_node);
			Heapfile newhf = new Heapfile("NIDheapfile");
			RID newrid = new RID(new PageId(newnid.pageNo.pid), newnid.slotNo);
			Tuple newtuple = new Tuple();
			newtuple.setHdr((short)1, new AttrType[] {new AttrType(AttrType.attrId)}, new short[] {});
			newtuple.setIDFld(1, newrid);
			newhf.insertRecord(newtuple.getTupleByteArray());
			short[] str_sizes = new short[0];
			
			AttrType[] atrType = new AttrType[1];
			atrType[0] = new AttrType(AttrType.attrId);

			FldSpec[] projlist = new FldSpec[1];
			projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

			niditer = new FileScan("NIDheapfile", atrType,
					str_sizes, (short) 1, 1, projlist, null);
		}
				
		int i;
		int n = pathExpression.size();

		objExpList = new Object[n];
		attrTypeList = new AttrType[n];

		objExpList[0] = type[0];
		attrTypeList[0] = new AttrType(AttrType.attrId);
		
		
		for (i = 1; i < n; i++) {
			String input = pathExpression.get(i)[1];
			if (input.matches("\\d+\\s?\\d+\\s?\\d+\\s?\\d+\\s?\\d+")) {
				String[] descInput = input.trim().split(" ");
				objExpList[i] = new Descriptor();
				int[] values = new int[5];
				for (int ctr = 0; ctr < 5; ctr++) {
					values[ctr] = Integer.parseInt(descInput[ctr]);
				}
				((Descriptor) objExpList[i]).set(values[0], values[1],
						values[2], values[3], values[4]);
				attrTypeList[i] = new AttrType(AttrType.attrDesc);
			} else {
				objExpList[i] = input.trim();
				attrTypeList[i] = new AttrType(AttrType.attrString);
			}
		}

		return type[0];
	}

	/**
	 * @param inputPathExpression
	 * @param nhf
	 * @param ztf_desc
	 * @param btf_node
	 * @return
	 * @throws Exception
	 */
	// Returns an iterator over all possible NIDs of the head node, an array of objects & its types for Query2
	public int pathExpressionQuery2Parser(String inputPathExpression, NodeHeapfile nhf, ZTreeFile ztf_desc, BTreeFile btf_node) throws Exception {
		int [] type = new int[1];
		
		List<String[]> pathExpression = splitPathExpression(inputPathExpression, type);

		// Finding the list of NIDs corresponding to the headnode
		BatchMapperClass batchinsert = new BatchMapperClass();
		String headnode = pathExpression.get(0)[1];
		
		/* Check if the NN is a string label or descriptor
		 * Set iterator over the NIDs returned by the BatchMapper class
		 */
		if (headnode.matches("\\d+\\s?\\d+\\s?\\d+\\s?\\d+\\s?\\d+")) {
			niditer = batchinsert.getNidFromDescriptor(headnode, nhf, ztf_desc); 
		}
		else {
			NID newnid = batchinsert.getNidFromNodeLabel(headnode, nhf, btf_node);
			Heapfile newhf = new Heapfile("NIDheapfile");
			RID newrid = new RID(new PageId(newnid.pageNo.pid), newnid.slotNo);
			Tuple newtuple = new Tuple();
			newtuple.setHdr((short)1, new AttrType[] {new AttrType(AttrType.attrId)}, new short[] {});
			newtuple.setIDFld(1, newrid);
			newhf.insertRecord(newtuple.getTupleByteArray());
			short[] str_sizes = new short[0];
			
			AttrType[] atrType = new AttrType[1];
			atrType[0] = new AttrType(AttrType.attrId);

			FldSpec[] projlist = new FldSpec[1];
			projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

			niditer = new FileScan("NIDheapfile", atrType,
					str_sizes, (short) 1, 1, projlist, null);
		}
		
		int i;
		int n = pathExpression.size();

		objExpList = new Object[n];
		attrTypeList = new AttrType[n];

		objExpList[0] = null;
		attrTypeList[0] = new AttrType(AttrType.attrId);
		for (i = 1; i < n; i++) {
			String input = pathExpression.get(i)[1].trim();
			if (pathExpression.get(i)[0].equals("EL")) {								
				objExpList[i] = input;				
				attrTypeList[i] = new AttrType(AttrType.attrString);
			} else {
				objExpList[i] = Integer.parseInt(input);
				attrTypeList[i] = new AttrType(AttrType.attrInteger);
			}
		}
		return type[0];
	}

	/**
	 * @param inputPathExpression
	 * @param nhf
	 * @param ztf_desc
	 * @param btf_node
	 * @return
	 * @throws Exception
	 */
	// Returns an iterator over all possible NIDs of the head node, an array of objects & its types for Query3
	public int pathExpressionQuery3Parser(String inputPathExpression, NodeHeapfile nhf, ZTreeFile ztf_desc, BTreeFile btf_node) throws Exception {
		int [] type = new int[1];
		
		List<String[]> pathExpression = splitPathExpression(inputPathExpression, type);

		// Finding the list of NIDs corresponding to the headnode
		BatchMapperClass batchinsert = new BatchMapperClass();
		String headnode = pathExpression.get(0)[1];
		
		/* Check if the NN is a string label or descriptor
		 * Set iterator over the NIDs returned by the BatchMapper class
		 */
		if (headnode.matches("\\d+\\s?\\d+\\s?\\d+\\s?\\d+\\s?\\d+")) {
			niditer = batchinsert.getNidFromDescriptor(headnode, nhf, ztf_desc); 
		}
		else {
			NID newnid = batchinsert.getNidFromNodeLabel(headnode, nhf, btf_node);
			Heapfile newhf = new Heapfile("NIDheapfile");
			RID newrid = new RID(new PageId(newnid.pageNo.pid), newnid.slotNo);
			Tuple newtuple = new Tuple();
			newtuple.setHdr((short)1, new AttrType[] {new AttrType(AttrType.attrId)}, new short[] {});
			newtuple.setIDFld(1, newrid);
			newhf.insertRecord(newtuple.getTupleByteArray());
			short[] str_sizes = new short[0];
			
			AttrType[] atrType = new AttrType[1];
			atrType[0] = new AttrType(AttrType.attrId);

			FldSpec[] projlist = new FldSpec[1];
			projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

			niditer = new FileScan("NIDheapfile", atrType,
					str_sizes, (short) 1, 1, projlist, null);
		}		

		objExpList = new Object[2];
		attrTypeList = new AttrType[2];

		objExpList[0] = null;
		attrTypeList[0] = new AttrType(AttrType.attrId);
		String input = pathExpression.get(1)[1].trim();
		if (pathExpression.get(1)[0].equals("MNE")) {								
			objExpList[1] = Integer.parseInt(input);				
			attrTypeList[1] = new AttrType(AttrType.attrInteger);
		}
		else {
			objExpList[1] = Integer.parseInt(input);	
			attrTypeList[1] = new AttrType(AttrType.attrString);
		}		
		return type[0];
	}
	
	/**
	 * @param objExpressions
	 * @param attrTypes
	 * @param trianglePathExpression
	 * @return
	 */
	// Returns an array of objects & its types for Triangle Query
	public int triangleQueryParser(Object[] objExpressions,
			AttrType[] attrTypes, String trianglePathExpression) {
		int[] type = new int[1];
		List<String[]> pathExpression = splitPathExpression(trianglePathExpression, type);
		int i;

		for(i=0; i<3; i++){
			String input = pathExpression.get(i)[1].trim();
			if (pathExpression.get(i)[0].equals("EL")) {
				objExpressions[i] = input;
				attrTypes[i] = new AttrType(AttrType.attrString);
			}
			else {
				objExpressions[i] = Integer.parseInt(input);
				attrTypes[i] = new AttrType(AttrType.attrInteger);
			}
		}
		return type[0];
	}

	/**
	 * @param pathexp
	 * @param type
	 * @return
	 */
	//Splits the input query string into appropriate format to be used by parser functions
	public List<String[]> splitPathExpression(String pathexp, int[] type) {

		List<String> partialListForQueries = new ArrayList<String>();
		String rest = "";

		Pattern p = Pattern.compile("[PT]Q\\d?(\\w)\\s?[:>]\\s?(.*)");
		Matcher m = p.matcher(pathexp);

		while (m.find()) {
			// add "a" "b" "c" info
			type[0] = m.group(1).equals("a") ? 0 : m.group(1).equals("b") ? 1
					: 2;
			rest = m.group(2);
		}
		String[] arr = rest.split("/");
		for (String str : arr) {
			partialListForQueries.add(str);
		}

		List<String[]> map = keyValueSeparator(partialListForQueries);
		return map;
	}

	/**
	 * @param partialListForQueries
	 * @return
	 */
	public List<String[]> keyValueSeparator(List<String> partialListForQueries) {

		List<String[]> map = new ArrayList<String[]>();
		for (String pathex : partialListForQueries) {
			String[] keyValue = new String[2];
			Pattern p = Pattern
					.compile("(MNE|MTEW|MEW|NL|ND|EL|EW)\\s?[:]\\s?(\\d+_\\d+|\\d+ \\d+ \\d+ \\d+ \\d+|\\d+)");
			Matcher m = p.matcher(pathex);
			while (m.find()) {
				keyValue[0] = m.group(1);
				keyValue[1] = m.group(2);
			}
			map.add(keyValue);
		}
		return map;
	}
}