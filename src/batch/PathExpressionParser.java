package batch;

import global.AttrType;
import global.Descriptor;
import global.NID;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import zindex.ZTreeFile;
import btree.BTreeFile;
import nodeheap.NodeHeapfile;

public class PathExpressionParser {

	public int pathExpressionQuery1Parser(List<Object[]> objExpList,
			List<AttrType[]> attrTypeList, String inputPathExpression,
			BTreeFile btf_node, NodeHeapfile nhf, ZTreeFile ztf_desc)
			throws Exception {
		int [] type = new int[1];
		
		List<String[]> pathExpression = splitPathExpression(inputPathExpression, type);

		// Finding the list of NIDs corresponding to the headnode
		BatchInsert batchinsert = new BatchInsert();
		String headnode = pathExpression.get(0)[1];
		List<NID> nidlist = null;
		if (headnode.matches("\\d+\\s?\\d+\\s?\\d+\\s?\\d+\\s?\\d+")) {
			nidlist = batchinsert.getNidFromDescriptor(headnode, nhf, ztf_desc); 
		} else {
			nidlist = new ArrayList<NID>();
			nidlist.add(batchinsert.getNidFromNodeLabel(headnode, nhf, btf_node));
		}

		int i;
		int n = pathExpression.size();

		Object[] objectArray = new Object[n];
		AttrType[] attrArray = new AttrType[n];

		objectArray[0] = null;
		attrArray[0] = new AttrType(AttrType.attrInteger);
		for (i = 1; i < n; i++) {
			String input = pathExpression.get(i)[1];
			if (input.matches("\\d+\\s?\\d+\\s?\\d+\\s?\\d+\\s?\\d+")) {
				String[] descInput = input.trim().split(" ");
				objectArray[i] = new Descriptor();
				int[] values = new int[5];
				for (int ctr = 0; ctr < 5; ctr++) {
					values[ctr] = Integer.parseInt(descInput[ctr]);
				}
				((Descriptor) objectArray[i]).set(values[0], values[1],
						values[2], values[3], values[4]);
				attrArray[i] = new AttrType(AttrType.attrDesc);
			} else {
				objectArray[i] = input.trim();
				attrArray[i] = new AttrType(AttrType.attrString);
			}
		}

		for (i = 0; i < nidlist.size(); i++) {
			Object[] finalObjectArray = new Object[n];
			System.arraycopy(objectArray, 0, finalObjectArray, 0, n);
			finalObjectArray[0] = nidlist.get(i);
			objExpList.add(finalObjectArray);
			attrTypeList.add(attrArray);
		}
		return type[0];
	}

	public int pathExpressionQuery2Parser(List<Object[]> objExpList,
			List<AttrType[]> attrTypeList, String inputPathExpression, NodeHeapfile nhf, ZTreeFile ztf_desc, BTreeFile btf_node) throws Exception {
		int [] type = new int[1];
		
		List<String[]> pathExpression = splitPathExpression(inputPathExpression, type);

		// Finding the list of NIDs corresponding to the headnode
		BatchInsert batchinsert = new BatchInsert();
		String headnode = pathExpression.get(0)[1];
		List<NID> nidlist = null;
		if (headnode.matches("\\d+\\s?\\d+\\s?\\d+\\s?\\d+\\s?\\d+")) {
			nidlist = batchinsert.getNidFromDescriptor(headnode, nhf, ztf_desc); 
		} else {
			nidlist = new ArrayList<NID>();
			nidlist.add(batchinsert.getNidFromNodeLabel(headnode, nhf, btf_node));
		}

		int i;
		int n = pathExpression.size();

		Object[] objectArray = new Object[n];
		AttrType[] attrArray = new AttrType[n];

		objectArray[0] = null;
		attrArray[0] = new AttrType(AttrType.attrInteger);
		for (i = 1; i < n; i++) {
			String input = pathExpression.get(i)[1].trim();
			if (pathExpression.get(i)[0].equals("EL")) {								
				objectArray[i] = input;				
				attrArray[i] = new AttrType(AttrType.attrString);
			} else {
				objectArray[i] = Integer.parseInt(input);
				attrArray[i] = new AttrType(AttrType.attrInteger);
			}
		}
		for (i = 0; i < nidlist.size(); i++) {
			Object[] finalObjectArray = new Object[n];
			System.arraycopy(objectArray, 0, finalObjectArray, 0, n);
			finalObjectArray[0] = nidlist.get(i);
			objExpList.add(finalObjectArray);
			attrTypeList.add(attrArray);
		}
		return type[0];
	}

	public int pathExpressionQuery3Parser(List<Object[]> objExpList,
			List<AttrType[]> attrTypeList, String inputPathExpression, NodeHeapfile nhf, ZTreeFile ztf_desc, BTreeFile btf_node) throws Exception {
		int [] type = new int[1];
		
		List<String[]> pathExpression = splitPathExpression(inputPathExpression, type);

		// Finding the list of NIDs corresponding to the headnode
		BatchInsert batchinsert = new BatchInsert();
		String headnode = pathExpression.get(0)[1];
		List<NID> nidlist = null;
		if (headnode.matches("\\d+\\s?\\d+\\s?\\d+\\s?\\d+\\s?\\d+")) {
			nidlist = batchinsert.getNidFromDescriptor(headnode, nhf, ztf_desc); 
		} else {
			nidlist = new ArrayList<NID>();
			nidlist.add(batchinsert.getNidFromNodeLabel(headnode, nhf, btf_node));
		}

		int i;
		//int n = pathExpression.size();

		Object[] objectArray = new Object[2];
		AttrType[] attrArray = new AttrType[2];

		objectArray[0] = null;
		attrArray[0] = new AttrType(AttrType.attrInteger);
		//for (i = 1; i < n; i++) {
		String input = pathExpression.get(1)[1].trim();
		if (pathExpression.get(1)[0].equals("MNE")) {								
			objectArray[1] = input+":MNE";				
			attrArray[1] = new AttrType(AttrType.attrInteger);
		} else {
			objectArray[1] = input+":MTEW";	
			attrArray[1] = new AttrType(AttrType.attrInteger);
		}
		//}
		for (i = 0; i < nidlist.size(); i++) {
			Object[] finalObjectArray = new Object[2];
			System.arraycopy(objectArray, 0, finalObjectArray, 0, 2);
			finalObjectArray[0] = nidlist.get(i);
			objExpList.add(finalObjectArray);
			attrTypeList.add(attrArray);
		}
		// ObjectArray is [[NID1, 10:MNE],[NID2, 15:MTEW]...]
		return type[0];
	}
	
	public int triangleQueryParser(Object[] objExpressions,
			AttrType[] attrTypes, String trianglePathExpression) {
		int[] type = new int[1];
		List<String[]> pathExpression = splitPathExpression(trianglePathExpression, type);
		int i;
		//int n = pathExpression.size();
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

	public List<String[]> keyValueSeparator(List<String> partialListForQueries) {

		List<String[]> map = new ArrayList<String[]>();
		for (String pathex : partialListForQueries) {
			String[] keyValue = new String[2];
			Pattern p = Pattern
					.compile("(MNE|MTEW|MEW||NL|ND|EL|EW)\\s?[:]\\s?(\\d+_\\d+|\\d+ \\d+ \\d+ \\d+ \\d+|\\d+)");
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
