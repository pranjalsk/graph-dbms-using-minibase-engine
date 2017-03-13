package diskmgr;

import edgeheap.*;
import nodeheap.NodeHeapfile;

public class PCounter {
	private static int rCounter;
	private static int wCounter;

	public static void initialize() {

		rCounter = 0;
		wCounter = 0;
	}

	public static void readIncrement() {
		rCounter++;
	}

	public int getRCounterForNHF(NodeHeapfile file){
		return rCounter;
	}
	public int getRCounterForEHF(EdgeHeapFile file){

		return rCounter;
	}

	public static void writeIncrement() {
		wCounter++;
	}

	
	public int getWCounterForNHF(NodeHeapfile file){
		return wCounter;
	}
	public int getWCounterForEHF(EdgeHeapFile file){

		return wCounter;
	}

}