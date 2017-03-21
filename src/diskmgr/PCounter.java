package diskmgr;


public class PCounter {
	private static int rCounter;
	private static int wCounter;

	public static void initialize() {

		rCounter = 0;
		wCounter = 0;
	}

	public static void readIncrement() {
		rCounter= rCounter + 1;
	}

	public static int getRCounter(){
		return rCounter;
	}

	public static void writeIncrement() {
		wCounter= wCounter + 1;
	}
	
	public static int getWCounter(){
		return wCounter;
	}


}