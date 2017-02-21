package global;

public class Descriptor {
	
	public static final int DESCRIPTOR_SIZE = 5;
	int value[] = new int[DESCRIPTOR_SIZE];

	public void set(int value0, int value1, int value2, int value3, int value4) {
		value[0] = value0;
		value[1] = value1;
		value[2] = value2;
		value[3] = value3;
		value[4] = value4;
	}

	public int get(int idx) {
		return value[idx];
	}

	double equal(Descriptor desc) {

		for (int ind = 0; ind < value.length; ind++) {
			if (value[ind] != desc.value[ind]) {
				return 0;
			}
		}

		return 1;
	}

	double distance(Descriptor desc) {

		double squareSum = 0;
		for (int ind = 0; ind < value.length; ind++) {
			double sum = Math.pow((value[ind] - desc.value[ind]), 2);
			squareSum += sum;
		}

		return Math.sqrt(squareSum);
	}
}