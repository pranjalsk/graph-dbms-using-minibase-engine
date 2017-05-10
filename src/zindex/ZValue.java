package zindex;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import global.Descriptor;
import btree.KeyNotMatchException;

/**
 * manages the interleaving and de-interleaving of descriptor key
 */
public class ZValue {

	/**
	 * returns the interleaved zvalue of given descriptor
	 * @param desc
	 * @return string zvalue
	 * @throws KeyNotMatchException
	 * @throws UnsupportedEncodingException
	 */
	public static String getZValue(Descriptor desc)
			throws KeyNotMatchException, UnsupportedEncodingException {

		long[] val_low_bit = new long[5];
		long[] val_high_bit = new long[5];
		long[] val = new long[5];

		for (int i = 0; i < val.length; i++) {
			val[i] = desc.get(i);
			val_low_bit[i] = val[i] & 0x00FF;
			val_high_bit[i] = (val[i] & 0xFF00) >> 8;
		}

		long zVal_low_bit = 0;
		long zVal_high_bit = 0;

		for (int i = 0; i < 8; i++) {
			long val0_low_masked_i = (val_low_bit[0] & (1 << i));
			long val1_low_masked_i = (val_low_bit[1] & (1 << i));
			long val2_low_masked_i = (val_low_bit[2] & (1 << i));
			long val3_low_masked_i = (val_low_bit[3] & (1 << i));
			long val4_low_masked_i = (val_low_bit[4] & (1 << i));

			zVal_low_bit |= (val0_low_masked_i << ((4 * i) + 4));
			zVal_low_bit |= (val1_low_masked_i << ((4 * i) + 3));
			zVal_low_bit |= (val2_low_masked_i << ((4 * i) + 2));
			zVal_low_bit |= (val3_low_masked_i << ((4 * i) + 1));
			zVal_low_bit |= (val4_low_masked_i << ((4 * i)));

			long val0_high_masked_i = (val_high_bit[0] & (1 << i));
			long val1_high_masked_i = (val_high_bit[1] & (1 << i));
			long val2_high_masked_i = (val_high_bit[2] & (1 << i));
			long val3_high_masked_i = (val_high_bit[3] & (1 << i));
			long val4_high_masked_i = (val_high_bit[4] & (1 << i));

			zVal_high_bit |= (val0_high_masked_i << ((4 * i) + 4));
			zVal_high_bit |= (val1_high_masked_i << ((4 * i) + 3));
			zVal_high_bit |= (val2_high_masked_i << ((4 * i) + 2));
			zVal_high_bit |= (val3_high_masked_i << ((4 * i) + 1));
			zVal_high_bit |= (val4_high_masked_i << ((4 * i)));
		}

		byte[] zVal_high_End = longToBytes(zVal_high_bit);
		byte[] zVal_low_End = longToBytes(zVal_low_bit);
		char[] zValArr = new char[10];
		int ind = 0;
		for (int i = 3; i < zVal_high_End.length; i++) {
			char tempChar = (char) (zVal_high_End[i] & 0x00FF);

			zValArr[ind++] = tempChar;
		}

		for (int i = 3; i < zVal_low_End.length; i++) {
			char tempChar = (char) (zVal_low_End[i] & 0x00FF);
			zValArr[ind++] = tempChar;
		}

		return new String(zValArr);

	}

	/**
	 * converts the given long to byte array
	 * @param x
	 * @return
	 */
	private static byte[] longToBytes(long x) {

		ByteBuffer buffer = ByteBuffer.allocate(0x8);

		buffer.putLong(x);
		return buffer.array();
	}
	
	/**
	 * de-interleaves the zValue and returns a descriptor
	 * @param zVal
	 * @return descriptor
	 */
	public static Descriptor getDescriptor(String zVal){
		
		byte[] zValByteArr = new byte[zVal.length()];
		char[] zValCharArr = zVal.toCharArray();
		for(int i = 0; i < zValCharArr.length; i++){
			zValByteArr[i] = (byte)(zValCharArr[i] & 0xFF);
		}
		byte[] zVal_high_End = new byte[8];
		byte[] zVal_low_End = new byte[8];
		
		int high_End_Ind = 3;
		int low_End_Ind = 3;
		for(int i = 0; i < zValByteArr.length && low_End_Ind < 8; i++){
			if(i < 5){
				zVal_high_End[high_End_Ind++] = zValByteArr[i];
			}else{
				zVal_low_End[low_End_Ind++] = zValByteArr[i];
			}
		}
		

		long zVal_low_bit = bytesToLong(zVal_low_End);
		long zVal_high_bit = bytesToLong(zVal_high_End);
		
		long[] val_low_bit = new long[5];
		long[] val_high_bit = new long[5];
		
		for(int i = 0; i < 8; i++){
			long val_low_masked_i = (zVal_low_bit >> ((5*i))) & 0x1F;
			long val1_low_masked_0 = (val_low_masked_i & 0x10) >> 4;
			long val1_low_masked_1 = (val_low_masked_i & 0x08) >> 3;
			long val2_low_masked_2 = (val_low_masked_i & 0x04) >> 2;
			long val3_low_masked_3 = (val_low_masked_i & 0x02) >> 1;
			long val4_low_masked_4 = (val_low_masked_i & 0x01);
			
			val_low_bit[0] |= val1_low_masked_0 << i;
			val_low_bit[1] |= val1_low_masked_1 << i;
			val_low_bit[2] |= val2_low_masked_2 << i;
			val_low_bit[3] |= val3_low_masked_3 << i;
			val_low_bit[4] |= val4_low_masked_4 << i;
			
			long val_high_masked_i = (zVal_high_bit >> ((5*i))) & 0x1F;
			long val1_high_masked_0 = (val_high_masked_i & 0x10) >> 4;
			long val1_high_masked_1 = (val_high_masked_i & 0x08) >> 3;
			long val2_high_masked_2 = (val_high_masked_i & 0x04) >> 2;
			long val3_high_masked_3 = (val_high_masked_i & 0x02) >> 1;
			long val4_high_masked_4 = (val_high_masked_i & 0x01);
			
			val_high_bit[0] |= val1_high_masked_0 << i;
			val_high_bit[1] |= val1_high_masked_1 << i;
			val_high_bit[2] |= val2_high_masked_2 << i;
			val_high_bit[3] |= val3_high_masked_3 << i;
			val_high_bit[4] |= val4_high_masked_4 << i;
			
		}
		
		long[] val = new long[5];

		for (int i = 0; i < val.length; i++) {
			val[i] = val_low_bit[i];
			val[i] |= val_high_bit[i] << 8;
		}
		Descriptor desc = new Descriptor();
		desc.set((int)val[0],(int)val[1],(int)val[2],(int)val[3],(int)val[4]);
		return desc;
	}
	
	/**
	 * converts the given bytes to long 
	 * @param bytes
	 * @return long 
	 */
	private static long bytesToLong(byte[] bytes){
		long value = 0;
		for (int i = 0; i < bytes.length; i++)
		{
		   value = (value << 8) + (bytes[i] & 0xff);
		}
		return value;
	}

}
