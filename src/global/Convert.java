/* file Convert.java */

package global;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Convert {

	/**
	 * read 4 bytes from given byte array at the specified position convert it
	 * to an integer
	 * 
	 * @param data
	 *            a byte array
	 * @param position
	 *            in data[]
	 * @exception java.io.IOException
	 *                I/O errors
	 * @return the integer
	 */
	public static int getIntValue(int position, byte[] data) throws java.io.IOException {
		InputStream in;
		DataInputStream instr;
		int value;
		byte tmp[] = new byte[4];

		// copy the value from data array out to a tmp byte array
		
		System.arraycopy(data, position, tmp, 0, 4);
		/*
		 * creates a new data input stream to read data from the specified input
		 * stream
		 */
		in = new ByteArrayInputStream(tmp);
		instr = new DataInputStream(in);
		value = instr.readInt();

		
		return value;
	}

	/**
	 * read 4 bytes from given byte array at the specified position convert it
	 * to a float value
	 * 
	 * @param data
	 *            a byte array
	 * @param position
	 *            in data[]
	 * @exception java.io.IOException
	 *                I/O errors
	 * @return the float value
	 */
	public static float getFloValue(int position, byte[] data) throws java.io.IOException {
		InputStream in;
		DataInputStream instr;
		float value;
		byte tmp[] = new byte[4];

		// copy the value from data array out to a tmp byte array
		System.arraycopy(data, position, tmp, 0, 4);

		/*
		 * creates a new data input stream to read data from the specified input
		 * stream
		 */
		in = new ByteArrayInputStream(tmp);
		instr = new DataInputStream(in);
		value = instr.readFloat();

		return value;
	}

	/**
	 * read 2 bytes from given byte array at the specified position convert it
	 * to a short integer
	 * 
	 * @param data
	 *            a byte array
	 * @param position
	 *            the position in data[]
	 * @exception java.io.IOException
	 *                I/O errors
	 * @return the short integer
	 */
	public static short getShortValue(int position, byte[] data) throws java.io.IOException {
		InputStream in;
		DataInputStream instr;
		short value;
		byte tmp[] = new byte[2];

		// copy the value from data array out to a tmp byte array
		System.arraycopy(data, position, tmp, 0, 2);

		/*
		 * creates a new data input stream to read data from the specified input
		 * stream
		 */
		in = new ByteArrayInputStream(tmp);
		instr = new DataInputStream(in);
		value = instr.readShort();

		return value;
	}

	/**
	 * reads a string that has been encoded using a modified UTF-8 format from
	 * the given byte array at the specified position
	 * 
	 * @param data
	 *            a byte array
	 * @param position
	 *            the position in data[]
	 * @param length
	 *            the length of the string in bytes (=strlength +2)
	 * @exception java.io.IOException
	 *                I/O errors
	 * @return the string
	 */
	public static String getStrValue(int position, byte[] data, int length) throws java.io.IOException {
		InputStream in;
		DataInputStream instr;
		String value;
		byte tmp[] = new byte[length];

		System.arraycopy(data, position, tmp, 0, length);

		/*
		 * creates a new data input stream to read data from the specified input
		 * stream
		 */
		
		in = new ByteArrayInputStream(tmp);
		instr = new DataInputStream(in);
		try{
			value = instr.readUTF();
		}catch(Exception e){
			value = "";
		//	System.out.println("Value set");
		}

		return value;
		/*short lengthToRead = (short)(((tmp[0] << 8)) | ((tmp[1] & 0xff)));
		if(lengthToRead < 0) return "";
		byte[] bytesToread = new byte[lengthToRead];
		try{
			System.arraycopy(data, position+2, bytesToread, 0, lengthToRead);
		}catch(Exception e){
			System.arraycopy(data, position+2, bytesToread, 0, 0);
			String values = new String(bytesToread, "UTF-8");
			System.out.println(">>>>>>>>>>>>>>>>>"+values);
		}
			
		return new String(bytesToread, "UTF-8");*/
	}

	/**
	 * reads 2 bytes from the given byte array at the specified position convert
	 * it to a character
	 * 
	 * @param data
	 *            a byte array
	 * @param position
	 *            the position in data[]
	 * @exception java.io.IOException
	 *                I/O errors
	 * @return the character
	 */
	public static char getCharValue(int position, byte[] data) throws java.io.IOException {
		InputStream in;
		DataInputStream instr;
		char value;
		byte tmp[] = new byte[2];
		// copy the value from data array out to a tmp byte array
		System.arraycopy(data, position, tmp, 0, 2);

		/*
		 * creates a new data input stream to read data from the specified input
		 * stream
		 */
		in = new ByteArrayInputStream(tmp);
		instr = new DataInputStream(in);
		value = instr.readChar();
		return value;
	}

	public static Descriptor getDescValue(int position, byte[] data) throws IOException {

		Descriptor value = new Descriptor();
		
		int intArr[] = new int[Descriptor.DESCRIPTOR_SIZE];
		
		for(int ind = 0; ind  < Descriptor.DESCRIPTOR_SIZE; ind++, position+=4){
			intArr[ind] = getIntValue(position, data);
		}
		
		value.set(intArr[0], intArr[1], intArr[2], intArr[3], intArr[4]);
		return value;
		
		
	}
	
	public static RID getIdValue(int position, byte[] data) throws IOException {

		RID value = new RID();
		
		int intArr[] = new int[2];
		
		for(int ind = 0; ind  < 2; ind++, position+=4){
			intArr[ind] = getIntValue(position, data);
		}
		
		value.pageNo.pid = intArr[0];
		value.slotNo = intArr[1];
		return value;
		
		
	}

	/**
	 * update an integer value in the given byte array at the specified position
	 * 
	 * @param data
	 *            a byte array
	 * @param value
	 *            the value to be copied into the data[]
	 * @param position
	 *            the position of tht value in data[]
	 * @exception java.io.IOException
	 *                I/O errors
	 */
	public static void setIntValue(int value, int position, byte[] data) throws java.io.IOException {
		/*
		 * creates a new data output stream to write data to underlying output
		 * stream
		 */

		OutputStream out = new ByteArrayOutputStream();
		DataOutputStream outstr = new DataOutputStream(out);

		// write the value to the output stream

		outstr.writeInt(value);

		// creates a byte array with this output stream size and the
		// valid contents of the buffer have been copied into it
		byte[] B = ((ByteArrayOutputStream) out).toByteArray();

		// copies the first 4 bytes of this byte array into data[]
		System.arraycopy(B, 0, data, position, 4);

	}

	/**
	 * update a float value in the given byte array at the specified position
	 * 
	 * @param data
	 *            a byte array
	 * @param value
	 *            the value to be copied into the data[]
	 * @param position
	 *            the position of tht value in data[]
	 * @exception java.io.IOException
	 *                I/O errors
	 */
	public static void setFloValue(float value, int position, byte[] data) throws java.io.IOException {
		/*
		 * creates a new data output stream to write data to underlying output
		 * stream
		 */

		OutputStream out = new ByteArrayOutputStream();
		DataOutputStream outstr = new DataOutputStream(out);

		// write the value to the output stream

		outstr.writeFloat(value);

		// creates a byte array with this output stream size and the
		// valid contents of the buffer have been copied into it
		byte[] B = ((ByteArrayOutputStream) out).toByteArray();

		// copies the first 4 bytes of this byte array into data[]
		System.arraycopy(B, 0, data, position, 4);

	}

	/**
	 * update a short integer in the given byte array at the specified position
	 * 
	 * @param data
	 *            a byte array
	 * @param value
	 *            the value to be copied into data[]
	 * @param position
	 *            the position of tht value in data[]
	 * @exception java.io.IOException
	 *                I/O errors
	 */
	public static void setShortValue(short value, int position, byte[] data) throws java.io.IOException {
		/*
		 * creates a new data output stream to write data to underlying output
		 * stream
		 */

		OutputStream out = new ByteArrayOutputStream();
		DataOutputStream outstr = new DataOutputStream(out);

		// write the value to the output stream

		outstr.writeShort(value);

		// creates a byte array with this output stream size and the
		// valid contents of the buffer have been copied into it
		byte[] B = ((ByteArrayOutputStream) out).toByteArray();

		// copies the first 2 bytes of this byte array into data[]
		System.arraycopy(B, 0, data, position, 2);

	}

	/**
	 * Insert or update a string in the given byte array at the specified
	 * position.
	 * 
	 * @param data
	 *            a byte array
	 * @param value
	 *            the value to be copied into data[]
	 * @param position
	 *            the position of tht value in data[]
	 * @exception java.io.IOException
	 *                I/O errors
	 */
	public static void setStrValue(String value, int position, byte[] data) throws java.io.IOException {
		/*
		 * creates a new data output stream to write data to underlying output
		 * stream
		 */

		OutputStream out = new ByteArrayOutputStream();
		DataOutputStream outstr = new DataOutputStream(out);
		// write the value to the output stream
		outstr.writeUTF(value);
		// creates a byte array with this output stream size and the
		// valid contents of the buffer have been copied into it
		byte[] B = ((ByteArrayOutputStream) out).toByteArray();

		int sz = outstr.size();
		System.arraycopy(B, 0, data, position, sz);
		
		// copies the contents of this byte array into data[]Charset.forName("UTF-8")
		/*byte[] b = value.getBytes("UTF-8");
		short length = (short)b.length;
		byte[] B = new byte[b.length+2];
		B[0] = (byte)((length & 0xFF00) >> 8);
		B[1] = (byte)(length & 0x00FF);
		for(int i = 2; i < B.length; i++){
			B[i] = b[i-2];
		}
		System.arraycopy(B, 0, data, position, B.length);*/

	}

	/**
	 * Update a character in the given byte array at the specified position.
	 * 
	 * @param data
	 *            a byte array
	 * @param value
	 *            the value to be copied into data[]
	 * @param position
	 *            the position of tht value in data[]
	 * @exception java.io.IOException
	 *                I/O errors
	 */
	public static void setCharValue(char value, int position, byte[] data) throws java.io.IOException {
		/*
		 * creates a new data output stream to write data to underlying output
		 * stream
		 */

		OutputStream out = new ByteArrayOutputStream();
		DataOutputStream outstr = new DataOutputStream(out);

		// write the value to the output stream
		outstr.writeChar(value);

		// creates a byte array with this output stream size and the
		// valid contents of the buffer have been copied into it
		byte[] B = ((ByteArrayOutputStream) out).toByteArray();

		// copies contents of this byte array into data[]
		System.arraycopy(B, 0, data, position, 2);

	}

	public static void setDescValue(Descriptor value, int position, byte[] data) 
			throws java.io.IOException {
		
		for(int ind  = 0; ind < Descriptor.DESCRIPTOR_SIZE; ind++, position+=4){
			setIntValue(value.get(ind), position, data);
		}
	}
	
	public static void setIDValue(RID value, int position, byte[] data) 
			throws java.io.IOException {
		
		setIntValue(value.pageNo.pid, position, data);
		setIntValue(value.slotNo, position+4, data);
	}

}
