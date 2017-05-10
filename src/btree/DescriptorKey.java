package btree;

import global.Descriptor;

/**  IntegerKey: It extends the KeyClass.
 *   It defines the integer Key.
 */ 
public class DescriptorKey extends KeyClass {

  private Descriptor key;

  public String toString(){
     return key.toString();
  }

  /** Class constructor
   *  @param     value   the value of the integer key to be set 
   */
  public DescriptorKey(Descriptor value) 
  { 
    key=new Descriptor(value);
  }

  


 
}
