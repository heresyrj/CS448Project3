package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

  HeapFile file;
  HeapScan scan;
  Schema schema;
  RID currRID;
  RID lastRID;

  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
    this.schema = schema;
    this.file = file;
    scan = file.openScan();
    currRID = new RID();
    lastRID = new RID();
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    System.out.println("Filescan depth "+depth);
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    close();
    scan = file.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return scan != null;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    scan.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return scan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    byte[] data;
    RID curr =  new RID();
    data = scan.getNext(curr);
    lastRID.copyRID(currRID);//update lastRID
    currRID.copyRID(curr);//update currRID
    return new Tuple(schema,data);
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    return lastRID;
  }

  public Schema getSchema() { return schema; }

} // public class FileScan extends Iterator
