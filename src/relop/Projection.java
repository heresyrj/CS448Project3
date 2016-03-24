package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

  Iterator iter;
  private Integer[] fields;
  Schema ss = null;

  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
    this.iter = iter;
    this.fields = fields;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    iter.restart();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return iter.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    iter.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return iter.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    Tuple tuple = iter.getNext();

    if(tuple != null) {
      Object[] allFields = tuple.getAllFields();
      Object[] val = new Object[fields.length];
      for(int i = 0; i < fields.length; i++){
        val[i] = allFields[fields[i]];
      }
      tuple = new Tuple(ss, val);
    }

    return tuple;

  }

  public Schema getSchema() {
    Schema s = iter.getSchema();
    ss = new Schema(fields.length);
    for (int i = 0; i < fields.length; i++) {
      int num = fields[i];
      ss.initField(i, s.fieldType(num), s.fieldLength(num), s.fieldName(num));
    }
    iter.setSchema(ss);
    return ss;
  }

} // public class Projection extends Iterator
