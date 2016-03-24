package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class HashJoin extends Iterator {

    /** public Tuple[] getAll(SearchKey key)
     *  public void add(SearchKey key, Tuple value)
     *  are overwritten and given
     * */
    private HashTableDup hashTable;


    private Iterator outer;
    private Iterator inner;
    private int lHashCol;
    private int rHashCol;
    private Predicate[] preds;
    private Schema lSchema;
    private Schema rSchema;
    private Schema schema;

    private boolean startJoin = true;
    Tuple leftTuple;

    // boolean variable to indicate whether the pre-fetched tuple is consumed or not
    private boolean nextTupleIsConsumed;

    // pre-fetched tuple
    private Tuple nextTuple;

    private HashJoin(Iterator left, Iterator right) {
        this.outer = left;
        this.inner = right;
        lSchema = left.getSchema();
        rSchema = right.getSchema();
        this.schema = Schema.join(lSchema, rSchema);
        hashTable = new HashTableDup();
        nextTupleIsConsumed = true;
    }

    public HashJoin(Iterator left, Iterator right, int lHashCol, int rHashCol) {
        this(left, right);
        this.lHashCol = lHashCol;
        this.rHashCol = rHashCol;
    }

    public HashJoin(Iterator left, Iterator right, Predicate[] preds) {
        this(left, right);
        this.preds = preds;
    }

    /**
     * Gives a one-line explanation of the iterator, repeats the call on any
     * child iterators, and increases the indent depth along the way.
     */
    public void explain(int depth) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Restarts the iterator, i.e. as if it were just constructed.
     */
    public void restart() {
        outer.restart();
        nextTupleIsConsumed = true;
    }

    /**
     * Returns true if the iterator is open; false otherwise.
     */
    public boolean isOpen() {
        return outer.isOpen();
    }

    /**
     * Closes the iterator, releasing any resources (i.e. pinned pages).
     */
    public void close() {
        outer.close();
        inner.close();
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     *
     */
    public boolean hasNext() {
        return false;
    }

    /**
     * Gets the next tuple in the iteration.
     *
     * @throws IllegalStateException if no more tuples
     */
    public Tuple getNext() {
        nextTupleIsConsumed = true;
        return nextTuple;
    }


    /**
     ************************ Utilities *******************************
     * Convert the input iterators into HashIndex iter if already is.
     * After the conversion, both Iter will use same hash func
     * in Bucketscan provided to finish partitioning phase.
     */


}
