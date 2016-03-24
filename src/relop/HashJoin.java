package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class HashJoin extends Iterator {

    private Iterator outer;
    private Iterator inner;
    private Predicate[] preds;
    private Schema schema;
    private Schema lSchema;
    private Schema rSchema;

    private boolean startJoin = true;
    Tuple leftTuple;

    // boolean variable to indicate whether the pre-fetched tuple is consumed or not
    private boolean nextTupleIsConsumed;

    // pre-fetched tuple
    private Tuple nextTuple;

    /**
     * Constructs a join, given the left and right iterators and join predicates
     * (relative to the combined schema).
     */
    public HashJoin(Iterator left, Iterator right, Predicate... preds) {
        this.outer = left;
        this.inner = right;
        this.preds = preds;
        lSchema = left.getSchema();
        rSchema = right.getSchema();
        this.schema = Schema.join(lSchema, rSchema);

        nextTupleIsConsumed = true;
    }

    public void extract(Predicate pred) {

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
}
