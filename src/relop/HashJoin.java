package relop;

import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import index.HashIndex;

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
    private Predicate[] preds;
    private Schema lSchema;
    private Schema rSchema;
    private Schema schema;

    private boolean startJoin = true;

    // boolean variable to indicate whether the pre-fetched tuple is consumed or not
    private boolean nextTupleIsConsumed;

    // pre-fetched tuple
    private Tuple nextTuple;

    private HashJoin(Iterator left, Iterator right) {
        this.outer = convertScan(left);
        this.inner = convertScan(right);
        lSchema = left.getSchema();
        rSchema = right.getSchema();
        this.schema = Schema.join(lSchema, rSchema);
        hashTable = new HashTableDup();
        nextTupleIsConsumed = true;
    }

    public HashJoin(Iterator left, Iterator right, int lHashCol, int rHashCol) {
        this(left, right);
        formPreds(left, right, lHashCol, rHashCol);
    }

    public HashJoin(Iterator left, Iterator right, Predicate[] preds) {
        this(left, right);
        this.preds = preds;
    }

    public void formPreds(Iterator left, Iterator right, int lHashCol, int rHashCol) {
        int lcount = left.getSchema().getCount();
        preds = new Predicate[]{new Predicate(AttrOperator.EQ, AttrType.FIELDNO, lHashCol, AttrType.FIELDNO, rHashCol+lcount)};
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

    public Schema getSchema() {
        return schema;
    }

    public void probing() {
        inner.getNext();
    }



    /**
     ************************ Utilities *******************************
     * Convert the input iterators into HashIndex iter if already is.
     * After the conversion, both Iter will use same hash func
     * in Bucketscan provided to finish partitioning phase.
     */

    private char iterType(Iterator iter) {
        char type;
        if(iter instanceof IndexScan) {
            type = 'i';
        } else if (iter instanceof KeyScan) {
            type = 'k';
        } else if (iter instanceof FileScan) {
            type = 'f';
        } else {
            type = 'e';//error
        }
        return type;
    }

    /**
     * 1. Both KeyScan and IndexScan have built-in bucketScan
     * And all the implementation related to Iterator has same mechanism
     * Therefore needless to change.
     * 2. The only case need to be converted is FileScan
     * However, it's inefficient to scan all the entries to build index,
     * which is also required not to do so.
     * */
    public Iterator convertScan(Iterator iter) {
        char type = iterType(iter);
        if(type == 'f') {
            FileScan fs = (FileScan) iter;
            HashIndex tempIndex = new HashIndex(null);
            int i = 1;
            while(fs.hasNext()){
                RID rid = fs.getNextRID();
                tempIndex.insertEntry(new SearchKey(i),rid);
                i++;
            }
            fs.close();
            tempIndex.printSummary();
            return new IndexScan(fs.getSchema(), tempIndex, fs.file);
        } else {
            return iter;
        }
    }



}
