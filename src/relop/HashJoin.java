package relop;

import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
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
    private IndexScan outer;
    private IndexScan inner;
    private Predicate[] preds;
    private Schema schema;
    private int lcount;
    private HashTableDup table;

    // boolean variable to indicate whether the pre-fetched tuple is consumed or not
    private boolean nextTupleIsConsumed;

    // pre-fetched tuple
    private Tuple nextTuple;

    Tuple[] tuples;
    Tuple rightTuple;
    int tCount = 0;
    int tLen = 0;

    private HashJoin(Iterator left, Iterator right) {
        Schema lSchema = left.getSchema();
        Schema rSchema = right.getSchema();
        this.schema = Schema.join(lSchema, rSchema);
        nextTupleIsConsumed = true;
        lcount = left.getSchema().getCount();
        table = new HashTableDup();
    }

    public HashJoin(Iterator left, Iterator right, int lHashCol, int rHashCol) {
        this(left, right);
        formPreds(lHashCol, rHashCol);
        this.outer = convertScan(left, preds, 'l');
        this.inner = convertScan(right, preds, 'r');

        addToHashTable();
    }

    public HashJoin(Iterator left, Iterator right, Predicate[] preds) {
        this(left, right);
        this.preds = preds;
        this.outer = convertScan(left, preds, 'l');
        this.inner = convertScan(right, preds, 'r');
        addToHashTable();
    }

    private void formPreds(int lHashCol, int rHashCol) {
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

    private void addToHashTable() {
        Tuple t;
        SearchKey k;
        while (outer.hasNext()){
            t = outer.getNext();
            k = outer.getLastKey();
            table.add(k, t);
        }
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     *
     */
    public boolean hasNext() {

        if (!nextTupleIsConsumed)
            return true;

        if (tCount < tLen) {
            Tuple temp = tuples[tCount];
            tCount++;
            nextTuple = Tuple.join(temp, rightTuple, this.schema);
            for (int i = 0; i < preds.length; i++) {
                if (preds[i].evaluate(nextTuple)) {
                    nextTupleIsConsumed = false;
                    return true;
                }
            }
        }



        SearchKey rightKey;

        while (true){
            while (inner.hasNext()){
                rightTuple = inner.getNext();
                rightKey = inner.getLastKey();

                tuples = table.getAll(rightKey);

                if (tuples == null) { continue;}

                tLen = tuples.length;

                for (tCount = 0; tCount < tLen; tCount++) {
                    Tuple temp = tuples[tCount];
                    nextTuple = Tuple.join(temp, rightTuple, this.schema);
                    for (int i = 0; i < preds.length; i++) {
                        if (preds[i].evaluate(nextTuple)) {
                            nextTupleIsConsumed = false;
                            tCount++;
                            return true;
                        }
                    }
                }
            }
            if (!outer.hasNext()) return false;
        }
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
        } else if (iter instanceof HashJoin) {
                type = 'h';
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
    private IndexScan convertScan(Iterator iter, Predicate[] preds, char indicator) {
        char type = iterType(iter);
        int searchCol = (indicator == 'l')? (int)preds[0].left:(int)preds[0].right-lcount;
        switch (type){
            case 'f':
                FileScan fs = (FileScan)iter;
                HashIndex tempIndex = new HashIndex(null);
                while(iter.hasNext()){
                    Tuple t = fs.getNext();
                    RID rid = fs.currRID;
                    tempIndex.insertEntry(new SearchKey(t.getField(searchCol)),rid);
                }
                iter.close();
                return new IndexScan(iter.getSchema(), tempIndex, fs.file);
            case 'k':
                KeyScan ks = (KeyScan) iter;
                return new IndexScan(ks.getSchema(), ks.index, ks.file);
            case 'h':
                HeapFile hf = new HeapFile(null);
                HashIndex index = new HashIndex(null);
                Tuple t;
                while (iter.hasNext()) {
                    t = iter.getNext();
                    RID rid = hf.insertRecord(t.getData());
                    index.insertEntry(new SearchKey(t.getField(searchCol)), rid);
                }
                iter.close();
                return new IndexScan(iter.getSchema(),index,hf);
            default:
                return (IndexScan) iter;
        }

    }





}
