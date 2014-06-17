package org.uhug.code.sample.pig.udf.matt;

import java.util.Iterator;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author matt davies
 */
public class SimpleWordCleanerDBTest {

    @Test
    public void testSimple() throws Exception {
        BagFactory mBagFactory = BagFactory.getInstance();
        TupleFactory mTupleFactory = TupleFactory.getInstance();

        SimpleWordCleanerDB cleaner = new SimpleWordCleanerDB();
        DataBag inputBag = mBagFactory.newDefaultBag();

        Tuple tuple = mTupleFactory.newTuple();
        tuple.append("the5");
        inputBag.add(tuple);

        Tuple tuple2 = mTupleFactory.newTuple();
        tuple2.append("quick");
        inputBag.add(tuple2);

        // Convert to a tuple so it can run
        Tuple iTuple = mTupleFactory.newTuple();
        iTuple.append(inputBag);

        DataBag result = cleaner.exec(iTuple);
        assertEquals("Size should be 2", 2, result.size());

        Iterator<Tuple> tupleIterator = result.iterator();
        int i = 0;
        while (tupleIterator.hasNext()) {
            Tuple t = tupleIterator.next();
            switch (i) {
                case 0:
                    assertEquals("Should equal", "the", t.get(0).toString());
                    break;
                case 1:
                    assertEquals("Should equal", "quick", t.get(0).toString());
                    break;
            }
            i++;
        }
    }

}
