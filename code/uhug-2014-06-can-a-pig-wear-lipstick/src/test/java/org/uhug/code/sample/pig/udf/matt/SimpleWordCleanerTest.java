package org.uhug.code.sample.pig.udf.matt;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;
import static org.junit.Assert.*;
/**
 *
 * @author matt davies
 */
public class SimpleWordCleanerTest {

    @Test
    public void testNoChanges() throws Exception {
        String input = "testword";

        String output = runnit(input);
        assertEquals("Should equal", input, output);
    }

    @Test
    public void testRemovePrefix() throws Exception {
        String input = "1testword";
        String expected = "testword";

        String output = runnit(input);
        assertEquals("Should equal", expected, output);
    }

    @Test
    public void testRemoveSuffix() throws Exception {
        String input = "testword1";
        String expected = "testword";

        String output = runnit(input);
        assertEquals("Should equal", expected, output);
    }

    @Test
    public void testRemoveBoth() throws Exception {
        String input = "#testword1";
        String expected = "testword";

        String output = runnit(input);
        assertEquals("Should equal", expected, output);
    }

    private String runnit(String input) throws Exception {
        // create the cleaner
        SimpleWordCleaner cleaner = new SimpleWordCleaner();

        // simulate the UDF call
        TupleFactory mTupleFactory = TupleFactory.getInstance();
        Tuple tuple = mTupleFactory.newTuple();
        tuple.append(input);

        Tuple result = cleaner.exec(tuple);
        return result.get(0).toString();
    }
}
