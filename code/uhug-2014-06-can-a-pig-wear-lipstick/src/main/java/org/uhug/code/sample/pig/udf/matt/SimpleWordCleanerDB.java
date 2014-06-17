package org.uhug.code.sample.pig.udf.matt;

import java.io.IOException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 *
 * @author matt davies
 */
public class SimpleWordCleanerDB extends EvalFunc<DataBag> {

    private static final int WORD_INDEX = 0;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private BagFactory mBagFactory = BagFactory.getInstance();
    private String suffix = null;
    private Cleaner cleaner;

    public SimpleWordCleanerDB() {
        init(null);
    }

    public SimpleWordCleanerDB(String suffix) {
        init(suffix);
    }

    private void init(String suffix) {
        this.suffix = suffix;
        cleaner = new Cleaner(suffix);
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema outputSchema = new Schema();
            Schema recSchema = new Schema();
            recSchema.add(new Schema.FieldSchema("term", DataType.CHARARRAY));
            outputSchema.add(new Schema.FieldSchema("terms", recSchema, DataType.BAG));
            return outputSchema;
        } catch (FrontendException exx) {
            throw new RuntimeException(exx);
        }
    }

    @Override
    public DataBag exec(Tuple b) throws IOException {
        DataBag returnBag = mBagFactory.newDefaultBag();

        if (b == null || b.size() == 0) {
            // do nothing;
        } else {
            DataBag bag = (DataBag) b.get(0);
            if (null != bag) {
                for (Tuple t : bag) {
                    if (t == null || t.size() == 0) {
                        // do nothing;
                    } else {
                        String term = cleaner.cleanWord((String) t.get(WORD_INDEX));
                        if (null != term) {
                            Tuple newTuple = mTupleFactory.newTuple();
                            newTuple.append(term);
                            returnBag.add(newTuple);
                        }
                    }
                }
            }
        }
        return returnBag;
    }
}
