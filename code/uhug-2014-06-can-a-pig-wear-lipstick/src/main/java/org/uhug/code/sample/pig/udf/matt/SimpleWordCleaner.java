package org.uhug.code.sample.pig.udf.matt;

import java.io.IOException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 *
 * @author matt davies
 */
public class SimpleWordCleaner extends EvalFunc<Tuple> {
    private static final int WORD_INDEX = 0;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private String suffix = null;
    private Cleaner cleaner;

    public SimpleWordCleaner() {
        init(null);
    }

    public SimpleWordCleaner(String suffix) {
        init(suffix);
    }

    private void init(String suffix) {
        this.suffix = suffix;
        cleaner = new Cleaner(suffix);
    }
    

    @Override
    public Schema outputSchema(Schema input) {
        Schema tupleSchema = new Schema();
        tupleSchema.add(new Schema.FieldSchema("term", DataType.CHARARRAY));
        return tupleSchema;
    }

    @Override
    public Tuple exec(Tuple b) throws IOException {
        Tuple newTuple = mTupleFactory.newTuple();
        if (b == null || b.size() == 0) {
            // do nothing;
        } else {
            newTuple.append(cleaner.cleanWord((String) b.get(WORD_INDEX)));
        }
        return newTuple;
    }

    
}
