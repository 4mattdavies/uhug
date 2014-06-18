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
public class PrettyTime extends EvalFunc<Tuple> {
    private static final int TIME_INDEX = 0;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd";
    private static String format = DEFAULT_FORMAT;

    public PrettyTime() {
    }

    public PrettyTime(String format) {
        this.format = format;
    }

    @Override
    public Schema outputSchema(Schema input) {
        Schema tupleSchema = new Schema();
        tupleSchema.add(new Schema.FieldSchema("time", DataType.CHARARRAY));
        return tupleSchema;
    }

    @Override
    public Tuple exec(Tuple b) throws IOException {
        Tuple newTuple = mTupleFactory.newTuple();
        if (b == null || b.size() == 0) {
            // do nothing;
        } else {
            org.joda.time.DateTime d = (org.joda.time.DateTime) b.get(TIME_INDEX);
            if (null != d) {
                newTuple.append(d.toString(format));
            }
        }
        return newTuple;
    }
}
