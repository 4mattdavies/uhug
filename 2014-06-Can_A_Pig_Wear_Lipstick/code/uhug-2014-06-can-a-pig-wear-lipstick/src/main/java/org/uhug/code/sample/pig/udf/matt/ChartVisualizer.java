package org.uhug.code.sample.pig.udf.matt;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 *
 * @author matt davies
 */
public class ChartVisualizer extends EvalFunc<Tuple> {
    private static final int DATA_INDEX = 0;
    private static final int MAX_VALUE_INDEX = 1;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();

 

    @Override
    public Schema outputSchema(Schema input) {
        Schema tupleSchema = new Schema();
        tupleSchema.add(new Schema.FieldSchema("plot", DataType.CHARARRAY));
        return tupleSchema;
    }

    @Override
    public Tuple exec(Tuple b) throws IOException {
        Tuple newTuple = mTupleFactory.newTuple();
        if (b == null || b.size() == 0) {
            // do nothing;
        } else {
            Double d = (Double) b.get(DATA_INDEX);
            Double maxValue = (Double) b.get(MAX_VALUE_INDEX);
            if (null != d) {
                newTuple.append(printStars(d, maxValue));
            
            }
        }
        return newTuple;
    }

    private String printStars(Double amt, Double maxValue) {
        StringBuilder sb = new StringBuilder();
        int padding = (int) (Math.ceil(maxValue) - Math.ceil(amt));
        for (int i = 0; i < padding; i++) {
            sb.append(" ");
        }
        for (int i = 0; i < (int) Math.ceil(amt); i++) {
            sb.append("*");
        }
        return sb.toString();
    }
}
