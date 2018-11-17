package sparksoniq.jsoniq.runtime.iterator.functions.sequences.aggregate;

import org.apache.spark.api.java.JavaRDD;
import sparksoniq.exceptions.IteratorFlowException;
import sparksoniq.exceptions.UnexpectedTypeException;
import sparksoniq.jsoniq.item.Item;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.jsoniq.runtime.metadata.IteratorMetadata;
import sparksoniq.semantics.DynamicContext;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

public class MaxFunctionIterator extends AggregateFunctionIterator {

    private RuntimeIterator _iterator;

    public MaxFunctionIterator(List<RuntimeIterator> arguments, IteratorMetadata iteratorMetadata) {
        super(arguments, AggregateFunctionOperator.MAX, iteratorMetadata);
    }

    @Override
    public void open(DynamicContext context) {
        super.open(context);

        _iterator = this._children.get(0);
        _iterator.open(_currentDynamicContext);
        if (_iterator.hasNext()) {
            this._hasNext = true;
        } else {
            this._hasNext = false;
        }
        _iterator.close();
    }

    @Override
    public Item next() {
        if (this._hasNext) {
            List<Item> results;
            if (!_iterator.isRDD()) {
                results = getItemsFromIteratorWithCurrentContext(_iterator);
            } else {
                results = _iterator.getRDD().collect();
            }
            this._hasNext = false;
            results.forEach(r -> {
                if (!r.isAtomic())
                    throw new UnexpectedTypeException("Max expression has non numeric args " +
                            r.serialize(), getMetadata());
            });

            return Collections.max(results);
        } else
            throw new IteratorFlowException(FLOW_EXCEPTION_MESSAGE + "MAX function",
                    getMetadata());
    }
}
