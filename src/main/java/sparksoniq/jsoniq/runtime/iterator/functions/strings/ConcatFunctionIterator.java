package sparksoniq.jsoniq.runtime.iterator.functions.strings;

import sparksoniq.exceptions.IteratorFlowException;
import sparksoniq.jsoniq.item.Item;
import sparksoniq.jsoniq.item.StringItem;
import sparksoniq.jsoniq.item.metadata.ItemMetadata;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.jsoniq.runtime.iterator.functions.base.LocalFunctionCallIterator;
import sparksoniq.jsoniq.runtime.metadata.IteratorMetadata;
import sparksoniq.semantics.DynamicContext;

import java.util.List;

public class ConcatFunctionIterator extends LocalFunctionCallIterator {
    public ConcatFunctionIterator(List<RuntimeIterator> arguments, IteratorMetadata iteratorMetadata) {
        super(arguments, iteratorMetadata);
    }

    @Override
    public Item next() {
        if (this.hasNext()) {
            this._hasNext = false;
            return result;
        } else
            throw new IteratorFlowException(RuntimeIterator.FLOW_EXCEPTION_MESSAGE + " substring function", getMetadata());
    }

    @Override
    public void open(DynamicContext context) {
        if (this._isOpen)
            throw new IteratorFlowException("Runtime iterator cannot be opened twice", getMetadata());
        this._isOpen = true;
        this._currentDynamicContext = context;

        StringBuilder builder = new StringBuilder("");
        for (RuntimeIterator iterator : this._children) {
            StringItem stringItem = this.getSingleItemOfTypeFromIterator(iterator, StringItem.class);
            builder.append(stringItem.getStringValue());
        }
        this.result = new StringItem(builder.toString(), ItemMetadata.fromIteratorMetadata(getMetadata()));
        this._hasNext = true;
    }

    private Item result;
}
