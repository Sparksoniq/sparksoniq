package sparksoniq.jsoniq.runtime.iterator.functions.strings;

import sparksoniq.exceptions.IteratorFlowException;
import sparksoniq.jsoniq.item.IntegerItem;
import sparksoniq.jsoniq.item.Item;
import sparksoniq.jsoniq.item.StringItem;
import sparksoniq.jsoniq.item.metadata.ItemMetadata;
import sparksoniq.jsoniq.runtime.iterator.RuntimeIterator;
import sparksoniq.jsoniq.runtime.iterator.functions.base.LocalFunctionCallIterator;
import sparksoniq.jsoniq.runtime.metadata.IteratorMetadata;
import sparksoniq.semantics.DynamicContext;

import java.util.List;

public class SubstringFunctionIterator extends LocalFunctionCallIterator {

    private Item result;
    
    public SubstringFunctionIterator(List<RuntimeIterator> arguments, IteratorMetadata iteratorMetadata) {
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

        String result;
        StringItem stringItem = this.getSingleItemOfTypeFromIterator(this._children.get(0), StringItem.class);
        IntegerItem indexItem = this.getSingleItemOfTypeFromIterator(this._children.get(1), IntegerItem.class);
        int index = sanitizeIndexItem(indexItem);
        if (this._children.size() > 2) {
            IntegerItem endIndexItem = this.getSingleItemOfTypeFromIterator(this._children.get(2), IntegerItem.class);
            int endIndex = sanitizeEndIndex(stringItem, endIndexItem, index);
            result = stringItem.getStringValue().substring(index, endIndex);
        } else {
            result = stringItem.getStringValue().substring(index);
        }

        this.result = new StringItem(result, ItemMetadata.fromIteratorMetadata(getMetadata()));
        this._hasNext = true;
    }

    private int sanitizeEndIndex(StringItem stringItem, IntegerItem endIndexItem, int startIndex) {
        //char indexing starts from 1 in JSONiq
        return Math.min(stringItem.getStringValue().length(), startIndex + endIndexItem.getIntegerValue());
    }

    private int sanitizeIndexItem(IntegerItem indexItem) {
        //char indexing starts from 1 in JSONiq
        return indexItem.getIntegerValue() - 1 > 0 ? indexItem.getIntegerValue() - 1 : 0;
    }
}
