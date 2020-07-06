package org.rumbledb.items;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.time.Period;
import org.rumbledb.api.Item;
import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.exceptions.IteratorFlowException;
import org.rumbledb.exceptions.UnexpectedTypeException;
import org.rumbledb.expressions.comparison.ComparisonExpression;
import org.rumbledb.types.ItemType;

public class DateItem extends AtomicItem {

    private static final long serialVersionUID = 1L;
    private ZonedDateTime value;
    private boolean hasTimeZone = true;

    public DateItem() {
        super();
    }

    DateItem(ZonedDateTime value, boolean hasTimeZone) {
        super();
        this.value = value;
        this.hasTimeZone = hasTimeZone;
    }

    DateItem(String dateTimeString) {
        this.value = DateTimeItem.parseDateTime(dateTimeString, ItemType.dateItem);
        if (!dateTimeString.endsWith("Z") && this.value.getZone() == DateTimeZone.getDefault()) {
            this.hasTimeZone = false;
            this.value = this.value.withZoneRetainFields(DateTimeZone.UTC);
        }
    }

    public ZonedDateTime getValue() {
        return this.value;
    }

    @Override
    public ZonedDateTime getDateTimeValue() {
        return this.value;
    }

    @Override
    public boolean isDate() {
        return true;
    }

    @Override
    public boolean hasDateTime() {
        return true;
    }

    @Override
    public boolean hasTimeZone() {
        return this.hasTimeZone;
    }

    @Override
    public boolean getEffectiveBooleanValue() {
        return false;
    }

    @Override
    public boolean equals(Object otherObject) {
        if (!(otherObject instanceof Item)) {
            return false;
        }
        Item otherItem = (Item) otherObject;
        if (otherItem.isDate()) {
            return this.getValue().isEqual(otherItem.getDateTimeValue());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.getValue().hashCode();
    }

    @Override
    public boolean isCastableAs(ItemType itemType) {
        return itemType.equals(ItemType.dateItem)
            ||
            itemType.equals(ItemType.dateTimeItem)
            ||
            itemType.equals(ItemType.stringItem);
    }

    @Override
    public Item castAs(ItemType itemType) {
        if (itemType.equals(ItemType.dateItem)) {
            return this;
        }
        if (itemType.equals(ItemType.dateTimeItem)) {
            return ItemFactory.getInstance().createDateTimeItem(this.getValue(), this.hasTimeZone);
        }
        if (itemType.equals(ItemType.stringItem)) {
            return ItemFactory.getInstance().createStringItem(this.serialize());
        }
        throw new ClassCastException();
    }

    @Override
    public boolean isTypeOf(ItemType type) {
        return type.equals(ItemType.dateItem) || super.isTypeOf(type);
    }

    @Override
    public Item add(Item other) {
        if (other.isYearMonthDuration() || other.isDayTimeDuration()) {
            return ItemFactory.getInstance()
                .createDateItem(this.getValue().plus(other.getDurationValue()), this.hasTimeZone);
        }
        throw new ClassCastException();
    }

    @Override
    public Item subtract(Item other) {
        if (other.isDate()) {
            return ItemFactory.getInstance()
                .createDayTimeDurationItem(new Period(other.getDateTimeValue(), this.getValue(), PeriodType.dayTime()));
        }
        if (other.isYearMonthDuration() || other.isDayTimeDuration()) {
            return ItemFactory.getInstance()
                .createDateItem(this.getValue().minus(other.getDurationValue()), this.hasTimeZone);
        }
        throw new ClassCastException();
    }

    @Override
    public int compareTo(Item other) {
        if (other.isNull()) {
            return 1;
        }
        if (other.isDate()) {
            return this.getValue().compareTo(other.getDateTimeValue());
        }
        throw new IteratorFlowException(
                "Cannot compare item of type "
                    + this.getDynamicType().toString()
                    +
                    " with item of type "
                    + other.getDynamicType().toString()
        );
    }

    @Override
    public Item compareItem(
            Item other,
            ComparisonExpression.ComparisonOperator comparisonOperator,
            ExceptionMetadata metadata
    ) {
        if (!other.isDate() && !other.isNull()) {
            throw new UnexpectedTypeException(
                    "\""
                        + this.getDynamicType().toString()
                        + "\": invalid type: can not compare for equality to type \""
                        + other.getDynamicType().toString()
                        + "\"",
                    metadata
            );
        }
        return super.compareItem(other, comparisonOperator, metadata);
    }

    @Override
    public String serialize() {
        String value = this.getValue().toString();
        String zone = this.getValue().getZone() == DateTimeZone.UTC ? "Z" : this.getValue().getZone().toString();
        int dateTimeSeparatorIndex = value.indexOf("T");
        return value.substring(0, dateTimeSeparatorIndex) + (this.hasTimeZone ? zone : "");
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeLong(this.getDateTimeValue().get, true);
        output.writeBoolean(this.hasTimeZone);
        output.writeString(this.getDateTimeValue().getZone().toString());
    }

    @Override
    public void read(Kryo kryo, Input input) {
        Long millis = input.readLong(true);
        this.hasTimeZone = input.readBoolean();
        ZoneId zone = ZoneId.of(input.readString());
        this.value = new ZonedDateTime(millis, zone);
    }

    @Override
    public ItemType getDynamicType() {
        return ItemType.dateItem;
    }
}
