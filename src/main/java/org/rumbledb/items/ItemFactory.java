package org.rumbledb.items;

import java.time.ZonedDateTime;
import java.time.Period;
import org.rumbledb.api.Item;
import org.rumbledb.exceptions.ExceptionMetadata;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class ItemFactory {

    private static ItemFactory instance;
    private Item nullItem;
    private Item trueBooleanItem;
    private Item falseBooleanItem;
    private Item zeroItem;

    public static ItemFactory getInstance() {
        if (instance == null) {
            instance = new ItemFactory();
            instance.nullItem = new NullItem();
            instance.trueBooleanItem = new BooleanItem(true);
            instance.falseBooleanItem = new BooleanItem(false);
            instance.zeroItem = new IntegerItem(0);
        }
        return instance;
    }

    public Item createStringItem(String s) {
        return new StringItem(s);
    }

    public Item createBooleanItem(boolean b) {
        return b ? this.trueBooleanItem : this.falseBooleanItem;
    }

    public Item createNullItem() {
        return this.nullItem;
    }

    public Item createIntegerItem(int i) {
        if (i == 0) {
            return this.zeroItem;
        }
        return new IntegerItem(i);
    }

    public Item createDecimalItem(BigDecimal d) {
        return new DecimalItem(d);
    }

    public Item createDoubleItem(double d) {
        return new DoubleItem(d);
    }

    public Item createDurationItem(Period p) {
        return new DurationItem(p);
    }

    public Item createYearMonthDurationItem(Period p) {
        return new YearMonthDurationItem(p);
    }

    public Item createDayTimeDurationItem(Period p) {
        return new DayTimeDurationItem(p);
    }

    public Item createDateTimeItem(ZonedDateTime dt, boolean hasTimeZone) {
        return new DateTimeItem(dt, hasTimeZone);
    }

    public Item createDateTimeItem(String s) {
        return new DateTimeItem(s);
    }

    public Item createDateItem(ZonedDateTime dt, boolean hasTimeZone) {
        return new DateItem(dt, hasTimeZone);
    }

    public Item createDateItem(String s) {
        return new DateItem(s);
    }

    public Item createTimeItem(DateTime dt, boolean hasTimeZone) {
        return new TimeItem(dt, hasTimeZone);
    }

    public Item createTimeItem(String s) {
        return new TimeItem(s);
    }

    public Item createAnyURIItem(String s) {
        return new AnyURIItem(s);
    }

    public Item createHexBinaryItem(String s) {
        return new HexBinaryItem(s);
    }

    public Item createBase64BinaryItem(String s) {
        return new Base64BinaryItem(s);
    }

    public Item createObjectItem() {
        return new ObjectItem();
    }

    public Item createArrayItem() {
        return new ArrayItem();
    }

    public Item createArrayItem(List<Item> items) {
        return new ArrayItem(items);
    }

    public Item createObjectItem(List<String> keys, List<Item> values, ExceptionMetadata itemMetadata) {
        return new ObjectItem(keys, values, itemMetadata);
    }

    public Item createObjectItem(Map<String, List<Item>> keyValuePairs) {
        return new ObjectItem(keyValuePairs);
    }

}
