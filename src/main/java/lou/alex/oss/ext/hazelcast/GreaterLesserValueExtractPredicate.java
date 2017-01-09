package lou.alex.oss.ext.hazelcast;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.ComparisonType;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.AbstractIndexAwarePredicate;
import com.hazelcast.query.impl.predicates.GreaterLessPredicate;
import com.hazelcast.query.impl.predicates.NegatablePredicate;
import com.hazelcast.query.impl.predicates.PredicateDataSerializerHook;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Created by Alex on 1/9/2017.
 */
public class GreaterLesserValueExtractPredicate<V, T extends Comparable<T>> extends AbstractIndexAwarePredicate implements NegatablePredicate {
    protected Comparable value;
    boolean equal;
    boolean less;
    private final Function<V, T> extractor;
    private final String name;

    public GreaterLesserValueExtractPredicate(String attribute, T value, boolean equal, boolean less, Function<V, T> extractor) {
        super(attribute);

        this.name = attribute;
        this.extractor = extractor;

        if (value == null) {
            throw new NullPointerException("Arguments can't be null");
        }

        this.value = value;
        this.equal = equal;
        this.less = less;
    }

    @Override
    protected Object readAttributeValue(Map.Entry entry) {
        return extractor.apply((V)entry.getValue());
    }

    @Override
    protected boolean applyForSingleAttributeValue(Map.Entry mapEntry, Comparable attributeValue) {
        if (attributeValue == null) {
            return false;
        }
        Comparable givenValue = convert(mapEntry, attributeValue, value);
        int result = attributeValue.compareTo(givenValue);
        return equal && result == 0 || (less ? (result < 0) : (result > 0));
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = getIndex(queryContext);
        final ComparisonType comparisonType;
        if (less) {
            comparisonType = equal ? ComparisonType.LESSER_EQUAL : ComparisonType.LESSER;
        } else {
            comparisonType = equal ? ComparisonType.GREATER_EQUAL : ComparisonType.GREATER;
        }
        return index.getSubRecords(comparisonType, value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        value = in.readObject();
        equal = in.readBoolean();
        less = in.readBoolean();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(value);
        out.writeBoolean(equal);
        out.writeBoolean(less);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append(less ? "<" : ">");
        if (equal) {
            sb.append("=");
        }
        sb.append(value);
        return sb.toString();
    }

    @Override
    public Predicate negate() {
        return new GreaterLessPredicate(name, value, !equal, !less);
    }

    @Override
    public int getId() {
        return PredicateDataSerializerHook.GREATERLESS_PREDICATE;
    }
}
