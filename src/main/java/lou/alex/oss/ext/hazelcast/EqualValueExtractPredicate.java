package lou.alex.oss.ext.hazelcast;

import com.hazelcast.query.impl.predicates.EqualPredicate;

import java.util.Map;
import java.util.function.Function;

public class EqualValueExtractPredicate<V, T extends Comparable<T>> extends EqualPredicate {
    private final Function<V, T> extractor;

    public EqualValueExtractPredicate(String name, Function<V, T> extractor, T value) {
        super(name, value);
        this.extractor = extractor;
    }

    @Override
    protected Object readAttributeValue(Map.Entry entry) {
        return extractor.apply((V)entry.getValue());
    }
}
