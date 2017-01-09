package lou.alex.oss.ext.hazelcast;

import com.hazelcast.query.impl.predicates.LikePredicate;

import java.util.Map;
import java.util.function.Function;

public class LikeValueExtractPredicate<V, T> extends LikePredicate {
    private final Function<V, T> extractor;

    public LikeValueExtractPredicate(String name, Function<V, T> extractor, String expression) {
        super(name, expression);
        this.extractor = extractor;
    }

    @Override
    protected Object readAttributeValue(Map.Entry entry) {
        return extractor.apply((V)entry.getValue());
    }
}
