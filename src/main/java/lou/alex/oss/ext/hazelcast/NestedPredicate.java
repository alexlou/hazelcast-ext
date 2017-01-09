package lou.alex.oss.ext.hazelcast;

import com.hazelcast.query.Predicate;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;

public class NestedPredicate<ETY, FST, SND extends Comparator<SND>> implements Predicate {

    public enum Association {
        All, Any
    }

    private final Function<ETY, FST> firstExtractor;
    private final Function<ETY, Iterable<FST>> firstsExtractor;
    private final Association firstAssociation;
    private final Function<FST, SND> secondExtractor;
    private final Predicate secondPredicate;

    public NestedPredicate(
            Association firstAsso,
            Function<ETY, FST> firstExtractor,
            Function<FST, SND> secondExtractor,
            Predicate secondPredicate) {
        this.firstExtractor = (Function<ETY, FST> & Serializable) firstExtractor;
        this.firstsExtractor = null;
        this.firstAssociation = firstAsso;
        this.secondExtractor = (Function<FST, SND> & Serializable) secondExtractor;
        this.secondPredicate = secondPredicate;
    }

    public NestedPredicate(
            Function<ETY, Iterable<FST>> firstsExtractor,
            Association firstAsso,
            Function<FST, SND> secondExtractor,
            Predicate secondPredicate) {
        this.firstsExtractor = (Function<ETY, Iterable<FST>> & Serializable) firstsExtractor;
        this.firstExtractor = null;
        this.firstAssociation = firstAsso;
        this.secondExtractor = (Function<FST, SND> & Serializable) secondExtractor;
        this.secondPredicate = secondPredicate;
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        if (firstExtractor != null) {
            FST first = firstExtractor.apply((ETY) mapEntry.getValue());
            SND SND = secondExtractor.apply(first);
            return secondPredicate.apply(new SimpleMapEntry<>(mapEntry.getKey(), SND));
        }
        Iterable<FST> firsts = firstsExtractor.apply((ETY) mapEntry.getValue());
        switch (firstAssociation) {
            case All: {
                for (FST first : firsts) {
                    SND SND = secondExtractor.apply(first);
                    if (! secondPredicate.apply(new SimpleMapEntry<>(mapEntry.getKey(), SND))) {
                        return false;
                    }
                }
                return true;
            }
            case Any: {
                for (FST first : firsts) {
                    SND SND = secondExtractor.apply(first);
                    if (secondPredicate.apply(new SimpleMapEntry<>(mapEntry.getKey(), SND))) {
                        return true;
                    }
                }
                return false;
            }
        }

        return false;
    }

    private static class SimpleMapEntry<K, V> implements Map.Entry<K, V> {
        private final K key;
        private V value;

        public SimpleMapEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            V tmp = this.value;
            this.value = value;
            return tmp;
        }
    }
}
