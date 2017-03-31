package algos;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;

import java.io.Serializable;

public class IndexBitSetData implements Serializable {

    private SparseBitSet bitSet = new SparseBitSet(Integer.MAX_VALUE);

    public SparseBitSet getBitSet() {
        return this.bitSet;
    }

    public IndexBitSetData setVal(String s) {
        this.bitSet.set((MurmurHash.hash(s.getBytes()) & 0x7fffffff) % Integer.MAX_VALUE);
        return this;
    }

    public IndexBitSetData merge(IndexBitSetData index2) throws CardinalityMergeException {
        this.bitSet.or(index2.getBitSet());
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexBitSetData indexData = (IndexBitSetData) o;

        return getBitSet() != null ? getBitSet().equals(indexData.getBitSet()) : indexData.getBitSet() == null;
    }

    @Override
    public int hashCode() {
        int result = 31 * (getBitSet() != null ? getBitSet().hashCode() : 0);
        return result;
    }
}
