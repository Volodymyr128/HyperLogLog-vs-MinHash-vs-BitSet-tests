package algos;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import java.io.Serializable;

/**
 * Created by Vitaliy Andrieiev on 9/13/16.
 */
public class IndexData implements Serializable {

    private HyperLogLogPlus hyperLogLog = new HyperLogLogPlus(16);

    public HyperLogLogPlus getHyperLogLog() {
        return hyperLogLog;
    }

    public IndexData setVal(String s) {
        final byte[] bytes = s.getBytes();
        this.hyperLogLog.offerHashed(MurmurHash.hash64(bytes, bytes.length));
        return this;
    }

    public IndexData merge(IndexData index2) throws CardinalityMergeException {
        this.hyperLogLog = (HyperLogLogPlus) this.hyperLogLog.merge(index2.getHyperLogLog());
        return this;
    }

}
