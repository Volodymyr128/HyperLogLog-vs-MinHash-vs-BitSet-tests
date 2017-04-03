package algos.utils;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import algos.IndexBitSetData;
import algos.SparseBitSet;

/**
 * Created by volodymyr.bakhmatiuk on 3/28/17.
 */
public class SimilarityUtils {

    public static SparseBitSet createSBS(String[] input) {
        IndexBitSetData bitSetData = new IndexBitSetData();
        for (String str: input) {
            bitSetData.setVal(str);
        }
        return bitSetData.getBitSet();
    }

    public static HyperLogLogPlus createHLL(String[] input) {
        HyperLogLogPlus hyperLogLog = new HyperLogLogPlus(16);
        for (String str: input) {
            hyperLogLog.offerHashed(MurmurHash.hash64(str.getBytes(), str.getBytes().length));
        }
        return hyperLogLog;
    }


    public static float similarity(SparseBitSet sparseBitSet1, SparseBitSet sparseBitSet2) {
        int valuesTotal = 0;
        int matched = 0;
        for (int i = sparseBitSet1.nextSetBit(0); i >= 0; i = sparseBitSet1.nextSetBit(i + 1)) {
            valuesTotal++;
            if (sparseBitSet2.get(i)) {
                matched++;
            }
        }
        return valuesTotal == 0 ? 0.0f : matched / Float.valueOf(valuesTotal);
    }

    public static float similarity(HyperLogLogPlus hyperLogLog1, HyperLogLogPlus hyperLogLog2) throws CardinalityMergeException {
        HyperLogLogPlus merge = new HyperLogLogPlus(16);
        merge.addAll(hyperLogLog1);
        merge.addAll(hyperLogLog2);
        long intersectEstimate = (hyperLogLog1.cardinality() + hyperLogLog2.cardinality()) - merge.cardinality();
        float  result = intersectEstimate / (float) hyperLogLog1.cardinality();
//        logger.info("similarity to 2 : " + intersectEstimate / (double) hyperLogLogPlus2.cardinality());
        return result;
    }
}
