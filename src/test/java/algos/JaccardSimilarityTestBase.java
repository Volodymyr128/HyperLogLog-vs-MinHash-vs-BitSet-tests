package algos;

import algos.SparseBitSet;
import com.carrotsearch.sizeof.RamUsageEstimator;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import algos.utils.SimilarityUtils;

import java.io.File;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;

import static junit.framework.TestCase.assertTrue;
import static algos.utils.InMemoryDatasetUtils.generateArray;
import static algos.utils.InMemoryDatasetUtils.generateSimilarArray;
import static algos.utils.OnDiskDatasetUtils.*;
import static algos.utils.TestUtils.convertSize;
import static algos.utils.TestUtils.getDeviation;

/**
 * Created by volodymyr.bakhmatiuk on 4/3/17.
 */
public abstract class JaccardSimilarityTestBase {

    final static String FILE_NAME_1 = "input-1";
    final static String FILE_NAME_2 = "input-2";

    /**
     *
     * @param similarity how much similar elements both arrays will have in common
     * @param input1Rows size of the first input array
     * @param input2Rows size of the second input array
     * @param minExpectedDeviation test will pass if hll result will have deviation more than {@code maxExpectedDeviation}
     * @param maxExpectedDeviation test will pass if hll result will have deviation less than {@code maxExpectedDeviation}
     * @param supplier1 supplier to generate data for the first input (part of that data will be copied to the second input)
     * @param supplier2 supplier to generate data for the second input
     * @throws CardinalityMergeException
     */
    protected void testHLLInMemory(
            float similarity,
            int input1Rows,
            int input2Rows,
            float minExpectedDeviation,
            float maxExpectedDeviation,
            Supplier<String> supplier1,
            Supplier<String> supplier2
    ) throws CardinalityMergeException {
        System.out.println(
                String.format(
                        "Start HLL test:\n\tsimilaruty = %1$f \n\tinput1Rows = %2$d \n\tinput2Rows = %3$d \n\tminExpectedDeviation = %4$f \n\tmaxExpectedDeviation = %5$f\n",
                        similarity, input1Rows, input2Rows, minExpectedDeviation, maxExpectedDeviation)
        );
        String[] input1 = generateArray(String.class, supplier1, input1Rows);
        String[] input2 = generateSimilarArray(String.class, input1, input2Rows, supplier2, similarity);
        HyperLogLogPlus hll1 = measure(SimilarityUtils::createHLL, input1, "Hyper Log Log 1");
        HyperLogLogPlus hll2 = measure(SimilarityUtils::createHLL, input2, "Hyper Log Log 2");
        float hllSimilarity = SimilarityUtils.similarity(hll2, hll1);
        System.out.println(
                String.format("HLL similarity = %1$f, deviation = %2$f", hllSimilarity, getDeviation(similarity, hllSimilarity))
        );
        final float actualDeviation = Math.abs(hllSimilarity - similarity);
        assertTrue(minExpectedDeviation <= actualDeviation && actualDeviation <= maxExpectedDeviation);
        System.out.println("Finish HLL test\n============");
    }

    /**
     *
     * @param similarity how much similar elements both arrays will have in common
     * @param input1Rows size of the first input array
     * @param input2Rows size of the second input array
     * @param minExpectedDeviation test will pass if bitset result will have deviation more than {@code maxExpectedDeviation}
     * @param maxExpectedDeviation test will pass if bitset result will have deviation less than {@code maxExpectedDeviation}
     * @param supplier1 supplier to generate data for the first input (part of that data will be copied to the second input)
     * @param supplier2 supplier to generate data for the second input
     * @throws CardinalityMergeException
     */
    protected void testBitSetInMemory(
            float similarity,
            int input1Rows,
            int input2Rows,
            float minExpectedDeviation,
            float maxExpectedDeviation,
            Supplier<String> supplier1,
            Supplier<String> supplier2
    ) throws CardinalityMergeException {
        System.out.println(
                String.format(
                        "Start BitSet test:\n\tsimilaruty = %1$f \n\tinput1Rows = %2$d \n\tinput2Rows = %3$d \n\tminExpectedDeviation = %4$f \n\tmaxExpectedDeviation = %5$f\n",
                        similarity, input1Rows, input2Rows, minExpectedDeviation, maxExpectedDeviation)
        );
        String[] input1 = generateArray(String.class, supplier1, input1Rows);
        String[] input2 = generateSimilarArray(String.class, input1, input2Rows, supplier2, similarity);
        SparseBitSet bitSet1 = measure(SimilarityUtils::createSBS, input1, "Bit Set 1");
        SparseBitSet bitSet2 = measure(SimilarityUtils::createSBS, input2, "Bit Set 2");
        float bitSetSimilarity = SimilarityUtils.similarity(bitSet2, bitSet1);
        System.out.println(
                String.format("BiSet similarity = %1$f, deviation = %2$f", bitSetSimilarity, getDeviation(similarity, bitSetSimilarity))
        );
        final float actualDeviation = Math.abs(bitSetSimilarity - similarity);
        assertTrue(minExpectedDeviation <= actualDeviation && actualDeviation <= maxExpectedDeviation);
        System.out.println("Finish BitSet test\n============");
    }

    /**
     *
     * @param similarity how much similar elements both arrays will have in common
     * @param input1Rows size of the first input array
     * @param input2Rows size of the second input array
     * @param minExpectedDeviation test will pass if hll result will have deviation more than {@code maxExpectedDeviation}
     * @param maxExpectedDeviation test will pass if hll result will have deviation less than {@code maxExpectedDeviation}
     * @param supplier1 supplier to generate data for the first input (part of that data will be copied to the second input)
     * @param supplier2 supplier to generate data for the second input
     * @throws CardinalityMergeException
     */
    protected void testHLLOnDisk(
            float similarity,
            int input1Rows,
            int input2Rows,
            float minExpectedDeviation,
            float maxExpectedDeviation,
            Supplier<String> supplier1,
            Supplier<String> supplier2
    ) throws CardinalityMergeException {
        System.out.println(
                String.format(
                        "Start HLL test:\n\tsimilaruty = %1$f \n\tinput1Rows = %2$d \n\tinput2Rows = %3$d \n\tminExpectedDeviation = %4$f \n\tmaxExpectedDeviation = %5$f\n",
                        similarity, input1Rows, input2Rows, minExpectedDeviation, maxExpectedDeviation)
        );
        //persist data on disk
        Arrays.asList(FILE_NAME_1, FILE_NAME_2).stream().map(File::new).forEach(f -> {
            if (f.exists()) f.delete();
        });
        persistDataToFile(FILE_NAME_1, input1Rows, supplier1, false);
        generateSimilarArrayToFile(FILE_NAME_1, FILE_NAME_2, input2Rows, similarity, supplier2);

        HyperLogLogPlus hll1 = makeHLLFromFile(FILE_NAME_1);
        HyperLogLogPlus hll2 = makeHLLFromFile(FILE_NAME_2);
        float hllSimilarity = SimilarityUtils.similarity(hll2, hll1);
        System.out.println(
                String.format("HLL similarity = %1$f, deviation = %2$f", hllSimilarity, getDeviation(similarity, hllSimilarity))
        );
        final float actualDeviation = Math.abs(hllSimilarity - similarity);
        assertTrue(minExpectedDeviation <= actualDeviation && actualDeviation <= maxExpectedDeviation);
        System.out.println("Finish HLL test\n============");
    }

    /**
     *
     * @param similarity how much similar elements both arrays will have in common
     * @param input1Rows size of the first input array
     * @param input2Rows size of the second input array
     * @param minExpectedDeviation test will pass if hll result will have deviation more than {@code maxExpectedDeviation}
     * @param maxExpectedDeviation test will pass if hll result will have deviation less than {@code maxExpectedDeviation}
     * @param supplier1 supplier to generate data for the first input (part of that data will be copied to the second input)
     * @param supplier2 supplier to generate data for the second input
     * @throws CardinalityMergeException
     */
    protected void testBitSetOnDisk(
            float similarity,
            int input1Rows,
            int input2Rows,
            float minExpectedDeviation,
            float maxExpectedDeviation,
            Supplier<String> supplier1,
            Supplier<String> supplier2
    ) throws CardinalityMergeException {
        System.out.println(
                String.format(
                        "Start BitSet test:\n\tsimilaruty = %1$f \n\tinput1Rows = %2$d \n\tinput2Rows = %3$d \n\tminExpectedDeviation = %4$f \n\tmaxExpectedDeviation = %5$f\n",
                        similarity, input1Rows, input2Rows, minExpectedDeviation, maxExpectedDeviation)
        );
        //persist data on disk
        Arrays.asList(FILE_NAME_1, FILE_NAME_2).stream().map(File::new).forEach(f -> {
            if (f.exists()) f.delete();
        });
        persistDataToFile(FILE_NAME_1, input1Rows, supplier1, false);
        generateSimilarArrayToFile(FILE_NAME_1, FILE_NAME_2, input2Rows, similarity, supplier2);

        SparseBitSet bitSet1 = makeBitSetFromFile(FILE_NAME_1);
        SparseBitSet bitSet2 = makeBitSetFromFile(FILE_NAME_2);
        float bitSetSimilarity = SimilarityUtils.similarity(bitSet1, bitSet2);
        System.out.println(
                String.format("HLL similarity = %1$f, deviation = %2$f", bitSetSimilarity, getDeviation(similarity, bitSetSimilarity))
        );
        final float actualDeviation = Math.abs(bitSetSimilarity - similarity);
        assertTrue(minExpectedDeviation <= actualDeviation && actualDeviation <= maxExpectedDeviation);
        System.out.println("Finish BitSet test\n============");
    }



    protected static <T, R> R measure(Function<T[], R> func, T[] input, String consumerName) {
        System.out.println("Takes input of size " + input.length + ", with RAM usage of " + convertSize(RamUsageEstimator.sizeOfAll(input)));
        long start = System.currentTimeMillis();
        R result = func.apply(input);
        long spent = (System.currentTimeMillis() - start) / 1000;
        System.out.println("Spent " + spent + " seconds to execute " + consumerName);
        System.out.println("Result takes " + convertSize(RamUsageEstimator.sizeOf(result)));
        return result;
    }
}
