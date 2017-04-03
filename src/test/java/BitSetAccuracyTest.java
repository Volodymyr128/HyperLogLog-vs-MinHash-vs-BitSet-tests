import algos.SparseBitSet;
import com.carrotsearch.sizeof.RamUsageEstimator;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;
import utils.SimilarityUtils;
import utils.SizeMetric;

import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import static junit.framework.TestCase.assertTrue;
import static utils.InMemoryDatasetUtils.generateArray;
import static utils.InMemoryDatasetUtils.generateSimilarArray;
import static utils.TestUtils.getDeviation;

/**
 * Created by volodymyr.bakhmatiuk on 4/3/17.
 */
@RunWith(JUnit4ClassRunner.class)
public class BitSetAccuracyTest {

    static final Supplier<String> UUID_SUPPLIER = () -> UUID.randomUUID().toString();
    static final Supplier<String> DOUBLE_UUID_SUPPLIER = () -> UUID.randomUUID().toString() + UUID.randomUUID().toString();

    @Test
    public void test_BitSet_onUniqueData() throws CardinalityMergeException {
        /*
         * Similarity 0.5F, means dataset2 consists on 50% from dataset1
         */
        final float SIMILARITY_05 = 0.5F;
        // 1х2 proportions
        testBitSet(SIMILARITY_05, 100, 200, SizeMetric.KBS, SizeMetric.KBS, 3, 0.00F,0.01F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_05, 10_000, 20_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.00F, 0.01F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_05, 1_000_000, 2_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.00F,0.01F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // 1х3 proportions
        testBitSet(SIMILARITY_05, 100, 300, SizeMetric.KBS, SizeMetric.KBS, 3, 0.089F,0.11F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_05, 10_000, 30_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.089F, 0.11F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_05, 1_000_000, 3_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.089F, 0.11F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // 1х4 proportions
        testBitSet(SIMILARITY_05, 100, 400, SizeMetric.KBS, SizeMetric.KBS, 3, 0.16F,0.17F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_05, 10_000, 40_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.16F, 0.17F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_05, 1_000_000, 4_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.16F, 0.17F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // 1x5 proportions
        testBitSet(SIMILARITY_05, 100, 500, SizeMetric.KBS, SizeMetric.KBS, 3, 0.21F,0.22F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_05, 10_000, 50_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.21F, 0.22F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_05, 1_000_000, 5_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.21F, 0.22F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // 1x6 proportions
        testBitSet(SIMILARITY_05, 100, 600, SizeMetric.KBS, SizeMetric.KBS, 3, 0.24F,0.25F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_05, 10_000, 60_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.24F, 0.25F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_05, 1_000_000, 6_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.24F, 0.25F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // 1x7 proportions
        testBitSet(SIMILARITY_05, 100, 700, SizeMetric.KBS, SizeMetric.KBS, 3, 0.27F,0.28F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_05, 10_000, 70_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.27F, 0.28F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_05, 1_000_000, 7_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.27F, 0.28F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        /*
         * Similarity 0.7F, means dataset2 consists on 50% from dataset1. + 10% of deviation if compare with 0.5F similarity
         */
        final float SIMILARITY_07 = 0.7F;
        // 1x2 proportion
        testBitSet(SIMILARITY_07, 100, 200, SizeMetric.KBS, SizeMetric.KBS, 3, 0.07F,0.08F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_07, 10_000, 20_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.07F, 0.08F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_07, 1_000_000, 2_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.07F,0.08F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // 1х3 proportions
        testBitSet(SIMILARITY_07, 100, 300, SizeMetric.KBS, SizeMetric.KBS, 3, 0.17F,0.18F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_07, 10_000, 30_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.17F, 0.18F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_07, 1_000_000, 3_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.17F, 0.18F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // 1х4 proportions
        testBitSet(SIMILARITY_07, 100, 400, SizeMetric.KBS, SizeMetric.KBS, 3, 0.24F,0.25F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_07, 10_000, 40_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.24F, 0.25F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_07, 1_000_000, 4_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.24F, 0.25F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // 1x5 proportions
        testBitSet(SIMILARITY_07, 100, 500, SizeMetric.KBS, SizeMetric.KBS, 3, 0.29F,0.31F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_07, 10_000, 50_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.29F, 0.31F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_07, 1_000_000, 5_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.29F, 0.31F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // 1x6 proportions
        testBitSet(SIMILARITY_07, 100, 600, SizeMetric.KBS, SizeMetric.KBS, 3, 0.34F,0.35F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_07, 10_000, 60_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.34F, 0.35F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_07, 1_000_000, 6_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.34F, 0.35F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // 1x7 proportions
        testBitSet(SIMILARITY_07, 100, 700, SizeMetric.KBS, SizeMetric.KBS, 3, 0.37F,0.38F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_07, 10_000, 70_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.37F, 0.38F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSet(SIMILARITY_07, 1_000_000, 7_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.37F, 0.38F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    /**
     *
     * @param similarity how much similar elements both arrays will have in common
     * @param input1Rows size of the first input array
     * @param input2Rows size of the second input array
     * @param input1Metric memory units to estimate the first array
     * @param input2Metric memory units to estimate the second array
     * @param precision
     * @param minExpectedDeviation test will pass if bitset result will have deviation more than {@code maxExpectedDeviation}
     * @param maxExpectedDeviation test will pass if bitset result will have deviation less than {@code maxExpectedDeviation}
     * @param supplier1 supplier to generate data for the first input (part of that data will be copied to the second input)
     * @param supplier2 supplier to generate data for the second input
     * @throws CardinalityMergeException
     */
    private void testBitSet(
            float similarity,
            int input1Rows,
            int input2Rows,
            SizeMetric input1Metric,
            SizeMetric input2Metric,
            int precision,
            float minExpectedDeviation,
            float maxExpectedDeviation,
            Supplier<String> supplier1,
            Supplier<String> supplier2
    ) throws CardinalityMergeException {
        System.out.println(
                String.format(
                        "Start BitSet test:\n\tsimilaruty = %1$f \n\tinput1Rows = %2$d \n\tinput2Rows = %3$d \n\tinput1Metric = %4$s \n\tinput2Metric = %5$s \n\tprecision = %6$d \n\tmaxExpectedDeviation = %7$f\n",
                        similarity, input1Rows, input2Rows, input1Rows, input2Rows, precision, maxExpectedDeviation)
        );
        String[] input1 = generateArray(String.class, supplier1, input1Rows);
        String[] input2 = generateSimilarArray(String.class, input1, input2Rows, supplier2, similarity);
        SparseBitSet bitSet1 = measure(SimilarityUtils::createSBS, input1, "Bit Set 1", input1Metric);
        SparseBitSet bitSet2 = measure(SimilarityUtils::createSBS, input2, "Bit Set 2", input2Metric);
        float bitSetSimilarity = SimilarityUtils.similarity(bitSet2, bitSet1);
        System.out.println(
                String.format("BiSet similarity = %1$f, deviation = %2$f", bitSetSimilarity, getDeviation(similarity, bitSetSimilarity, precision))
        );
        final float actualDeviation = Math.abs(bitSetSimilarity - similarity);
        assertTrue(minExpectedDeviation <= actualDeviation && actualDeviation <= maxExpectedDeviation);
    }

    public static <T, R> R measure(Function<T[], R> func, T[] input, String consumerName, SizeMetric sizeMetric) {
        System.out.println("Takes input of size " + input.length + ", with RAM usage of " + sizeMetric.convert(RamUsageEstimator.sizeOfAll(input), 2));
        long start = System.currentTimeMillis();
        R result = func.apply(input);
        long spent = (System.currentTimeMillis() - start) / 1000;
        System.out.println("Spent " + spent + " seconds to execute " + consumerName);
        System.out.println("Result takes " + sizeMetric.convert(RamUsageEstimator.sizeOf(result), 2));
        return result;
    }
}
