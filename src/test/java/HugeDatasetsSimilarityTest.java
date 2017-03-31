import com.carrotsearch.sizeof.RamUsageEstimator;
import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;
import utils.SimilarityUtils;
import utils.SizeMetric;
import java.io.File;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertTrue;
import static utils.OnDiskDatasetUtils.*;
import static utils.TestUtils.*;

/**
 * Created by volodymyr.bakhmatiuk on 3/30/17.
 * This test is performed on huge datasets which overflow RAM memory
 */
@RunWith(JUnit4ClassRunner.class)
public class HugeDatasetsSimilarityTest {

    final static String FILE_NAME_1 = "input-1";
    final static String FILE_NAME_2 = "input-2";
    static final Supplier<String> UUID_SUPPLIER = () -> UUID.randomUUID().toString();
    static final Supplier<String> DOUBLE_UUID_SUPPLIER = () -> UUID.randomUUID().toString() + UUID.randomUUID().toString();

    @Test
    // PAY ATTENTION - the same deviation as on small data sets in SmallDatasetsSimilarityTest!!!
    public void test_HLL_onUniqueData() throws CardinalityMergeException {
        // 1х2 proportions
        testHLL(0.5F, 2_000_000, 4_000_000, SizeMetric.MBS, SizeMetric.MBS, 3, 0.00F,0.01F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // 1х3 proportions
        testHLL(0.5F, 2_000_000, 6_000_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.089F,0.11F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // 1x7 proportions
        testHLL(0.5F, 2_000_000, 14_000_000, SizeMetric.KBS, SizeMetric.KBS, 3, 0.27F,0.28F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    /**
     *
     * @param similarity how much similar elements both arrays will have in common
     * @param input1Rows size of the first input array
     * @param input2Rows size of the second input array
     * @param input1Metric memory units to estimate the first array
     * @param input2Metric memory units to estimate the second array
     * @param precision
     * @param minExpectedDeviation test will pass if hll result will have deviation more than {@code maxExpectedDeviation}
     * @param maxExpectedDeviation test will pass if hll result will have deviation less than {@code maxExpectedDeviation}
     * @param supplier1 supplier to generate data for the first input (part of that data will be copied to the second input)
     * @param supplier2 supplier to generate data for the second input
     * @throws CardinalityMergeException
     */
    private void testHLL(
            float similarity,
            int input1Rows,
            int input2Rows,
            SizeMetric array1Metric,
            SizeMetric array2Metric,
            int precision,
            float minExpectedDeviation,
            float maxExpectedDeviation,
            Supplier<String> supplier1,
            Supplier<String> supplier2
    ) throws CardinalityMergeException {
        System.out.println(
                String.format(
                        "Start HLL test:\n\tsimilaruty = %1$f \n\tinput1Rows = %2$d \n\tinput2Rows = %3$d \n\tarray1Metric = %4$s \n\tarray2Metric = %5$s \n\tprecision = %6$d \n\tmaxExpectedDeviation = %7$f\n",
                        similarity, input1Rows, input2Rows, array1Metric, array2Metric, precision, maxExpectedDeviation)
        );
        //persist data on disk
        Arrays.asList(FILE_NAME_1, FILE_NAME_2).stream().map(File::new).forEach(f -> {
            if (f.exists()) f.delete();
        });
        persistDataToFile(FILE_NAME_1, input1Rows, supplier1, false);
        generateSimilarArrayToFile(FILE_NAME_1, FILE_NAME_2, input2Rows, similarity, supplier2);

        HyperLogLogPlus hll1 = createHLL(FILE_NAME_1, SizeMetric.MBS);
        HyperLogLogPlus hll2 = createHLL(FILE_NAME_2, SizeMetric.MBS);
        float hllSimilarity = SimilarityUtils.similarity(hll2, hll1);
        System.out.println(
                String.format("HLL similarity = %1$f, deviation = %2$f", hllSimilarity, getDeviation(similarity, hllSimilarity, precision))
        );
        final float actualDeviation = Math.abs(hllSimilarity - similarity);
        assertTrue(minExpectedDeviation <= actualDeviation && actualDeviation <= maxExpectedDeviation);
        System.out.println("Finish HLL test\n============");
    }

    public static HyperLogLogPlus createHLL(String inputFileName, SizeMetric sizeMetric) {
        HyperLogLogPlus hll = new HyperLogLogPlus(16);
        long spent = performFuncOnFileBatchByBatch(
                inputFileName,
                (String[] array) -> Stream.of(array).forEach(str -> hll.offerHashed(MurmurHash.hash64(str.getBytes(), str.getBytes().length))),
                100_000
        );
        System.out.println("Spent " + spent + " seconds to execute HLL");
        System.out.println("Result takes " + sizeMetric.convert(RamUsageEstimator.sizeOf(hll), 2));
        return hll;
    }
}
