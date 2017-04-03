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
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertTrue;
import static utils.InMemoryDatasetUtils.*;
import static utils.OnDiskDatasetUtils.generateSimilarArrayToFile;
import static utils.OnDiskDatasetUtils.performFuncOnFileBatchByBatch;
import static utils.OnDiskDatasetUtils.persistDataToFile;
import static utils.TestUtils.*;

/**
 * Created by volodymyr.bakhmatiuk on 3/28/17.
 * This test is performed on small datasets which feet in RAM size
 */
@RunWith(JUnit4ClassRunner.class)
public class HyperLogLogAccuracyTest {

    final static String FILE_NAME_1 = "input-1";
    final static String FILE_NAME_2 = "input-2";
    static final Supplier<String> UUID_SUPPLIER = () -> UUID.randomUUID().toString();
    static final Supplier<String> DOUBLE_UUID_SUPPLIER = () -> UUID.randomUUID().toString() + UUID.randomUUID().toString();

    /**
     * DUPLICATES_0X argument means how much duplicates (in percentage perspective) input will have
     */
    private final float DUPLICATES_00 = 0.0F;
    private final float DUPLICATES_01 = 0.1F;
    private final float DUPLICATES_03 = 0.3F;
    private final float DUPLICATES_05 = 0.5F;
    private final float DUPLICATES_09 = 0.9F;

    /**
     * SIMILARITY_0X argument means which percentage of input2 will be copied from input1
     */
    private final float SIMILARITY_01 = 0.1F;
    private final float SIMILARITY_025 = 0.25F;
    private final float SIMILARITY_03 = 0.3F;
    private final float SIMILARITY_05 = 0.5F;
    private final float SIMILARITY_07 = 0.7F;
    private final float SIMILARITY_075 = 0.75F;

    // ===============================================
    // ================================================
    // PROOF #1: Input size do not influence accuracy
    // ================================================
    // ===============================================

    @Test
    public void deviationDoesNotDependsOnSize_1() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.00F;
        final float MAX_EXPECTED_DEVIATION = 0.01F;
        testHLLInMemory(SIMILARITY_01, 100, 700, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_01, 10_000, 70_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_01, 1_000_000, 7_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void deviationDoesNotDependsOnSize_2() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.00F;
        final float MAX_EXPECTED_DEVIATION = 0.01F;
        testHLLInMemory(SIMILARITY_05, 100, 200, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 10_000, 20_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 1_000_000, 2_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);

        testHLLOnDisk(0.5F, 2_000_000, 4_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void deviationDoesNotDependsOnSize_3() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.089F;
        final float MAX_EXPECTED_DEVIATION = 0.11F;
        testHLLInMemory(SIMILARITY_05, 100, 300, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 10_000, 30_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 1_000_000, 3_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);

        testHLLOnDisk(0.5F, 2_000_000, 6_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void deviationDoesNotDependsOnSize_4() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.16F;
        final float MAX_EXPECTED_DEVIATION = 0.173F;
        testHLLInMemory(SIMILARITY_05, 100, 400, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 10_000, 40_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 1_000_000, 4_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void deviationDoesNotDependsOnSize_5() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.21F;
        final float MAX_EXPECTED_DEVIATION = 0.22F;
        testHLLInMemory(SIMILARITY_05, 100, 500, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 1_000_000, 5_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void deviationDoesNotDependsOnSize_6() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.24F;
        final float MAX_EXPECTED_DEVIATION = 0.252F;
        testHLLInMemory(SIMILARITY_05, 100, 600, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 10_000, 60_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 1_000_000, 6_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void deviationDoesNotDependsOnSize_7() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.27F;
        final float MAX_EXPECTED_DEVIATION = 0.28F;
        testHLLInMemory(SIMILARITY_05, 100, 700, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 10_000, 70_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 1_000_000, 7_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);

        testHLLOnDisk(0.5F, 2_000_000, 14_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    // ===============================================
    // ================================================
    // PROOF #2: Size proportions between inputs DO influence accuracy
    // ================================================
    // ===============================================

    @Test
    public void influenceOfProportionBetweenInputs_1() throws CardinalityMergeException {
                                               // proportions:  1:1    1:2     1:3    1:4     1:5    1:6     1:7
        final float[] MIN_EXPECTED_DEVIATIONS = new float [] { 0.00F, 0.00F, 0.089F, 0.16F,  0.21F, 0.24F,  0.27F };
        final float[] MAX_EXPECTED_DEVIATIONS = new float [] { 0.01F, 0.01F, 0.11F,  0.173F, 0.22F, 0.252F, 0.28F };
        final int FIRST_INPUT_SIZE = 10_000;

        for (int i = 0; i < MIN_EXPECTED_DEVIATIONS.length; i++) {
            testHLLInMemory(SIMILARITY_05, FIRST_INPUT_SIZE, (i + 1) * FIRST_INPUT_SIZE, MIN_EXPECTED_DEVIATIONS[i], MAX_EXPECTED_DEVIATIONS[i], UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        }
    }



    // --------------------- TODO prove with formula? ------------------------------------

    @Test
    public void influenceOfSimilarityBetweenInputs() throws CardinalityMergeException {
                                             // similarities   0%      10%     20%     30%    40%      50%      60%    70%    80%    90%    100%
        final float[] MIN_EXPECTED_DEVIATIONS = new float[] { 0.000F, 0.000F, 0.000F, 0.07F, 0.148F,  0.213F, 0.264F, 0.29F, 0.29F, 0.230F, 0.0F };
        final float[] MAX_EXPECTED_DEVIATIONS = new float[] { 0.006F, 0.004F, 0.002F, 0.08F, 0.152F,  0.216F, 0.268F, 0.31F, 0.31F, 0.236F, 0.0F };

        for (int i = 0; i <= 10; i++) {
            float similarity = i / (float) 10;
            testHLLInMemory(similarity, 10_000, 50_000, MIN_EXPECTED_DEVIATIONS[i], MAX_EXPECTED_DEVIATIONS[i], UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        }
    }

    @Test
    public void influenceOfProportionBetweenInputs_2() throws CardinalityMergeException {
                                               // proportions:  1:1    1:2    1:3    1:4    1:5    1:6    1:7
        final float[] MIN_EXPECTED_DEVIATIONS = new float [] { 0.00F, 0.07F, 0.17F, 0.24F, 0.29F, 0.34F, 0.37F };
        final float[] MAX_EXPECTED_DEVIATIONS = new float [] { 0.01F, 0.18F, 0.18F, 0.25F, 0.31F, 0.35F, 0.38F };
        final int FIRST_INPUT_SIZE = 10_000;

        for (int i = 0; i < MIN_EXPECTED_DEVIATIONS.length; i++) {
            testHLLInMemory(SIMILARITY_07, FIRST_INPUT_SIZE, (i + 1) * FIRST_INPUT_SIZE, MIN_EXPECTED_DEVIATIONS[i], MAX_EXPECTED_DEVIATIONS[i], UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        }
    }

    @Test //TODO calc how intersection influence accuracy
    public void accuracyDependsOnIntersectionSize() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.245F;
        final float MAX_EXPECTED_DEVIATION = 0.254F;
        testHLLInMemory(SIMILARITY_05, 10_000, 60_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);  // intersection = 60_000 * 0.50 = 30_000
        testHLLInMemory(SIMILARITY_075, 10_000, 40_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER); // intersection = 40_000 * 0.75 = 30_000

        testHLLInMemory(SIMILARITY_05, 20_000, 120_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER); // intersection = 120_000 * 0.50 = 60_000
        testHLLInMemory(SIMILARITY_075, 20_000, 80_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER); // intersection = 80_000  * 0.75 = 60_000

        testHLLInMemory(0.365F, 10_000, 120_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);


        testHLLInMemory(0.8F, 10_000, 30_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER); //0.176-0.177
//        testHLLInMemory(0.8F, 10_000, 30_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER); //0.176-0.177
//        testHLLInMemory(0.9F, 10_000, 30_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER); //0.133-0.134
    }

    // ===============================================
    // ================================================
    // PROOF #5: Different duplicates DO influence accuracy
    // ===============================================
    // ================================================

    //TODO why the same duplicates proportions does NOT influence accuracy, but different DOES influence?
    /**
     * Change percentage of duplicates from 10% to 90% and deviation doesn't change. Use similarity 50%. Use different duplciates arguments for 1st and 2nd input
     * @throws CardinalityMergeException
     */
    @Test
    public void deviationDoesNotDependOnDuplicates_4() throws CardinalityMergeException {
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, 0.23F, 0.24F, getDuplicateSupplier(DUPLICATES_01, "s1"), getDuplicateSupplier(DUPLICATES_00, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, 0.21F, 0.22F, getDuplicateSupplier(DUPLICATES_01, "s1"), getDuplicateSupplier(DUPLICATES_01, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, 0.08F, 0.09F, getDuplicateSupplier(DUPLICATES_01, "s1"), getDuplicateSupplier(DUPLICATES_05, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, 0.28F, 0.29F, getDuplicateSupplier(DUPLICATES_01, "s1"), getDuplicateSupplier(DUPLICATES_09, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, 0.33F, 0.34F, getDuplicateSupplier(DUPLICATES_05, "s1"), getDuplicateSupplier(DUPLICATES_00, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, 0.31F, 0.32F, getDuplicateSupplier(DUPLICATES_05, "s1"), getDuplicateSupplier(DUPLICATES_01, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, 0.21F, 0.22F, getDuplicateSupplier(DUPLICATES_05, "s1"), getDuplicateSupplier(DUPLICATES_05, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, 0.16F, 0.17F, getDuplicateSupplier(DUPLICATES_05, "s1"), getDuplicateSupplier(DUPLICATES_09, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, 0.46F, 0.47F, getDuplicateSupplier(DUPLICATES_09, "s1"), getDuplicateSupplier(DUPLICATES_00, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, 0.45F, 0.46F, getDuplicateSupplier(DUPLICATES_09, "s1"), getDuplicateSupplier(DUPLICATES_01, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, 0.42F, 0.43F, getDuplicateSupplier(DUPLICATES_09, "s1"), getDuplicateSupplier(DUPLICATES_05, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, 0.21F, 0.22F, getDuplicateSupplier(DUPLICATES_09, "s1"), getDuplicateSupplier(DUPLICATES_09, "s2"));
    }

    // ---------------------------------------------------------


    // ===============================================
    // ================================================
    // PROOF #3: Similarity DO influence accuracy
    // ================================================
    // ===============================================

    @Test
    public void influenceOfSimilarityOnAccuracy_1() throws CardinalityMergeException {
        final int INPUT_1_SIZE = 10_000;
        final int INPUT_2_SIZE = 20_000;
        testHLLInMemory(SIMILARITY_05, INPUT_1_SIZE, INPUT_2_SIZE, 0.00F, 0.01F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_07, INPUT_1_SIZE, INPUT_2_SIZE, 0.07F, 0.08F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void influenceOfSimilarityOnAccuracy_2() throws CardinalityMergeException {
        final int INPUT_1_SIZE = 10_000;
        final int INPUT_2_SIZE = 30_000;
        testHLLInMemory(SIMILARITY_05, INPUT_1_SIZE, INPUT_2_SIZE, 0.089F, 0.11F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_07, INPUT_1_SIZE, INPUT_2_SIZE, 0.17F, 0.18F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void influenceOfSimilarityOnAccuracy_3() throws CardinalityMergeException {
        final int INPUT_1_SIZE = 10_000;
        final int INPUT_2_SIZE = 40_000;
        testHLLInMemory(SIMILARITY_05, INPUT_1_SIZE, INPUT_2_SIZE, 0.16F, 0.173F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_07, INPUT_1_SIZE, INPUT_2_SIZE, 0.24F, 0.25F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void influenceOfSimilarityOnAccuracy_4() throws CardinalityMergeException {
        final int INPUT_1_SIZE = 10_000;
        final int INPUT_2_SIZE = 50_000;
        testHLLInMemory(SIMILARITY_05, INPUT_1_SIZE, INPUT_2_SIZE, 0.21F, 0.22F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_07, INPUT_1_SIZE, INPUT_2_SIZE, 0.29F, 0.31F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void influenceOfSimilarityOnAccuracy_5() throws CardinalityMergeException {
        final int INPUT_1_SIZE = 10_000;
        final int INPUT_2_SIZE = 60_000;
        testHLLInMemory(SIMILARITY_05, INPUT_1_SIZE, INPUT_2_SIZE, 0.24F, 0.252F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_07, INPUT_1_SIZE, INPUT_2_SIZE, 0.34F, 0.35F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void influenceOfSimilarityOnAccuracy_6() throws CardinalityMergeException {
        final int INPUT_1_SIZE = 10_000;
        final int INPUT_2_SIZE = 70_000;
        testHLLInMemory(SIMILARITY_05, INPUT_1_SIZE, INPUT_2_SIZE, 0.27F, 0.28F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_07, INPUT_1_SIZE, INPUT_2_SIZE, 0.37F, 0.38F, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    // ===============================================
    // ================================================
    // PROOF #4: Same duplicates proportion do NOT influence accuracy.
    //
    // By duplicates here I mean elements which has copies in scope of the same input,
    // but do not intersect with any of the element of another input
    // ================================================
    // ===============================================

    /* Proof that expected similarity should not depends on number of duplicates per input/column:

       INPUT_1_CARDINALITY = INPUT_1_SIZE * ( 1 - DUPLICATES_1)
       INPUT_2_CARDINALITY = INPUT_2_SIZE * SIMILARITY * ( 1 - DUPLICATES_1) +  INPUT_2_SIZE * (1 - SIMILARITY) * (1 - DUPLICATES_2) // BUT! The first part of INPUT_2_CARDINALITY does not participate in merge cardinality estimation, since SIMILARITY part will be copied to input2
       MERGE_CARDINALITY = INPUT_1_CARDINALITY + second_part_of(INPUT_2_CARDINALITY)
       INTERSECT_CARDINALITY = INPUT_1_CARDINALITY + INPUT_2_CARDINALITY - MERGE_CARDINALITY = INPUT_2_SIZE * SIMILARITY * ( 1 - DUPLICATES_1) = the_first_part_of(INPUT_2_CARDINALITY)
       SIMILARITY(input2, input1) = INTERSECT_CARDINALITY / INPUT_2_CARDINALITY = the_first_part_of(INPUT_2_CARDINALITY) / INPUT_2_CARDINALITY = SIMILARITY / (SIMILARITY + 1 - SIMILARITY) = SIMILARITY
     */

    /**
     * Change percentage of duplicates from 10% to 90% and deviation doesn't change. Use similarity 10%
     * @throws CardinalityMergeException
     */
    @Test
    public void deviationDoesNotDependOnDuplicates_1() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.00F;
        final float MAX_EXPECTED_DEVIATION = 0.01F;
        testHLLInMemory(SIMILARITY_01, 10_000, 20_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_00, "s1"), getDuplicateSupplier(DUPLICATES_00, "s2"));
        testHLLInMemory(SIMILARITY_01, 10_000, 20_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_01, "s1"), getDuplicateSupplier(DUPLICATES_01, "s2"));
        testHLLInMemory(SIMILARITY_01, 10_000, 20_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_05, "s1"), getDuplicateSupplier(DUPLICATES_05, "s2"));
        testHLLInMemory(SIMILARITY_01, 10_000, 20_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_09, "s1"), getDuplicateSupplier(DUPLICATES_09, "s2"));
    }

    /**
     * Change percentage of duplicates from 10% to 90% and deviation doesn't change. Use similarity 30%
     * @throws CardinalityMergeException
     */
    @Test
    public void deviationDoesNotDependOnDuplicates_2() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.07F;
        final float MAX_EXPECTED_DEVIATION = 0.08F;
        testHLLInMemory(SIMILARITY_03, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_00, "s1"), getDuplicateSupplier(DUPLICATES_00, "s2"));
        testHLLInMemory(SIMILARITY_03, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_01, "s1"), getDuplicateSupplier(DUPLICATES_01, "s2"));
        testHLLInMemory(SIMILARITY_03, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_05, "s1"), getDuplicateSupplier(DUPLICATES_05, "s2"));
        testHLLInMemory(SIMILARITY_03, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_09, "s1"), getDuplicateSupplier(DUPLICATES_09, "s2"));
    }

    /**
     * Change percentage of duplicates from 10% to 90% and deviation doesn't change. Use similarity 50%
     * @throws CardinalityMergeException
     */
    @Test
    public void deviationDoesNotDependOnDuplicates_3() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.21F;
        final float MAX_EXPECTED_DEVIATION = 0.22F;
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_00, "s1"), getDuplicateSupplier(DUPLICATES_00, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_01, "s1"), getDuplicateSupplier(DUPLICATES_01, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_05, "s1"), getDuplicateSupplier(DUPLICATES_05, "s2"));
        testHLLInMemory(SIMILARITY_05, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_09, "s1"), getDuplicateSupplier(DUPLICATES_09, "s2"));
    }

    // ===============================================
    // ================================================
    // PROOF #6: The same accuracy on random and simple data
    // ===============================================
    // ================================================

    @Test
    public void randomVsSimpleData_1() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.16F;
        final float MAX_EXPECTED_DEVIATION = 0.173F;
        final Supplier<String> supplier1 = getDuplicateSupplier(0, 10_000, DUPLICATES_05, "1");
        final Supplier<String> supplier2 = getDuplicateSupplier(0, 40_000, DUPLICATES_05, "2");
        testHLLInMemory(SIMILARITY_05, 10_000, 40_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 10_000, 40_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, supplier1, supplier2);
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
    private void testHLLInMemory(
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
     * @param minExpectedDeviation test will pass if hll result will have deviation more than {@code maxExpectedDeviation}
     * @param maxExpectedDeviation test will pass if hll result will have deviation less than {@code maxExpectedDeviation}
     * @param supplier1 supplier to generate data for the first input (part of that data will be copied to the second input)
     * @param supplier2 supplier to generate data for the second input
     * @throws CardinalityMergeException
     */
    private void testHLLOnDisk(
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

        HyperLogLogPlus hll1 = makeHLLFromFile(FILE_NAME_1, SizeMetric.MBS);
        HyperLogLogPlus hll2 = makeHLLFromFile(FILE_NAME_2, SizeMetric.MBS);
        float hllSimilarity = SimilarityUtils.similarity(hll2, hll1);
        System.out.println(
                String.format("HLL similarity = %1$f, deviation = %2$f", hllSimilarity, getDeviation(similarity, hllSimilarity))
        );
        final float actualDeviation = Math.abs(hllSimilarity - similarity);
        assertTrue(minExpectedDeviation <= actualDeviation && actualDeviation <= maxExpectedDeviation);
        System.out.println("Finish HLL test\n============");
    }

    public static HyperLogLogPlus makeHLLFromFile(String inputFileName, SizeMetric sizeMetric) {
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

    public static <T, R> R measure(Function<T[], R> func, T[] input, String consumerName) {
        System.out.println("Takes input of size " + input.length + ", with RAM usage of " + convertSize(RamUsageEstimator.sizeOfAll(input)));
        long start = System.currentTimeMillis();
        R result = func.apply(input);
        long spent = (System.currentTimeMillis() - start) / 1000;
        System.out.println("Spent " + spent + " seconds to execute " + consumerName);
        System.out.println("Result takes " + convertSize(RamUsageEstimator.sizeOf(result)));
        return result;
    }


}
