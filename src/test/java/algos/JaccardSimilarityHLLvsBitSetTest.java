package algos;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.function.Supplier;
import static algos.utils.TestUtils.*;

/**
 * Created by volodymyr.bakhmatiuk on 3/28/17.
 * This test is performed on small datasets which feet in RAM size
 */
@RunWith(JUnit4ClassRunner.class)
public class JaccardSimilarityHLLvsBitSetTest extends JaccardSimilarityTestBase {


    static final Supplier<String> UUID_SUPPLIER = () -> UUID.randomUUID().toString();
    static final Supplier<String> DOUBLE_UUID_SUPPLIER = () -> UUID.randomUUID().toString() + UUID.randomUUID().toString();

    @Test
    public void test() {
    }

    /**
     * DUPLICATES_0X argument means how much duplicates (in percentage perspective) input will have
     */
    private final float DUPLICATES_00 = 0.0F;
    private final float DUPLICATES_01 = 0.1F;
    private final float DUPLICATES_05 = 0.5F;
    private final float DUPLICATES_09 = 0.9F;

    /**
     * SIMILARITY_0X argument means which percentage of input2 will be copied from input1
     */
    private final float SIMILARITY_01 = 0.1F;
    private final float SIMILARITY_03 = 0.3F;
    private final float SIMILARITY_05 = 0.5F;
    private final float SIMILARITY_075 = 0.75F;
    private final float SIMILARITY_099 = 0.99F;

    /*
     *
     * Difference between similarity 0.5F and duplicates 0.5F.
     * input1 of size 10, input2 of size 12.
     *
     *  input1  input2
     *    1      1      // similarity = 0.5F means 50% of elements of input2 (6 elements) are copied from inpu1
     *    2      2
     *    3      3
     *    4      4
     *    5      5
     *    6      6
     *    7      11     // duplicates = 0.5F means each second element of input2 is a duplicate in scope of input2
     *    8      11
     *    9      12
     *    10     12
     *           13
     *           13
     */

    // ============================       ============================//
    // ==============================   ==============================//
    //                                                                //
    //        PARAMETERS THAT DO INFLUENCE HLL and BitSet accuracy    //
    //                                                                //
    // ============================       ============================//
    // ==============================   ==============================//


    // ===============================================
    // ================================================
    // PROOF #1: Similarity between inputs DO influence intersection size & accuracy
    // ================================================
    // ===============================================

    @Test
    public void influenceOfSimilarityBetweenInputs_1() throws CardinalityMergeException {
                                             // similarities   0%      10%     20%     30%    40%      50%      60%    70%    80%    90%    100%
        final float[] MIN_EXPECTED_DEVIATIONS = new float[] { 0.000F, 0.000F, 0.000F, 0.07F, 0.148F,  0.213F, 0.264F, 0.29F, 0.29F, 0.230F, 0.0F };
        final float[] MAX_EXPECTED_DEVIATIONS = new float[] { 0.006F, 0.004F, 0.002F, 0.08F, 0.152F,  0.216F, 0.268F, 0.31F, 0.31F, 0.236F, 0.0F };

        for (int i = 0; i <= 10; i++) {
            float similarity = i / (float) 10;
            testHLLInMemory(similarity, 10_000, 50_000, MIN_EXPECTED_DEVIATIONS[i], MAX_EXPECTED_DEVIATIONS[i], UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
            testBitSetInMemory(similarity, 10_000, 50_000, MIN_EXPECTED_DEVIATIONS[i], MAX_EXPECTED_DEVIATIONS[i], UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        }
    }

    @Test
    public void influenceOfSimilarityBetweenInputs_2() throws CardinalityMergeException {
                                              // similarities   0%      10%     20%    30%    40%     50%    60%     70%     80%    90%   100%
        final float[] MIN_EXPECTED_DEVIATIONS = new float[] { 0.000F, 0.000F, 0.080F, 0.17F, 0.25F,  0.33F, 0.395F, 0.445F, 0.46F, 0.39F, 0.0F };
        final float[] MAX_EXPECTED_DEVIATIONS = new float[] { 0.006F, 0.004F, 0.091F, 0.18F, 0.26F,  0.34F, 0.405F, 0.455F, 0.47F, 0.41F, 0.0F };

        for (int i = 0; i <= 10; i++) {
            float similarity = i / (float) 10;
            testHLLInMemory(similarity, 10_000, 100_000, MIN_EXPECTED_DEVIATIONS[i], MAX_EXPECTED_DEVIATIONS[i], UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
            testBitSetInMemory(similarity, 10_000, 100_000, MIN_EXPECTED_DEVIATIONS[i], MAX_EXPECTED_DEVIATIONS[i], UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        }
    }

    // ===============================================
    // ================================================
    // PROOF #2: Size proportions between inputs (without changing intersection size) DO influence accuracy
    // ================================================
    // ===============================================

    @Test
    public void inputProportionDifferencesDoMatterEvenWithTheSameIntersectionSize() throws CardinalityMergeException {
        // --------------------------------------- proportions 1:1    1:2    1:3    1:4    1:5    1:6
        final float[] MIN_EXPECTED_DEVIATIONS = new float[] { 0.18F, 0.45F, 0.31F, 0.23F, 0.18F, 0.15F };
        final float[] MAX_EXPECTED_DEVIATIONS = new float[] { 0.20F, 0.46F, 0.32F, 0.24F, 0.19F, 0.16F };
        final int FIRST_INPUT_SIZE = 10_000;
        final int MULTIPLIER = 25; // in order to get more obvious deviation
        for (int proportion = 1; proportion < 7; proportion++) {
            /*
               SIMILARITY_099 = 0.99F means that FIRST_INPUT_SIZE * 0.99F elements will be copied from the first array to the second
               SIMILARITY_099 / proportion - means that on each proportion we will copy the same amount of elements from input1 to input2
             */
            System.out.println("Proportion: " + proportion);
            testHLLInMemory(SIMILARITY_099 / proportion, FIRST_INPUT_SIZE, MULTIPLIER * proportion * FIRST_INPUT_SIZE, MIN_EXPECTED_DEVIATIONS[proportion - 1], MAX_EXPECTED_DEVIATIONS[proportion -1], UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
            testBitSetInMemory(SIMILARITY_099 / proportion, FIRST_INPUT_SIZE, MULTIPLIER * proportion * FIRST_INPUT_SIZE, MIN_EXPECTED_DEVIATIONS[proportion - 1], MAX_EXPECTED_DEVIATIONS[proportion -1], UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        }
    }

    // ===============================================
    // ================================================
    // PROOF #3: Different duplicates DO influence accuracy
    // ===============================================
    // Change percentage of duplicates from 10% to 90%. Use similarity 50%.
    // Use different duplicates arguments for 1st and 2nd input
    //
    // Difference between similarity 0.5F and DUPLICATES 0.5F:
    // similarity 0.5F means that half of input2 will be fullfilled with elements from input1. Those 50% of elements from input1 will be all unique in scope of input2
    // duplicates 0.5F means that remained 50% of elements of input2 will contain 50% of duplicates which won't intersect with any element of input1.
    // ================================================

    @Test
    public void deviationDoesDependOnDuplicates_1() throws CardinalityMergeException {
        final int INPUT_1_SIZE = 10_000;
        final int INPUT_2_SIZE = 50_000;

        final float[] MIN_EXPECTED_DEVIATIONS = new float[] { 0.23F, 0.21F, 0.08F, 0.28F };
        final float[] MAX_EXPECTED_DEVIATIONS = new float[] { 0.24F, 0.22F, 0.09F, 0.29F };
        final float[] DUPLICATES = new float[] { DUPLICATES_00, DUPLICATES_01, DUPLICATES_05, DUPLICATES_09 };
        for (int i = 0; i < DUPLICATES.length; i++) {
            final float minDev = MIN_EXPECTED_DEVIATIONS[i];
            final float maxDev = MAX_EXPECTED_DEVIATIONS[i];
            Supplier<String> supplier1 = getDuplicateSupplier(DUPLICATES_01, "s1");
            Supplier<String> supplier2 = getDuplicateSupplier(DUPLICATES[i], "s2");
            System.out.println("Iteartion " + i);
            testHLLInMemory(SIMILARITY_05, INPUT_1_SIZE, INPUT_2_SIZE, minDev, maxDev, supplier1, supplier2);
            testBitSetInMemory(SIMILARITY_05, INPUT_1_SIZE, INPUT_2_SIZE, minDev, maxDev, supplier1, supplier2);
        }
    }

    @Test
    public void deviationDoesDependOnDuplicates_2() throws CardinalityMergeException {
        final int INPUT_1_SIZE = 10_000;
        final int INPUT_2_SIZE = 50_000;

        final float[] MIN_EXPECTED_DEVIATIONS = new float[] {0.33F, 0.31F, 0.21F, 0.16F};
        final float[] MAX_EXPECTED_DEVIATIONS = new float[] {0.34F, 0.32F, 0.22F, 0.17F};
        final float[] DUPLICATES = new float[]{DUPLICATES_00, DUPLICATES_01, DUPLICATES_05, DUPLICATES_09};
        for (int i = 0; i < DUPLICATES.length; i++) {
            final float minDev = MIN_EXPECTED_DEVIATIONS[i];
            final float maxDev = MAX_EXPECTED_DEVIATIONS[i];
            Supplier<String> supplier1 = getDuplicateSupplier(DUPLICATES_05, "s1");
            Supplier<String> supplier2 = getDuplicateSupplier(DUPLICATES[i], "s2");
            System.out.println("Iteartion " + i);
            testHLLInMemory(SIMILARITY_05, INPUT_1_SIZE, INPUT_2_SIZE, minDev, maxDev, supplier1, supplier2);
            testBitSetInMemory(SIMILARITY_05, INPUT_1_SIZE, INPUT_2_SIZE, minDev, maxDev, supplier1, supplier2);
        }
    }

    @Test
    public void deviationDoesDependOnDuplicates_3() throws CardinalityMergeException {
        final int INPUT_1_SIZE = 10_000;
        final int INPUT_2_SIZE = 50_000;

        final float[] MIN_EXPECTED_DEVIATIONS = new float[]{0.46F, 0.45F, 0.42F, 0.21F};
        final float[] MAX_EXPECTED_DEVIATIONS = new float[]{0.47F, 0.46F, 0.43F, 0.22F};
        final float[] DUPLICATES = new float[]{DUPLICATES_00, DUPLICATES_01, DUPLICATES_05, DUPLICATES_09};
        for (int i = 0; i < DUPLICATES.length; i++) {
            final float minDev = MIN_EXPECTED_DEVIATIONS[i];
            final float maxDev = MAX_EXPECTED_DEVIATIONS[i];
            Supplier<String> supplier1 = getDuplicateSupplier(DUPLICATES_09, "s1");
            Supplier<String> supplier2 = getDuplicateSupplier(DUPLICATES[i], "s2");
            System.out.println("Iteartion " + i);
            testHLLInMemory(SIMILARITY_05, INPUT_1_SIZE, INPUT_2_SIZE, minDev, maxDev, supplier1, supplier2);
            testBitSetInMemory(SIMILARITY_05, INPUT_1_SIZE, INPUT_2_SIZE, minDev, maxDev, supplier1, supplier2);
        }
    }

    // ============================       ============================//
    // ==============================   ==============================//
    //                                                                //
    //    PARAMETERS THAT DO NOT INFLUENCE HLL and BitSet accuracy    //
    //                                                                //
    // ============================       ============================//
    // ==============================   ==============================//

    // ===============================================
    // ================================================
    // PROOF #4: Input size do NOT influence accuracy
    // ================================================
    // ===============================================

    @Test
    public void deviationDoesNotDependsOnSize_1() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.089F;
        final float MAX_EXPECTED_DEVIATION = 0.11F;
        // HLL
        testHLLInMemory(SIMILARITY_05, 100, 300, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 10_000, 30_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 1_000_000, 3_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLOnDisk(0.5F, 20_000_000, 60_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // BitSet
        testBitSetInMemory(SIMILARITY_05, 100, 300, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSetInMemory(SIMILARITY_05, 10_000, 30_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSetInMemory(SIMILARITY_05, 1_000_000, 3_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSetOnDisk(0.5F, 20_000_000, 60_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void deviationDoesNotDependsOnSize_2() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.16F;
        final float MAX_EXPECTED_DEVIATION = 0.173F;
        // HLL
        testHLLInMemory(SIMILARITY_05, 100, 400, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 10_000, 40_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 1_000_000, 4_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLOnDisk(SIMILARITY_05, 7_000_000, 28_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // BitSet
        testBitSetInMemory(SIMILARITY_05, 100, 400, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSetInMemory(SIMILARITY_05, 10_000, 40_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSetInMemory(SIMILARITY_05, 1_000_000, 4_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSetOnDisk(SIMILARITY_05, 7_000_000, 28_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }

    @Test
    public void deviationDoesNotDependsOnSize_3() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.27F;
        final float MAX_EXPECTED_DEVIATION = 0.28F;
        // HLL
        testHLLInMemory(SIMILARITY_05, 100, 700, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 10_000, 70_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLInMemory(SIMILARITY_05, 1_000_000, 7_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testHLLOnDisk(0.5F, 2_000_000, 14_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        // BitSet
        testBitSetInMemory(SIMILARITY_05, 100, 700, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSetInMemory(SIMILARITY_05, 10_000, 70_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSetInMemory(SIMILARITY_05, 1_000_000, 7_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSetOnDisk(0.5F, 2_000_000, 14_000_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);

    }


    // ===============================================
    // ================================================
    // PROOF #5: Same duplicates proportion do NOT influeinfluencence accuracy.
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
        testBitSetInMemory(SIMILARITY_01, 10_000, 20_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_00, "s1"), getDuplicateSupplier(DUPLICATES_00, "s2"));
        testBitSetInMemory(SIMILARITY_01, 10_000, 20_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_01, "s1"), getDuplicateSupplier(DUPLICATES_01, "s2"));
        testBitSetInMemory(SIMILARITY_01, 10_000, 20_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_05, "s1"), getDuplicateSupplier(DUPLICATES_05, "s2"));
        testBitSetInMemory(SIMILARITY_01, 10_000, 20_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_09, "s1"), getDuplicateSupplier(DUPLICATES_09, "s2"));
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
        testBitSetInMemory(SIMILARITY_03, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_00, "s1"), getDuplicateSupplier(DUPLICATES_00, "s2"));
        testBitSetInMemory(SIMILARITY_03, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_01, "s1"), getDuplicateSupplier(DUPLICATES_01, "s2"));
        testBitSetInMemory(SIMILARITY_03, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_05, "s1"), getDuplicateSupplier(DUPLICATES_05, "s2"));
        testBitSetInMemory(SIMILARITY_03, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_09, "s1"), getDuplicateSupplier(DUPLICATES_09, "s2"));
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
        testBitSetInMemory(SIMILARITY_05, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_00, "s1"), getDuplicateSupplier(DUPLICATES_00, "s2"));
        testBitSetInMemory(SIMILARITY_05, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_01, "s1"), getDuplicateSupplier(DUPLICATES_01, "s2"));
        testBitSetInMemory(SIMILARITY_05, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_05, "s1"), getDuplicateSupplier(DUPLICATES_05, "s2"));
        testBitSetInMemory(SIMILARITY_05, 10_000, 50_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, getDuplicateSupplier(DUPLICATES_09, "s1"), getDuplicateSupplier(DUPLICATES_09, "s2"));
    }

    // ===============================================
    // ================================================
    // PROOF #6: Length of the data do NOT influence accuracy
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
        testBitSetInMemory(SIMILARITY_05, 10_000, 40_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
        testBitSetInMemory(SIMILARITY_05, 10_000, 40_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, supplier1, supplier2);
    }

    // -----------------------------------------------------------------------------------------------------------------
    @Ignore
    @Test
    public void try_to_make_dependency_between_deviation_and_input_size_and_similarity() throws CardinalityMergeException {
        final float MIN_EXPECTED_DEVIATION = 0.245F;
        final float MAX_EXPECTED_DEVIATION = 0.254F;
        testHLLInMemory(SIMILARITY_05, 10_000, 60_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);  // intersection = 60_000 * 0.50 = 30_000
        testHLLInMemory(SIMILARITY_075, 10_000, 40_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER); // intersection = 40_000 * 0.75 = 30_000

        testHLLInMemory(SIMILARITY_05, 20_000, 120_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER); // intersection = 120_000 * 0.50 = 60_000
        testHLLInMemory(SIMILARITY_075, 20_000, 80_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER); // intersection = 80_000  * 0.75 = 60_000

        testHLLInMemory(0.365F, 10_000, 120_000, MIN_EXPECTED_DEVIATION, MAX_EXPECTED_DEVIATION, UUID_SUPPLIER, DOUBLE_UUID_SUPPLIER);
    }
}
