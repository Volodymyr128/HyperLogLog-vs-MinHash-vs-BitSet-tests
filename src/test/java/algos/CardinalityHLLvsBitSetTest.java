package algos;

import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertTrue;

/**
 * Created by volodymyr.bakhmatiuk on 4/3/17.
 */
@RunWith(JUnit4ClassRunner.class)
public class CardinalityHLLvsBitSetTest extends CardinalityTestBase {

    static final Supplier<String> UUID_SUPPLIER = () -> UUID.randomUUID().toString();

    @Test
    public void testCardinalityEstimations() {
        final int DATASET_SIZE = 10_000_000;
        IntStream hllDeviations = IntStream.range(0, 10).map(i -> testHLLCardinalityOnDisk(DATASET_SIZE, UUID_SUPPLIER));
        IntStream sbsDeviations = IntStream.range(0, 10).map(i -> testBitSetCardinalityOnDisk(DATASET_SIZE, UUID_SUPPLIER));
        /*
           HLL deviations: [78849, 137240, 212, 8144, 26102, 24716, 53939, 13148, 18802, 36725], 39787.7 in average
         */
        System.out.println("HLL deviations:    " + Arrays.toString(hllDeviations.toArray()));
        /*
           BitSet deviations [23164, 23266, 23350, 23411, 23329, 23283, 23372, 23100, 23139, 23300], 23271.4 in average
         */
        System.out.println("BitSet deviations: " + Arrays.toString(sbsDeviations.toArray()));
        assertTrue(hllDeviations.max().getAsInt() > sbsDeviations.max().getAsInt());
        assertTrue(hllDeviations.sum() > sbsDeviations.sum());
    }
}
