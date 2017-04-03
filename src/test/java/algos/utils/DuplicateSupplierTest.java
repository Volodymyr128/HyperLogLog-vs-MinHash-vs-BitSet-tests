package algos.utils;

import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;
import algos.utils.DuplicateSupplier;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static algos.utils.DuplicateSupplier.BATCH_SIZE;
import static algos.utils.TestUtils.getDuplicateSupplier;

/**
 * Created by volodymyr.bakhmatiuk on 3/31/17.
 */
@RunWith(JUnit4ClassRunner.class)
public class DuplicateSupplierTest {

    @Test
    public void test() {
        List<Integer> input = IntStream.range(0, 100).boxed().collect(toList());
        Supplier<Integer> supplier = () -> input.remove(0);
        Supplier<Integer> duplicateSupplier = new DuplicateSupplier<>(supplier, 0.5F);

        assertEquals(
                IntStream.range(0, 50).boxed().map(i -> duplicateSupplier.get()).collect(toList()),
                IntStream.range(0, 50).boxed().collect(toList())
        );

        assertTrue(
                IntStream.range(0, 50).boxed().map(i -> duplicateSupplier.get()).allMatch(i -> i >= 0 && i < 50)
        );

        assertEquals(
                IntStream.range(0, 50).boxed().map(i -> duplicateSupplier.get()).collect(toList()),
                IntStream.range(50, 100).boxed().collect(toList())
        );

        assertTrue(
                IntStream.range(0, 50).boxed().map(i -> duplicateSupplier.get()).allMatch(i -> i >= 50 && i < 100)
        );
    }

    @Test
    public void test2() {
        test_cardinality(1_000, 0.1F);
        test_cardinality(1_000, 0.25F);
        test_cardinality(1_000, 0.5F);
        test_cardinality(1_000, 0.6F);
        test_cardinality(1_000, 0.75F);
    }

    private void test_cardinality(int size, float duplicatesPercentage) {
        DuplicateSupplier<String> supplier = (DuplicateSupplier<String>) getDuplicateSupplier(duplicatesPercentage, "S");
        int expectedCardinality = supplier.getUniqueElementsPerBatch() * size / BATCH_SIZE + Math.min(size % BATCH_SIZE, supplier.getUniqueElementsPerBatch());
        assertEquals(
                expectedCardinality,
                IntStream.range(0, size).boxed().map(i -> supplier.get()).distinct().count()
        );
    }

}
