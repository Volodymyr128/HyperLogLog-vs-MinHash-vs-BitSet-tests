package algos.utils;

import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static algos.utils.OnDiskDatasetUtils.*;

/**
 * Created by volodymyr.bakhmatiuk on 3/30/17.
 */
@RunWith(JUnit4ClassRunner.class)
public class OnDiskDatasetUtilsTest {

    private final static Integer UUID_SIZE = UUID.randomUUID().toString().length();
    final static Supplier<String> RANDOM_SUPPLIER = () -> UUID.randomUUID().toString();

    @Test
    public void testReadingFromFile() {
        String fileName = "test-file";
        //write file
        persistDataToFile(fileName, 100, RANDOM_SUPPLIER, false);
        //read file
        String[] result = readBatchFromFile(fileName, 0, 100);
        //make assertions
        assertEquals(100, result.length);
        assertTrue(Stream.of(result).allMatch(str -> str.length() == UUID_SIZE));
    }

    @Test
    public void testReadingWithBiggerBatchSize() {
        String fileName = "test-file";
        //write file
        persistDataToFile(fileName, 100, RANDOM_SUPPLIER, false);
        //read file
        String[] result = readBatchFromFile(fileName, 0, 200);
        //make assertions
        assertEquals(100, result.length);
        assertTrue(Stream.of(result).allMatch(str -> str.length() == UUID_SIZE));
    }

    @Test
    public void testReadingWithSmallerBatchSize() {
        String fileName = "test-file";
        //write file
        List<String> input = IntStream.range(0, 100).boxed().map(Object::toString).collect(Collectors.toList());
        Supplier<String> supplier = () -> input.remove(0);
        persistDataToFile(fileName, 100, supplier, false);
        //read file
        String[] result = readBatchFromFile(fileName, 10, 20);
        String[] expectedResult = IntStream.range(10, 30).boxed().map(Object::toString).toArray(String[]::new);
        //make assertions
        assertEquals(expectedResult.length, result.length);
        assertTrue(Arrays.equals(expectedResult, result));
    }

    @Test
    public void testReadingBatchByBatch() {
        String fileName = "test-file";
        //write file
        List<String> input = IntStream.range(0, 100).boxed().map(Object::toString).collect(Collectors.toList());
        Supplier<String> supplier = () -> input.remove(0);
        persistDataToFile(fileName, 100, supplier, false);
        //read file
        final AtomicInteger counter = new AtomicInteger();
        final int batchSize = 10;
        Consumer<String[]> consumer = array -> {
            String[] expected = IntStream.range(counter.get(), counter.get() + batchSize).boxed().map(Object::toString).toArray(String[]::new);
            assertTrue(Arrays.equals(expected, array));
            counter.addAndGet(batchSize);
        };
        performFuncOnFileBatchByBatch(fileName, consumer, batchSize);
    }

    @Test
    public void test_copyFileToFile() {
        String origin = "test-file-1";
        String dest = "test-file-2";
        //write files
        List<String> input = IntStream.range(0, 100).boxed().map(Object::toString).collect(Collectors.toList());
        Supplier<String> supplier = () -> input.remove(0);
        persistDataToFile(origin, 100, supplier, false);
        copyFileToFile(origin, dest, 10, 80, false);
        //read file
        String[] result = readBatchFromFile(dest, 0, 70);
        String[] expectedResult = IntStream.range(10, 80).boxed().map(Object::toString).toArray(String[]::new);
        //make assertions
        assertEquals(expectedResult.length, result.length);
        assertTrue(Arrays.equals(expectedResult, result));
    }

    @Test
    public void test_generateSimilarArrayToFile() {
        //write origin file
        List<String> input1 = IntStream.range(0, 100).boxed().map(Object::toString).collect(Collectors.toList());
        Supplier<String> supplier1 = () -> input1.remove(0);
        persistDataToFile("test-file-1", 100, supplier1, false);

        //write similar file
        List<String> input2 = IntStream.range(100, 250).boxed().map(Object::toString).collect(Collectors.toList());
        Supplier<String> supplier2 = () -> input2.remove(0);
        generateSimilarArrayToFile("test-file-1", "test-file-2", 300, 0.5F, supplier2);

        //make assertions
        String[] result = readBatchFromFile("test-file-2", 0, 300);
        String[] expectedResult = Stream
                .of(IntStream.range(0, 100), IntStream.range(0, 50), IntStream.range(100, 250))
                .flatMap(IntStream::boxed)
                .map(Object::toString)
                .toArray(String[]::new);
        assertTrue(Arrays.equals(expectedResult, result));
    }
}
