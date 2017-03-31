import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static utils.InMemoryDatasetUtils.*;

/**
 * Created by volodymyr.bakhmatiuk on 3/30/17.
 */
@RunWith(JUnit4ClassRunner.class)
public class InMemoryDatasetUtilsTest {

    private final static Integer UUID_SIZE = UUID.randomUUID().toString().length();

    @Test
    public void generateArray_test() {
        String[] arr1 = generateArray(String.class, () -> UUID.randomUUID().toString(), 100);
        assertEquals(100, arr1.length);
        assertTrue(Stream.of(arr1).allMatch(str -> str.length() == UUID_SIZE));

        Integer[] arr2 = generateArray(Integer.class, () -> 1, 30);
        assertEquals(30, arr2.length);
        assertTrue(Stream.of(arr2).allMatch(i -> i == 1));
    }

    @Test
    public void generateSimilarArray_test() {
        String[] input1 = generateArray(String.class, () -> UUID.randomUUID().toString(), 100);
        assertTrue(Stream.of(input1).allMatch(str -> str.length() == UUID_SIZE));
        String[] similar1 = generateSimilarArray(String.class, input1, 120, () -> UUID.randomUUID().toString() + UUID.randomUUID().toString(), 0.5F);
        assertEquals(120, similar1.length);
        List<String> inputList1 = Arrays.asList(input1);
        assertEquals(60, Stream.of(similar1).filter(inputList1::contains).count());
        assertTrue(Stream.of(similar1).limit(60).allMatch(str -> str.length() == UUID_SIZE));
        assertTrue(Stream.of(similar1).skip(60).limit(60).allMatch(str -> str.length() == 2 * UUID_SIZE));

        String[] input2 = generateArray(String.class, () -> UUID.randomUUID().toString(), 100);
        assertTrue(Stream.of(input2).allMatch(str -> str.length() == UUID_SIZE));
        String[] similar2 = generateSimilarArray(String.class, input2, 60, () -> UUID.randomUUID().toString() + UUID.randomUUID().toString(), 0.75F);
        assertEquals(60, similar2.length);
        List<String> inputList2 = Arrays.asList(input2);
        assertEquals(45, Stream.of(similar2).filter(inputList2::contains).count());
        assertTrue(Stream.of(similar2).limit(45).allMatch(str -> str.length() == UUID_SIZE));
        assertTrue(Stream.of(similar2).skip(45).limit(15).allMatch(str -> str.length() == 2 * UUID_SIZE));

        String[] input3 = generateArray(String.class, () -> UUID.randomUUID().toString(), 100);
        assertTrue(Stream.of(input3).allMatch(str -> str.length() == UUID_SIZE));
        String[] similar3 = generateSimilarArray(String.class, input3, 10, () -> UUID.randomUUID().toString() + UUID.randomUUID().toString(), 1.0F);
        assertEquals(10, similar3.length);
        List<String> inputList3 = Arrays.asList(input3);
        assertEquals(10, Stream.of(similar3).filter(inputList3::contains).count());
        assertTrue(Stream.of(similar3).limit(10).allMatch(str -> str.length() == UUID_SIZE));
        assertTrue(Stream.of(similar3).noneMatch(str -> str.length() == 2 * UUID_SIZE));

        String[] input4 = generateArray(String.class, () -> UUID.randomUUID().toString(), 100);
        assertTrue(Stream.of(input4).allMatch(str -> str.length() == UUID_SIZE));
        String[] similar4 = generateSimilarArray(String.class, input4, 300, () -> UUID.randomUUID().toString() + UUID.randomUUID().toString(), 0.5F);
        assertEquals(300, similar4.length);
        List<String> inputList4 = Arrays.asList(input4);
        assertEquals(150, Stream.of(similar4).filter(inputList4::contains).count());
        assertTrue(Stream.of(similar4).limit(150).allMatch(str -> str.length() == UUID_SIZE));
        assertTrue(Stream.of(similar4).skip(150).limit(150).allMatch(str -> str.length() == 2 * UUID_SIZE));
    }
}
