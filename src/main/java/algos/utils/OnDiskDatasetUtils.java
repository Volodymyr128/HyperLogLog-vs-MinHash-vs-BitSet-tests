package algos.utils;

import algos.IndexBitSetData;
import algos.SparseBitSet;
import com.carrotsearch.sizeof.RamUsageEstimator;
import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static algos.utils.TestUtils.convertSize;

/**
 * Created by volodymyr.bakhmatiuk on 3/30/17.
 */
public class OnDiskDatasetUtils {

    static final String NEW_LINE_CHARACTER = "\n";

    public static void generateSimilarArrayToFile(String originFileName, String destFileName, int destFileSize, float similarity, Supplier<String> differentSupplier) {
        final long start = System.currentTimeMillis();
        if (similarity > 1.0 || similarity < 0) throw new IllegalArgumentException("Wrong similarity argument");
        //fill array with duplicates
        final int duplicatesAmount = Math.round(destFileSize * similarity);
        int rowsCopiedFromOrigin = copyFileToFile(originFileName, destFileName, 0, duplicatesAmount, false);

        // overblow array with duplicates if it's impossible to take needed amount from origin
        for (int pointer = rowsCopiedFromOrigin; duplicatesAmount > pointer;) {
            pointer += copyFileToFile(originFileName, destFileName, 0, Math.min(duplicatesAmount - pointer, rowsCopiedFromOrigin), true);
        }

        //fill array with unique values. At this point array should have {@code duplicatesAmount} elemets from origin array, other should be non initialized
        persistDataToFile(destFileName, destFileSize - duplicatesAmount, differentSupplier, true);
        System.out.println(
                String.format("It takes %1$d seconds to persist similarity array with %2$d rows to file %3$s", (System.currentTimeMillis() - start) / 1000, destFileSize, destFileName)
        );
    }

    public static int copyFileToFile(String originFile, String destFile, int fromPosition, int chunkSize, boolean append) {
        final long start = System.currentTimeMillis();
        final AtomicInteger counter = new AtomicInteger(0);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(destFile, append))) {
            Files
                    .lines(Paths.get(originFile))
                    .skip(fromPosition)
                    .limit(chunkSize)
                    .forEach(str -> {
                        try {
                            bw.write(str + NEW_LINE_CHARACTER);
                            // because originFile may have less than {@code chunkSize} elements starting from position {@code fromPosition}
                            counter.incrementAndGet();
                        }
                        catch (IOException e) { throw new RuntimeException("Failed to copy dest file from origin file"); }
                    });
            System.out.println(
                    String.format("It takes %1$d seconds to copy %2$d rows from file %3$s to file %4$s", (System.currentTimeMillis() - start) / 1000, counter.get(), originFile, destFile)
            );
            return counter.get();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write file " + destFile + " due " + e.getMessage());
        }
    }

    public static void persistDataToFile(String fileName, int size, Supplier<String> supplier, boolean append) {
        final long start = System.currentTimeMillis();
        int counter = size;
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, append))) {
            while (counter-- > 0) {
                bw.write(supplier.get() + NEW_LINE_CHARACTER);
            }
            System.out.println(
                    String.format("It takes %1$d seconds to append %2$d rows to file %3$s", (System.currentTimeMillis() - start) / 1000, size, fileName)
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to write file " + fileName + " due " + e.getMessage());
        }
    }

    public static String[] readBatchFromFile(String fileName, int fromPos, int batchSize) {
        final long start = System.currentTimeMillis();
        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            String[] array = stream.skip(fromPos).limit(batchSize).toArray(String[]::new);
            System.out.println(
                    String.format("It takes %1$d seconds to read %2$d rows from file %3$s", (System.currentTimeMillis() - start) / 1000, batchSize, fileName)
            );
            return array;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read from file " + fileName + " due " + e.getMessage());
        }
    }

    public static long performFuncOnFileBatchByBatch(String fileName, Consumer<String[]> consumer, int batchSize) {
        int counter = 0;
        long spentSeconds = 0;
        while (true) {
            int fromPos = counter * batchSize;
            String[] array = readBatchFromFile(fileName, fromPos, batchSize);
            if (array.length == 0) break;
            long start = System.currentTimeMillis();
            consumer.accept(array);
            spentSeconds += System.currentTimeMillis() - start;
            counter++;
        }
        return spentSeconds / 1000;
    }

    public static HyperLogLogPlus makeHLLFromFile(String inputFileName) {
        HyperLogLogPlus hll = new HyperLogLogPlus(16);
        long spent = performFuncOnFileBatchByBatch(
                inputFileName,
                (String[] array) -> Stream.of(array).forEach(str -> hll.offerHashed(MurmurHash.hash64(str.getBytes(), str.getBytes().length))),
                1_000_000
        );
        System.out.println("Spent " + spent + " seconds to execute HLL");
        System.out.println("Result takes " + convertSize(RamUsageEstimator.sizeOf(hll)));
        return hll;
    }

    public static SparseBitSet makeBitSetFromFile(String inputFileName) {
        IndexBitSetData bitSetData = new IndexBitSetData();
        long spent = performFuncOnFileBatchByBatch(
                inputFileName,
                (String[] array) -> Stream.of(array).forEach(bitSetData::setVal),
                1_000_000
        );
        System.out.println("Spent " + spent + " seconds to execute BitSet");
        System.out.println("Result takes " + convertSize(RamUsageEstimator.sizeOf(bitSetData.getBitSet())));
        return bitSetData.getBitSet();
    }
}
