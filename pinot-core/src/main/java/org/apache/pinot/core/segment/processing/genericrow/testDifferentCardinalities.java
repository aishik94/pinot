package org.apache.pinot.core.segment.processing.genericrow;

import it.unimi.dsi.fastutil.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;


public class testDifferentCardinalities {

  public static IntVector vectorWithCardinality(Double cardinalityPercentage, int elementCount,
      BufferAllocator allocator) {
    IntVector vector = new IntVector("high_cardinality_int", allocator);
    vector.allocateNew();
    vector.setValueCount(elementCount);

    // declare an int array to store the values
    List<Integer> valueList = new ArrayList<>();

    for (int i = 0; i < elementCount; i++) {
      valueList.add(i, i % (int) Math.round((elementCount * cardinalityPercentage / 100)));
    }
    Collections.shuffle(valueList);

    // add the elements to the vector
    for (int i = 0; i < elementCount; i++) {
      vector.set(i, valueList.get(i));
    }
    return vector;
  }

  public static List<Integer> vectorWithCardinality(Double cardinalityPercentage, int elementCount) {

    // declare an int array to store the values
    List<Integer> valueList = new ArrayList<>();

    for (int i = 0; i < elementCount; i++) {
      valueList.add(i, i % (int) Math.round((elementCount * cardinalityPercentage / 100)));
    }
    Collections.shuffle(valueList);

    return valueList;
  }

  public static void main(String[] args) {
    String jsonlFilePath = "/Users/aishik/Work/rawData/100k-864.json";
    String arrowFilePath = "/Users/aishik/Work/rawData/output_sorted_int.arrow";

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

      DefaultVectorComparators.IntComparator comparator = new DefaultVectorComparators.IntComparator();
      IndexSorter<IntVector> indexSorter = new IndexSorter<>();
      int DEFAULT_VECTOR_LENGTH = 100000;
      // Check time for 100% cardinality vector
      IntVector vector100 = vectorWithCardinality(100.0, 100000, allocator);
      IntVector indices1 = new IntVector("", allocator);
      indices1.setValueCount(DEFAULT_VECTOR_LENGTH);
      long startTime = System.currentTimeMillis();
      indexSorter.sort(vector100, indices1, comparator);
      System.out.println("Time taken to sort array: " + (System.currentTimeMillis() - startTime));

      // Check time for 50% cardinality vector
      IntVector vector50 = vectorWithCardinality(50.0, 100000, allocator);
      IntVector indices2 = new IntVector("", allocator);
      indices2.setValueCount(DEFAULT_VECTOR_LENGTH);
      startTime = System.currentTimeMillis();
      indexSorter.sort(vector50, indices2, comparator);
      System.out.println("Time taken to sort array: " + (System.currentTimeMillis() - startTime));

      // Check time for 10% cardinality vector
      IntVector vector10 = vectorWithCardinality(10.0, 100000, allocator);
      IntVector indices3 = new IntVector("", allocator);
      indices3.setValueCount(DEFAULT_VECTOR_LENGTH);
      startTime = System.currentTimeMillis();
      indexSorter.sort(vector10, indices3, comparator);
      System.out.println("Time taken to sort array: " + (System.currentTimeMillis() - startTime));

      // Check time for 1% cardinality vector
      IntVector vector1 = vectorWithCardinality(0.1, 100000, allocator);
      IntVector indices4 = new IntVector("", allocator);
      indices4.setValueCount(DEFAULT_VECTOR_LENGTH);
      startTime = System.currentTimeMillis();
      indexSorter.sort(vector1, indices4, comparator);
      System.out.println("Time taken to sort array: " + (System.currentTimeMillis() - startTime));

      // Check time for 1% cardinality vector
      List<Integer> v = vectorWithCardinality(0.001, 100000);
      startTime = System.currentTimeMillis();
      Arrays.quickSort(0, 100000, (a, b) -> Integer.compare(v.get(a), v.get(b)), (a, b) -> {
        int temp = v.get(a);
        v.set(a, v.get(b));
        v.set(b, temp);
      });
      System.out.println("Time taken to sort array: " + (System.currentTimeMillis() - startTime));
    }
  }
}