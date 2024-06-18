//package org.apache.pinot.core.segment.processing.genericrow;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import java.io.BufferedReader;
//import java.io.FileOutputStream;
//import java.io.FileReader;
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Scanner;
//import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
//import org.apache.arrow.algorithm.sort.IndexSorter;
//import org.apache.arrow.algorithm.sort.VectorValueComparator;
//import org.apache.arrow.memory.BufferAllocator;
//import org.apache.arrow.memory.RootAllocator;
//import org.apache.arrow.vector.IntVector;
//import org.apache.arrow.vector.VectorSchemaRoot;
//import org.apache.arrow.vector.VectorUnloader;
//import org.apache.arrow.vector.ipc.ArrowFileWriter;
//import org.apache.arrow.vector.types.pojo.ArrowType;
//import org.apache.arrow.vector.types.pojo.Field;
//import org.apache.arrow.vector.types.pojo.FieldType;
//import org.apache.arrow.vector.types.pojo.Schema;
//
//public class testPriorityQueueMergeSort {
//
//
//
//
//  public static void main(String[] args) {
//    Scanner sc = new Scanner(System.in);
//
//    sc.
//    String jsonlFilePath = "/Users/aishik/Work/rawData/100k-864.json";
//    String arrowFilePath = "/Users/aishik/Work/rawData/output_sorted_int_test.arrow";
//
//    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
//
//      // Create schema
//      List<Field> fields = new ArrayList<>();
//      fields.add(new Field("high_cardinality_int", FieldType.nullable(new ArrowType.Int(32, true)), null)); // Example field
//
//      // Add more fields according to your JSON structure
//      Schema schema = new Schema(fields);
//
//      // Create VectorSchemaRoot
//      VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
//
//      // Read JSONL file
//      ObjectMapper mapper = new ObjectMapper();
//      int _batchRowCount = 0;
//      try (BufferedReader reader = new BufferedReader(new FileReader(jsonlFilePath))) {
//        String line;
//        while ((line = reader.readLine()) != null) {
//          JsonNode jsonNode = mapper.readTree(line);
//
//          // Populate the VectorSchemaRoot
//          int value = jsonNode.get("high_cardinality_int").asInt();
//          ((IntVector) root.getVector("high_cardinality_int")).setSafe(_batchRowCount, value);
//
//          // Populate other fields
//          root.setRowCount(root.getRowCount() + 1);
//          _batchRowCount++;
//          System.out.printf("Processed %d rows\n", _batchRowCount);
//        }
//      }
//
//
//      System.out.println("rowCount: " + root.getVector("high_cardinality_int").getValueCount());
//
//      IndexSorter<IntVector> indexSorter = new IndexSorter<>();
//      IntVector vector = (IntVector) root.getVector("high_cardinality_int");
//
//      DefaultVectorComparators.IntComparator comparator = new DefaultVectorComparators.IntComparator();
//
//      IntVector indices = new IntVector("", allocator);
//      indices.setValueCount(vector.getValueCount());
//      long startTime = System.currentTimeMillis();
//      indexSorter.sort((IntVector) root.getVector("high_cardinality_int"), indices, comparator);
//      System.out.println("Time taken to sort array: " + (System.currentTimeMillis() - startTime));
//
//      // Create a new sorted vector
//      IntVector sortedVector = new IntVector("high_cardinality_int_sorted", allocator);
//      sortedVector.allocateNew(vector.getValueCount());
//
//      // Apply the sorted indices to the original vector and populate the new sorted vector
//      for (int i = 0; i < vector.getValueCount(); i++) {
//        sortedVector.setSafe(i, vector.get(indices.get(i)));
//      }
//      sortedVector.setValueCount(vector.getValueCount());
//
//      // replace the original vector with the sorted vector
////      root.removeVector(0);
//      root = root.addVector(0,sortedVector);
//
//      // Write to Arrow file
//      try (FileOutputStream fileOutputStream = new FileOutputStream(arrowFilePath);
//          ArrowFileWriter writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel())) {
//        writer.start();
//        writer.writeBatch();
//        writer.end();
//        System.out.printf("Arrow file written to %s\n", arrowFilePath);
//      }
//
//    } catch (IOException e) {
//      e.printStackTrace();
//    } finally {
//
//    }
//  }
//}