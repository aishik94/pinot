package org.apache.pinot.core.segment.processing.genericrow;

import java.io.FileInputStream;
import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;

public class ArrowFileReaderUtil {

  public static void main(String[] args) {
    String arrowFilePath = "/Users/aishik/Work/rawData/output_sorted_int.arrow";

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FileInputStream fileInputStream = new FileInputStream(arrowFilePath);
        ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator)) {

      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      reader.loadNextBatch();
      int rowCount = root.getRowCount();

      for (int i = 0; i < rowCount; i++) {
//        Text lowCardinalityString = ((VarCharVector) root.getVector("low_cardinality_string")).getObject(i);
//        System.out.println("low_cardinality_string: " + (lowCardinalityString != null ? lowCardinalityString.toString() : "null"));
        int highCardinalityInt = ((IntVector) root.getVector("high_cardinality_int")).get(i);
        System.out.println("high_cardinality_int: " + highCardinalityInt);
        // Add more fields as needed
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}