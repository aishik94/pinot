package org.apache.pinot.core.segment.processing.genericrow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;


public class JsonlToArrow {
  public static void main(String[] args) {
    String jsonlFilePath = "/Users/aishik/Work/rawData/100k-864.json";
    String arrowFilePath = "/Users/aishik/Work/rawData/output.arrow";

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      // Create schema
      List<Field> fields = new ArrayList<>();
      fields.add(new Field("low_cardinality_string", FieldType.nullable(new ArrowType.Utf8()), null)); // Example field
      // Add more fields according to your JSON structure

      Schema schema = new Schema(fields);

      // Create VectorSchemaRoot
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

      // Read JSONL file
      ObjectMapper mapper = new ObjectMapper();
      int _batchRowCount = 0;
      try (BufferedReader reader = new BufferedReader(new FileReader(jsonlFilePath))) {
        String line;
        while ((line = reader.readLine()) != null) {
          JsonNode jsonNode = mapper.readTree(line);
          // Populate the VectorSchemaRoot
          byte[] bytes = jsonNode.get("low_cardinality_string").asText().getBytes();
          ((VarCharVector) root.getVector("low_cardinality_string")).setSafe(_batchRowCount, bytes, 0, bytes.length);

          // Populate other fields

          root.setRowCount(root.getRowCount() + 1);
          _batchRowCount++;
          System.out.printf("Processed %d rows\n", _batchRowCount);
        }
      }

      // Write to Arrow file
      try (FileOutputStream fileOutputStream = new FileOutputStream(arrowFilePath);
          ArrowFileWriter writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel())) {
        writer.start();
        writer.writeBatch();
        writer.end();
        System.out.printf("Arrow file written to %s\n", arrowFilePath);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {

    }
  }
}
