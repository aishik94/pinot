package org.apache.pinot.core.segment.processing.genericrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.arrow.algorithm.sort.FixedWidthInPlaceVectorSorter;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.algorithm.sort.FixedWidthOutOfPlaceVectorSorter;
//import org.apache.arrow.algorithm.sort.VarBinaryOutOfPlaceVectorSorter;

public class JsonNodeArrowWriter implements Closeable, FileWriter<JsonNode> {

  private static final int DEFAULT_BATCH_SIZE = 1024;
//  private final DataOutputStream _offsetStream;
  private final FileOutputStream _dataStream;
  private final org.apache.arrow.vector.types.pojo.Schema _arrowSchema;
  private final Schema _pinotSchema;
  private VectorSchemaRoot _vectorSchemaRoot;
  private int _batchRowCount;
  List<String> _sortColumns;

  public JsonNodeArrowWriter(File offsetFile, File dataFile, Schema pinotSchema, List<String> sortColumns)
      throws Exception {
//    _offsetStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(offsetFile)));
    _dataStream = new FileOutputStream(dataFile);
    _arrowSchema = getArrowSchemaFromPinotSchema(pinotSchema);
    _pinotSchema = pinotSchema;
    _vectorSchemaRoot = VectorSchemaRoot.create(_arrowSchema, new RootAllocator(512 * 1024 * 1024));
    _batchRowCount = 0;
    _sortColumns = sortColumns;
  }

 // add main


  private org.apache.arrow.vector.types.pojo.Schema getArrowSchemaFromPinotSchema(Schema pinotSchema) {
    List<org.apache.arrow.vector.types.pojo.Field> arrowFields = new ArrayList<>();

    for (FieldSpec fieldSpec : pinotSchema.getAllFieldSpecs()) {
      FieldSpec.DataType storedType = fieldSpec.getDataType().getStoredType();
      FieldType fieldType;
      org.apache.arrow.vector.types.pojo.Field arrowField;
      if (fieldSpec.isSingleValueField()) {
        switch (storedType) {
          case INT:
            fieldType = FieldType.nullable(new ArrowType.Int(32, true));
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case LONG:
            fieldType = FieldType.nullable(new ArrowType.Int(64, true));
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case FLOAT:
            fieldType = FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case DOUBLE:
            fieldType = FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case STRING:
            fieldType = FieldType.nullable(new ArrowType.Utf8());
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case BYTES:
            fieldType = FieldType.nullable(new ArrowType.Binary());
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + storedType);
        }
      } else {
        switch (storedType) {
          case INT:
            fieldType = new FieldType(true, new ArrowType.List.Int(32, true), null);
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case LONG:
            fieldType = new FieldType(true, new ArrowType.List.Int(64, true), null);
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case FLOAT:
            fieldType = new FieldType(true, new ArrowType.List.FloatingPoint(FloatingPointPrecision.SINGLE), null);
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case DOUBLE:
            fieldType = new FieldType(true, new ArrowType.List.FloatingPoint(FloatingPointPrecision.DOUBLE), null);
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          case STRING:
            fieldType = new FieldType(true, new ArrowType.List.Utf8(), null);
            arrowField = new org.apache.arrow.vector.types.pojo.Field(fieldSpec.getName(), fieldType, null);
            arrowFields.add(arrowField);
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + storedType);
        }
      }
    }
    return new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
  }


  private void fillVectorsFromJsonNode(VectorSchemaRoot root, JsonNode jsonNode)
      throws IOException {
    List<FieldVector> fieldVectors = root.getFieldVectors();
    int count = 0;
    for (FieldSpec fieldSpec : _pinotSchema.getAllFieldSpecs()) {
      FieldVector fieldVector = fieldVectors.get(count++);
      byte[] bytes;
      if (fieldSpec.isSingleValueField()) {
        switch (fieldSpec.getDataType().getStoredType()) {
          case INT:
            ((IntVector) fieldVector).setSafe(_batchRowCount, jsonNode.get(fieldSpec.getName()).asInt());
            break;
          case LONG:
            ((BigIntVector) fieldVector).setSafe(_batchRowCount, jsonNode.get(fieldSpec.getName()).asLong());
            break;
          case FLOAT:
            ((Float4Vector) fieldVector).setSafe(_batchRowCount, (float) jsonNode.get(fieldSpec.getName()).asDouble());
            break;
          case DOUBLE:
            ((Float8Vector) fieldVector).setSafe(_batchRowCount, jsonNode.get(fieldSpec.getName()).asDouble());
            break;
          case STRING:
            bytes = jsonNode.get(fieldSpec.getName()).asText().getBytes();
            ((VarCharVector) fieldVector).setSafe(_batchRowCount, bytes, 0, bytes.length);
            break;
          case BYTES:
            bytes = jsonNode.get(fieldSpec.getName()).binaryValue();
            ((VarBinaryVector) fieldVector).setSafe(_batchRowCount, bytes, 0, bytes.length);
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + fieldSpec.getDataType().getStoredType());
        }
      } else {
        JsonNode valuesNode = jsonNode.get(fieldSpec.getName());
        int numValues = valuesNode.size();
        switch (fieldSpec.getDataType().getStoredType()) {
          case INT:
            UnionListWriter listWriter = ((ListVector) fieldVector).getWriter();
            listWriter.setPosition(_batchRowCount);
            listWriter.startList();
            for (JsonNode value : valuesNode) {
              listWriter.writeInt(value.asInt());
            }
            listWriter.setValueCount(numValues);
            listWriter.endList();
            break;
          case LONG:
            UnionListWriter listWriterLong = ((ListVector) fieldVector).getWriter();
            listWriterLong.setPosition(_batchRowCount);
            listWriterLong.startList();
            for (JsonNode value : valuesNode) {
              listWriterLong.writeBigInt(value.asLong());
            }
            listWriterLong.setValueCount(numValues);
            listWriterLong.endList();
            break;
          case FLOAT:
            UnionListWriter listWriterFloat = ((ListVector) fieldVector).getWriter();
            listWriterFloat.setPosition(_batchRowCount);
            listWriterFloat.startList();
            for (JsonNode value : valuesNode) {
              listWriterFloat.writeFloat4((float) value.asDouble());
            }
            listWriterFloat.setValueCount(numValues);
            listWriterFloat.endList();
            break;
          case DOUBLE:
            UnionListWriter listWriterDouble = ((ListVector) fieldVector).getWriter();
            listWriterDouble.setPosition(_batchRowCount);
            listWriterDouble.startList();
            for (JsonNode value : valuesNode) {
              listWriterDouble.writeFloat8(value.asDouble());
            }
            listWriterDouble.setValueCount(numValues);
            listWriterDouble.endList();
            break;
          case STRING:
            UnionListWriter listWriterString = ((ListVector) fieldVector).getWriter();
            listWriterString.setPosition(_batchRowCount);
            listWriterString.startList();
            for (JsonNode value : valuesNode) {
              bytes = value.asText().getBytes();
              BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
              ArrowBuf arrowBuf = allocator.buffer(bytes.length);
              arrowBuf.writeBytes(bytes);
              listWriterString.writeVarChar(0, bytes.length, arrowBuf);
            }
            listWriterString.setValueCount(numValues);
            listWriterString.endList();
            break;
          case BYTES:
            UnionListWriter listWriterBytes = ((ListVector) fieldVector).getWriter();
            listWriterBytes.setPosition(_batchRowCount);
            listWriterBytes.startList();
            for (JsonNode value : valuesNode) {
              bytes = value.binaryValue();
              BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
              ArrowBuf arrowBuf = allocator.buffer(bytes.length);
              arrowBuf.writeBytes(bytes);
              listWriterBytes.writeVarBinary(0, bytes.length, arrowBuf);
            }
            listWriterBytes.setValueCount(numValues);
            listWriterBytes.endList();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + fieldSpec.getDataType().getStoredType());
        }
      }
      _batchRowCount++;
    }
  }

  public void write(JsonNode jsonNode)
      throws IOException {
    fillVectorsFromJsonNode(_vectorSchemaRoot, jsonNode);
    _vectorSchemaRoot.setRowCount(_batchRowCount);
    if (_batchRowCount >= DEFAULT_BATCH_SIZE) {
      sortInPlace();
      try (ArrowFileWriter writer = new ArrowFileWriter(_vectorSchemaRoot, null, _dataStream.getChannel())) {
        writer.start();
        writer.writeBatch();
        writer.end();
      } catch (Exception e) {
        throw new RuntimeException("Failed to write Arrow file", e);
      }
      _batchRowCount = 0;
    }
  }

  public long writeData(JsonNode jsonNode)
      throws IOException {
    fillVectorsFromJsonNode(_vectorSchemaRoot, jsonNode);
    _vectorSchemaRoot.setRowCount(_batchRowCount);
    FileChannel newChannel = _dataStream.getChannel();
    if (_batchRowCount >= DEFAULT_BATCH_SIZE) {
      sortInPlace();
      try (ArrowFileWriter writer = new ArrowFileWriter(_vectorSchemaRoot, null, newChannel);) {
        writer.start();
        writer.writeBatch();
        writer.end();
        _dataStream.close();
        return writer.bytesWritten();
      } catch (Exception e) {
        throw new RuntimeException("Failed to write Arrow file", e);
      }
    }
    return 0;
  }

  void sortInPlace() {
    try (FieldVector vector = _vectorSchemaRoot.getVector(_sortColumns.get(0))) {

      FixedWidthInPlaceVectorSorter sorter = new FixedWidthInPlaceVectorSorter();
      VectorValueComparator<FieldVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vector);

      sorter.sortInPlace((BaseFixedWidthVector) vector, comparator);
    }
  }


  @Override
  public void close() {
    try {
//      _offsetStream.close();
      _dataStream.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to close Arrow writer", e);
    }
  }
}