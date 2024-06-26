package org.apache.pinot.core.segment.processing.genericrow;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.IndexSorter;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class JsonNodeArrowWriter {

  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final int DEFAULT_BATCH_SIZE_BYTES = 1048576;
  //  private final DataOutputStream _offsetStream;
  private final FileOutputStream _dataStream;
  private final org.apache.arrow.vector.types.pojo.Schema _arrowSchema;
  private final Schema _pinotSchema;
  List<String> _sortColumns;
  private VectorSchemaRoot _vectorSchemaRoot;
  private int _batchRowCount;
  private File _dataFile;
  private int suffixCount = 0;
  private int sortedSuffixCount = 0;
  private String filePrefix = "/Users/aishik/Work/rawData/outfiles/datafiles/outFile";
  private String sortedFilePrefix = "/Users/aishik/Work/rawData/outfiles/sorted/outfile";
  private String fileSuffix = ".arrow";
  private int _currentBufferSize;
  private RootAllocator _allocator;

  public JsonNodeArrowWriter(File dataFile, Schema pinotSchema, List<String> sortColumns)
      throws Exception {
    _dataFile = dataFile;
    _dataStream = new FileOutputStream(dataFile);
    _arrowSchema = getArrowSchemaFromPinotSchema(pinotSchema);
    _pinotSchema = pinotSchema;
    _allocator = new RootAllocator(512 * 1024 * 1024);
    _vectorSchemaRoot = VectorSchemaRoot.create(_arrowSchema, _allocator);
    _batchRowCount = 0;
    _sortColumns = sortColumns;
    _currentBufferSize = 0;
  }

  public static org.apache.arrow.vector.types.pojo.Schema getArrowSchemaFromPinotSchema(Schema pinotSchema) {
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

  private static void inPlaceSortAll(VectorSchemaRoot root, int[] sortIndices) {
    for (FieldVector vector : root.getFieldVectors()) {
      if (vector instanceof IntVector) {
        sortIntVector((IntVector) vector, sortIndices);
      } else if (vector instanceof VarCharVector) {
        sortVarCharVector((VarCharVector) vector, sortIndices);
      } else if (vector instanceof Float4Vector) {
        sortFloat4Vector((Float4Vector) vector, sortIndices);
      } else if (vector instanceof Float8Vector) {
        sortFloat8Vector((Float8Vector) vector, sortIndices);
      } else if (vector instanceof BigIntVector) {
        sortBigIntVector((BigIntVector) vector, sortIndices);
      } else if (vector instanceof VarBinaryVector) {
        sortVarBinaryVector((VarBinaryVector) vector, sortIndices);
      } else {
        throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass().getSimpleName());
      }
    }
  }

  private static void sortVarBinaryVector(VarBinaryVector vector, int[] sortIndices) {
    int length = sortIndices.length;
    byte[][] tempArray = new byte[length][];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.get(sortIndices[i]);
    }

    for (int i = 0; i < length; i++) {
      vector.set(i, tempArray[i]);
    }
  }

  private static void sortIntVector(IntVector vector, int[] sortIndices) {
    int length = sortIndices.length;
    int[] tempArray = new int[length];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.get(sortIndices[i]);
    }

    for (int i = 0; i < length; i++) {
      vector.set(i, tempArray[i]);
    }
  }

  private static void sortVarCharVector(VarCharVector vector, int[] sortIndices) {
    int length = sortIndices.length;
    byte[][] tempArray = new byte[length][];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.get(sortIndices[i]);
    }

    for (int i = 0; i < length; i++) {
      vector.set(i, tempArray[i]);
    }
  }

  private static void sortFloat4Vector(Float4Vector vector, int[] sortIndices) {
    int length = sortIndices.length;
    float[] tempArray = new float[length];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.get(sortIndices[i]);
    }

    for (int i = 0; i < length; i++) {
      vector.set(i, tempArray[i]);
    }
  }

  private static void sortFloat8Vector(Float8Vector vector, int[] sortIndices) {
    int length = sortIndices.length;
    double[] tempArray = new double[length];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.get(sortIndices[i]);
    }

    for (int i = 0; i < length; i++) {
      vector.set(i, tempArray[i]);
    }
  }

  private static void sortBigIntVector(BigIntVector vector, int[] sortIndices) {
    int length = sortIndices.length;
    long[] tempArray = new long[length];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.get(sortIndices[i]);
    }

    for (int i = 0; i < length; i++) {
      vector.set(i, tempArray[i]);
    }
  }

  public void fillVectorsFromJsonNode(VectorSchemaRoot root, JsonNode jsonNode)
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
            _currentBufferSize += fieldVector.getBufferSize();
            break;
          case LONG:
            ((BigIntVector) fieldVector).setSafe(_batchRowCount, jsonNode.get(fieldSpec.getName()).asLong());
            _currentBufferSize += fieldVector.getBufferSize();
            break;
          case FLOAT:
            ((Float4Vector) fieldVector).setSafe(_batchRowCount, (float) jsonNode.get(fieldSpec.getName()).asDouble());
            _currentBufferSize += fieldVector.getBufferSize();
            break;
          case DOUBLE:
            ((Float8Vector) fieldVector).setSafe(_batchRowCount, jsonNode.get(fieldSpec.getName()).asDouble());
            _currentBufferSize += fieldVector.getBufferSize();
            break;
          case STRING:
            bytes = jsonNode.get(fieldSpec.getName()).asText().getBytes();
            ((VarCharVector) fieldVector).setSafe(_batchRowCount, bytes, 0, bytes.length);
            _currentBufferSize += fieldVector.getBufferSize();
            break;
          case BYTES:
            bytes = jsonNode.get(fieldSpec.getName()).binaryValue();
            ((VarBinaryVector) fieldVector).setSafe(_batchRowCount, bytes, 0, bytes.length);
            _currentBufferSize += fieldVector.getBufferSize();
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
            _currentBufferSize += fieldVector.getBufferSize();
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
            _currentBufferSize += fieldVector.getBufferSize();
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
            _currentBufferSize += fieldVector.getBufferSize();
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
            _currentBufferSize += fieldVector.getBufferSize();
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
            _currentBufferSize += fieldVector.getBufferSize();
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
            _currentBufferSize += fieldVector.getBufferSize();
            break;
          default:
            throw new RuntimeException("Unsupported data type: " + fieldSpec.getDataType().getStoredType());
        }
      }
    }
    _batchRowCount++;
  }

  public void write(JsonNode jsonNode)
      throws IOException {
    fillVectorsFromJsonNode(_vectorSchemaRoot, jsonNode);
    _vectorSchemaRoot.setRowCount(_batchRowCount);
//    if (_currentBufferSize >= DEFAULT_BATCH_SIZE_BYTES)
    if (_vectorSchemaRoot.getRowCount() >= 25000) {
      String filePath = filePrefix + (suffixCount++) + fileSuffix;
      File outFile = new File(filePath);
      String sortedColumnFilePath;
      File sortedColumnFile;
      if (_sortColumns != null) {
        sortAllColumns();

        // Dump the sorted columns in separate files for easier access during reading.
        sortedColumnFilePath = sortedFilePrefix + (sortedSuffixCount++) + fileSuffix;
        sortedColumnFile = new File(sortedColumnFilePath);
        List<FieldVector> sortColumnsList = new ArrayList<>();
        List<Field> sortFields = new ArrayList<>();

        for (String sortColumn : _sortColumns) {
          sortColumnsList.add(_vectorSchemaRoot.getVector(sortColumn));
          sortFields.add(
              new Field(sortColumn, _vectorSchemaRoot.getVector(sortColumn).getField().getFieldType(), null));
        }
        try {
          VectorSchemaRoot sortedVectorSchemaRoot = new VectorSchemaRoot(sortFields, sortColumnsList);
          try (FileOutputStream fileOutputStream = new FileOutputStream(sortedColumnFile);
              ArrowFileWriter writer = new ArrowFileWriter(sortedVectorSchemaRoot, null,
                  fileOutputStream.getChannel());) {
            writer.start();
            writer.writeBatch();
            writer.end();
          } catch (Exception e) {
            throw new RuntimeException("Failed to write Arrow file", e);
          }
        } catch (Exception e) {
          throw new RuntimeException("Failed to create sorted VectorSchemaRoot", e);
        }
      }
      try (FileOutputStream fileOutputStream = new FileOutputStream(outFile);
          ArrowFileWriter writer = new ArrowFileWriter(_vectorSchemaRoot, null, fileOutputStream.getChannel());) {
        writer.start();
        writer.writeBatch();
        writer.end();
      } catch (Exception e) {
        throw new RuntimeException("Failed to write Arrow file", e);
      } finally {
//        _vectorSchemaRoot.close();

      }
      _currentBufferSize = 0;
      _batchRowCount = 0;
    }
  }

  private void sortAllColumns() {
    // Get sort indices from sort columns
    VectorValueComparator<VarCharVector> comparator = DefaultVectorComparators.createDefaultComparator(
        (VarCharVector) _vectorSchemaRoot.getVector(_sortColumns.get(0)));
    IndexSorter<VarCharVector> indexSorter = new IndexSorter<>();
    IntVector indices = new IntVector("sort_indices", _allocator);
    indices.setValueCount(_vectorSchemaRoot.getVector(_sortColumns.get(0)).getValueCount());
    indexSorter.sort((VarCharVector) _vectorSchemaRoot.getVector(_sortColumns.get(0)), indices, comparator);

    // declare a new int array
    int[] sortIndices = new int[indices.getValueCount()];

    // Fill this with the elements from indices vector
    for (int i = 0; i < indices.getValueCount(); i++) {
      sortIndices[i] = indices.get(i);
    }

    inPlaceSortAll(_vectorSchemaRoot, sortIndices);
  }

  public org.apache.arrow.vector.types.pojo.Schema getArrowSchema() {
    return _arrowSchema;
  }

  public long writeData(JsonNode jsonNode)
      throws IOException {
    fillVectorsFromJsonNode(_vectorSchemaRoot, jsonNode);
    _vectorSchemaRoot.setRowCount(_batchRowCount);
    FileChannel newChannel = _dataStream.getChannel();
    if (_currentBufferSize >= DEFAULT_BATCH_SIZE_BYTES) {
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
}