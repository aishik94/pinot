package org.apache.pinot.core.segment.processing.genericrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
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
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.pinot.plugin.inputformat.json.JSONRecordReader;
import org.apache.pinot.segment.local.utils.SchemaUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.algorithm.sort.FixedWidthOutOfPlaceVectorSorter;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
//import org.apache.arrow.algorithm.sort.VarBinaryOutOfPlaceVectorSorter;

public class JsonNodeArrowWriter{

  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final int DEFAULT_BATCH_SIZE_BYTES = 1048576;
//  private final DataOutputStream _offsetStream;
  private final FileOutputStream _dataStream;
  private final org.apache.arrow.vector.types.pojo.Schema _arrowSchema;
  private final Schema _pinotSchema;
  private VectorSchemaRoot _vectorSchemaRoot;
  private int _batchRowCount;
   private File _dataFile;
  List<String> _sortColumns;
  private int suffixCount = 0;
  private String filePrefix = "/Users/aishik/Work/rawData/outfiles/outFile";
  private String fileSuffix = ".arrow";
  private int _currentBufferSize;

  public JsonNodeArrowWriter(File dataFile, Schema pinotSchema, List<String> sortColumns)
      throws Exception {
    _dataFile = dataFile;
    _dataStream = new FileOutputStream(dataFile);
    _arrowSchema = getArrowSchemaFromPinotSchema(pinotSchema);
    _pinotSchema = pinotSchema;
    _vectorSchemaRoot = VectorSchemaRoot.create(_arrowSchema, new RootAllocator(512 * 1024 * 1024));
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
    if (_vectorSchemaRoot.getRowCount() >= 25000){
      String filePath = filePrefix + (suffixCount++) + fileSuffix;
      File outFile = new File(filePath);
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