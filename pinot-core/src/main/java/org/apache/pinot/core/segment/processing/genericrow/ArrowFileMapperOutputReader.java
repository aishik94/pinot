/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.segment.processing.genericrow;

import it.unimi.dsi.fastutil.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


public class ArrowFileMapperOutputReader implements MapperOutputReader, AutoCloseable {
  public static final int MAX_ROWS_TO_LOAD_PER_BATCH = 10000;
  public static final int ROOT_ALLOCATOR_CAPACITY = 512 * 1024 * 1024;
  boolean _isSortColumnConfigured;
  List<File> _dataFiles;
  List<File> _sortColumnFiles;
  List<org.apache.arrow.vector.ipc.ArrowFileReader> _dataFileReaders;
  List<org.apache.arrow.vector.ipc.ArrowFileReader> _sortColumnFileReaders;
  List<VectorSchemaRoot> _dataVectorSchemaRoots;
  List<VectorSchemaRoot> _sortColumnVectorSchemaRoots;
  RootAllocator _rootAllocator;
  Schema _arrowSchema;
  int _currentRowCount;
  int _chunkCount;
  int _currentChunkRowCount;
  VectorSchemaRoot _vectorSchemaRootForNonSortedCase;
  int _totalNumRows;
  List<Integer> _chunkRowCounts;
  boolean _isChunkLoaded;
  boolean _includeNullFields;

  public ArrowFileMapperOutputReader(List<File> dataFiles, List<File> sortColumnFiles, List<Integer> chunkRowCounts,
      Schema arrowSchema, int totalNumRows) {
    _dataFiles = dataFiles;
    _sortColumnFiles = sortColumnFiles;
    _sortColumnFileReaders = new ArrayList<>();
    _rootAllocator = new RootAllocator(ROOT_ALLOCATOR_CAPACITY);
    _isSortColumnConfigured = sortColumnFiles != null && !sortColumnFiles.isEmpty();
    _sortColumnVectorSchemaRoots = _isSortColumnConfigured ? new ArrayList<>() : null;
    _arrowSchema = arrowSchema;
    _dataVectorSchemaRoots = new ArrayList<>();
    _currentRowCount = 0;
    _currentChunkRowCount = 0;
    _totalNumRows = totalNumRows;
    _chunkCount = 0;
    _chunkRowCounts = chunkRowCounts;
    _vectorSchemaRootForNonSortedCase = VectorSchemaRoot.create(_arrowSchema, _rootAllocator);
    _isChunkLoaded = false;
    initialiseReadersForData();
    System.setProperty("arrow.enable_null_check_for_get", "false");
    _includeNullFields = false;
  }

  public ArrowFileMapperOutputReader(List<File> dataFiles, List<File> sortColumnFiles, List<Integer> chunkRowCounts,
      Schema arrowSchema, int totalNumRows, boolean includeNullFields) {
    _dataFiles = dataFiles;
    _sortColumnFiles = sortColumnFiles;
    _sortColumnFileReaders = new ArrayList<>();
    _rootAllocator = new RootAllocator(ROOT_ALLOCATOR_CAPACITY);
    _isSortColumnConfigured = sortColumnFiles != null && !sortColumnFiles.isEmpty();
    _sortColumnVectorSchemaRoots = _isSortColumnConfigured ? new ArrayList<>() : null;
    _arrowSchema = arrowSchema;
    _dataVectorSchemaRoots = new ArrayList<>();
    _currentRowCount = 0;
    _currentChunkRowCount = 0;
    _totalNumRows = totalNumRows;
    _chunkCount = 0;
    _chunkRowCounts = chunkRowCounts;
    _vectorSchemaRootForNonSortedCase = VectorSchemaRoot.create(_arrowSchema, _rootAllocator);
    _isChunkLoaded = false;
    initialiseReadersForData();
    System.setProperty("arrow.enable_null_check_for_get", "false");
    _includeNullFields = includeNullFields;
  }

  private void initialiseReadersForData() {
    _dataFileReaders = new ArrayList<>();
    for (File file : _dataFiles) {
      Path filePath = file.toPath();
      try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
        // Memory-map the file
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

        // Wrap the MappedByteBuffer in a SeekableByteChannel
        SeekableByteChannel seekableByteChannel = new SeekableByteChannel() {
          private int _position = 0;

          @Override
          public int read(ByteBuffer dst) throws IOException {
            int remaining = mappedByteBuffer.remaining();
            if (remaining == 0) {
              return -1;
            }
            int length = Math.min(dst.remaining(), remaining);
            byte[] data = new byte[length];
            mappedByteBuffer.get(data);
            dst.put(data);
            return length;
          }

          @Override
          public int write(ByteBuffer src) throws IOException {
            throw new UnsupportedOperationException("Read-only channel");
          }

          @Override
          public long position() throws IOException {
            return _position;
          }

          @Override
          public SeekableByteChannel position(long newPosition) throws IOException {
            mappedByteBuffer.position((int) newPosition);
            _position = (int) newPosition;
            return this;
          }

          @Override
          public long size() throws IOException {
            return mappedByteBuffer.capacity();
          }

          @Override
          public SeekableByteChannel truncate(long size) throws IOException {
            throw new UnsupportedOperationException("Read-only channel");
          }

          @Override
          public boolean isOpen() {
            return true;
          }

          @Override
          public void close() throws IOException {
            // No-op
          }
        };

        // Create the ArrowFileReader with the SeekableByteChannel
        ArrowFileReader reader = new ArrowFileReader(seekableByteChannel, _rootAllocator);
        _dataFileReaders.add(reader);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }


  public int getNumRows() {
    return _totalNumRows;
  }

  public int getCurrentRowCount() {
    return _currentRowCount;
  }

  private GenericRow convertToGenericRow(VectorSchemaRoot vectorSchemaRoot, int rowId) {
    GenericRow genericRow = new GenericRow();
    for (int i = 0; i < vectorSchemaRoot.getFieldVectors().size(); i++) {
      FieldVector fieldVector = vectorSchemaRoot.getFieldVectors().get(i);

      if (_includeNullFields && (i == vectorSchemaRoot.getFieldVectors().size() - 1)) {
        ListVector listVector = (ListVector) fieldVector;
        FieldVector dataVector = listVector.getDataVector();
        int startIndex = listVector.getElementStartIndex(rowId);
        int endIndex = listVector.getElementEndIndex(rowId);
        IntVector intVector = (IntVector) dataVector;

        for (int j = startIndex; j < endIndex; j++) {
            genericRow.addNullValueField(vectorSchemaRoot.getFieldVectors().get(intVector.get(j)).getName());
        }
        break;
      }

      if (fieldVector instanceof IntVector) {
        genericRow.putValue(fieldVector.getName(), ((IntVector) fieldVector).get(rowId));
      } else if (fieldVector instanceof BigIntVector) {
        genericRow.putValue(fieldVector.getName(), ((BigIntVector) fieldVector).get(rowId));
      } else if (fieldVector instanceof Float4Vector) {
        genericRow.putValue(fieldVector.getName(), ((Float4Vector) fieldVector).get(rowId));
      } else if (fieldVector instanceof Float8Vector) {
        genericRow.putValue(fieldVector.getName(), ((Float8Vector) fieldVector).get(rowId));
      } else if (fieldVector instanceof VarCharVector) {
        String result = ((VarCharVector) fieldVector).getObject(rowId).toString();
        genericRow.putValue(fieldVector.getName(), result);
      } else if (fieldVector instanceof VarBinaryVector) {
        genericRow.putValue(fieldVector.getName(), ((VarBinaryVector) fieldVector).getObject(rowId));
      } else if (fieldVector instanceof ListVector) {
        ListVector listVector = (ListVector) fieldVector;
        int startIndex = listVector.getElementStartIndex(rowId);
        int endIndex = listVector.getElementEndIndex(rowId);
        FieldVector dataVector = listVector.getDataVector();

        if (dataVector instanceof IntVector) {
          int intVectorWidth = ((IntVector) dataVector).getTypeWidth();

          if (intVectorWidth == 32) {
            int[] result = new int[endIndex - startIndex];
            for (int j = startIndex; j < endIndex; j++) {
              result[j - startIndex] = ((IntVector) dataVector).get(j);
            }
            genericRow.putValue(fieldVector.getName(), result);
          } else if (intVectorWidth == 64) {
            long[] result = new long[endIndex - startIndex];
            for (int j = startIndex; j < endIndex; j++) {
              result[j - startIndex] = ((IntVector) dataVector).get(j);
            }
            genericRow.putValue(fieldVector.getName(), result);
          } else {
            throw new UnsupportedOperationException("Unsupported vector type");
          }
        } else if (dataVector instanceof Float4Vector) {
          float[] result = new float[endIndex - startIndex];
          for (int j = startIndex; j < endIndex; j++) {
            result[j - startIndex] = ((Float4Vector) dataVector).get(j);
          }
          genericRow.putValue(fieldVector.getName(), result);
        } else if (dataVector instanceof Float8Vector) {
          double[] result = new double[endIndex - startIndex];
          for (int j = startIndex; j < endIndex; j++) {
            result[j - startIndex] = ((Float8Vector) dataVector).get(j);
          }
          genericRow.putValue(fieldVector.getName(), result);
        } else if (dataVector instanceof VarCharVector) {
          String[] result = new String[endIndex - startIndex];
          for (int j = startIndex; j < endIndex; j++) {
            result[j - startIndex] = ((VarCharVector) dataVector).getObject(j).toString();
          }
          genericRow.putValue(fieldVector.getName(), result);
        } else {
          throw new UnsupportedOperationException("Unsupported vector type");
        }
      } else {
        throw new UnsupportedOperationException("Unsupported vector type");
      }
    }
//    _currentRowCount++;
    return genericRow;
  }

  public ArrowFileGenericRowRecordReader getRecordReader() {
    return new ArrowFileGenericRowRecordReader(this);
  }

  public void rewind()
      throws IOException {
    _chunkCount = 0;
    _currentRowCount = 0;
    _currentChunkRowCount = 0;
    _vectorSchemaRootForNonSortedCase.clear();
    loadNextBatchInDataVectorSchemaRootForUnsortedData(0);
  }

  private void loadNextBatchInDataVectorSchemaRootForUnsortedData(int chunkId)
      throws IOException {
    // Get the reader for the chunk
    ArrowFileReader reader = _dataFileReaders.get(chunkId);
    ArrowBlock arrowBlock = reader.getRecordBlocks().get(0);
    try {
      reader.loadRecordBatch(arrowBlock);
    } catch (IOException e) {
      e.printStackTrace();
    }
    // Get the vector schema root for the chunk
    _vectorSchemaRootForNonSortedCase = reader.getVectorSchemaRoot();
    _isChunkLoaded = true;
  }

  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  public GenericRow next(GenericRow reuse)
      throws IOException {
    reuse = convertToGenericRow(_vectorSchemaRootForNonSortedCase, _currentChunkRowCount);
    _currentChunkRowCount++;
    _currentRowCount++;
    if ((_currentChunkRowCount == _vectorSchemaRootForNonSortedCase.getRowCount()) && (_chunkCount
        < _dataFiles.size() - 1)) {
      _currentChunkRowCount = 0;
      _vectorSchemaRootForNonSortedCase.close();
      loadNextBatchInDataVectorSchemaRootForUnsortedData(++_chunkCount);
    }
    return reuse;
  }

  public boolean hasNext() {
    return _currentRowCount < _totalNumRows;
  }

  public Pair<Integer, Integer> getChunkIdAndLocalRowIdFromGlobalRowId(int rowId) {
    int chunkId = 0;
    int localRowId = rowId;
    while ((chunkId < _chunkRowCounts.size()) && (localRowId >= _chunkRowCounts.get(chunkId))) {
      localRowId -= _chunkRowCounts.get(chunkId);
      chunkId++;
    }
    return Pair.of(chunkId, localRowId);
  }

  public void read(int rowId, GenericRow buffer) {
    Pair<Integer, Integer> chunkIdAndRowId = getChunkIdAndLocalRowIdFromGlobalRowId(rowId);
    if (_chunkCount != chunkIdAndRowId.left() || !_isChunkLoaded) {
      try {
        loadNextBatchInDataVectorSchemaRootForUnsortedData(chunkIdAndRowId.left());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    buffer.init(convertToGenericRow(_vectorSchemaRootForNonSortedCase, chunkIdAndRowId.right()));
  }

  public int getNumSortFields() {
    return _sortColumnFiles == null ? 0 : _sortColumnFiles.size();
  }

  @Override
  public void close() {
    for (org.apache.arrow.vector.ipc.ArrowFileReader arrowFileReader : _dataFileReaders) {
      try {
        arrowFileReader.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close arrow file reader", e);
      }
    }
    for (org.apache.arrow.vector.ipc.ArrowFileReader arrowFileReader : _sortColumnFileReaders) {
      try {
        arrowFileReader.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close arrow file reader", e);
      }
    }
    _rootAllocator.close();
  }
}
