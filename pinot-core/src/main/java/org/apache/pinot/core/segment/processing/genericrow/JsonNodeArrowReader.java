package org.apache.pinot.core.segment.processing.genericrow;

import it.unimi.dsi.fastutil.Pair;
import java.io.BufferedInputStream;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import one.profiler.AsyncProfiler;
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
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.commons.io.input.MemoryMappedFileInputStream;


public class JsonNodeArrowReader {

  private final org.apache.arrow.vector.types.pojo.Schema _arrowSchema;
  List<String> _sortColumns;
  boolean _isFirstTime = true;
  PriorityQueue<Pair<Integer, Integer>> _priorityQueue;
  Comparator<Pair<Integer, Integer>> _customComparator;
  List<Integer> _indexList;
  String _sortedDirectoryPath = "/Users/aishik/Work/rawData/outfiles/sorted/";
  String _dataDirectoryPath = "/Users/aishik/Work/rawData/outfiles/datafiles/";
  String _finalOutputPath = "/Users/aishik/Work/rawData/outfiles/finalOutput/";
  String sortColumnName = "high_cardinality_string";
  List<File> _fileList;
  List<File> _dataFileList;
  List<FieldVector> sortVectors = new ArrayList<>();
  File _outputFile;
  VectorSchemaRoot _vectorSchemaRoot;
  RootAllocator _rootAllocator;
  List<ArrowFileReader> _arrowFileReaders;
  List<FileInputStream> _fileInputStreams;
  List<VectorSchemaRoot> _vectorSchemaRoots;
  List<VectorSchemaRoot> _vectorSchemaRootsForData;
  List<ArrowFileReader> _dataReaderList;

  public JsonNodeArrowReader(org.apache.arrow.vector.types.pojo.Schema arrowSchema, List<String> sortColumns)
      throws Exception {

    _arrowSchema = arrowSchema;
    _sortColumns = sortColumns;
    _priorityQueue = new PriorityQueue<>(getCustomComparator());
    _fileList = getFileListFromDirectoryPath(_sortedDirectoryPath);
    _indexList = new ArrayList<>(_fileList.size());
    for (int i = 0; i < _fileList.size(); i++) {
      _indexList.add(0);
    }
    _dataFileList = getFileListFromDirectoryPath(_dataDirectoryPath);
    _outputFile = new File(_finalOutputPath + "finalOutput.arrow");
    _rootAllocator = new RootAllocator(512 * 1024 * 1024);
    _vectorSchemaRoot = VectorSchemaRoot.create(_arrowSchema, _rootAllocator);
    _arrowFileReaders = new ArrayList<>();
    _dataReaderList = new ArrayList<>();
//    initializeArrowFileReaders();
    initialiseNewReaders();
    initialiseNewReadersForData();
//    test();
    initializeVectorSchemaRoots();
    initializeVectorSchemaRootsForData();
    _customComparator = getCustomComparator();
    initializeMinHeap();
  }

  private void initializeVectorSchemaRoots()
      throws IOException {
    _vectorSchemaRoots = new ArrayList<>();
    for (int i = 0; i < _fileList.size(); i++) {
      ArrowFileReader reader = _arrowFileReaders.get(i);
      ArrowBlock arrowBlock = reader.getRecordBlocks().get(0);
      reader.loadRecordBatch(arrowBlock);
      VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
      _vectorSchemaRoots.add(vectorSchemaRoot);
    }
  }

  private void initializeVectorSchemaRootsForData()
      throws IOException {
    _vectorSchemaRootsForData = new ArrayList<>();
    for (int i = 0; i < _dataFileList.size(); i++) {
      ArrowFileReader reader = _dataReaderList.get(i);
      ArrowBlock arrowBlock = reader.getRecordBlocks().get(0);
      reader.loadRecordBatch(arrowBlock);
      VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
      _vectorSchemaRootsForData.add(vectorSchemaRoot);
    }
  }

  public void initializeMinHeap() {
    for (int i = 0; i < _fileList.size(); i++) {
      addToPriorityQueue(i, 0);
    }
  }

  private void test() {
    for (File file : _fileList) {
      try (MemoryMappedFileInputStream memoryMappedFileInputStream = MemoryMappedFileInputStream.builder().setFile(file).get();
          BufferedInputStream bufferedInputStream = new BufferedInputStream(memoryMappedFileInputStream)) {
        // Create Arrow reader from the mapped byte buffer
        ArrowFileReader reader = new ArrowFileReader(
            (SeekableReadChannel) Channels.newChannel(memoryMappedFileInputStream), _rootAllocator);
        _arrowFileReaders.add(reader);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

//  private void fileInputStreamMmap() {
//    for (File fileNew : _fileList) {
//      try (RandomAccessFile file = new RandomAccessFile(fileNew, "r");
//          FileChannel fileChannel = file.getChannel()) {
//        long fileSize = fileChannel.size();
//        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
//        // Create a ReadChannel from the MappedByteBuffer
//        ReadChannel readChannel = new SeekableReadChannel() {
//          @Override
//          public long readFully(ArrowBuf arrowBuf, long length) throws IOException {
//            long bufferSize = arrowBuf.capacity();
//            if (mappedByteBuffer.remaining() >= bufferSize) {
//              mappedByteBuffer.get(arrowBuf.nioBuffer().array());
//              return bufferSize;
//            } else {
//              return -1; // End of file
//            }
//          }
//
//          @Override
//          public void close() throws IOException {
//            // No need to close the MappedByteBuffer
//          }
//        };
//
//        // Create Arrow reader from the mapped byte buffer
//        ArrowFileReader reader = new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(mappedByteBuffer), _rootAllocator);
//        _arrowFileReaders.add(reader);
//      } catch (IOException e) {
//        throw new RuntimeException(e);
//      }
//    }
//
//  }

  private void initialiseNewReaders() {
    for (File file : _fileList) {
      Path filePath = file.toPath();
      try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
        // Memory-map the file
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

        // Wrap the MappedByteBuffer in a SeekableByteChannel
        SeekableByteChannel seekableByteChannel = new SeekableByteChannel() {
          private int position = 0;

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
            return position;
          }

          @Override
          public SeekableByteChannel position(long newPosition) throws IOException {
            mappedByteBuffer.position((int) newPosition);
            position = (int) newPosition;
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
        _arrowFileReaders.add(reader);
    } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void initialiseNewReadersForData() {
    for (File file : _dataFileList) {
      Path filePath = file.toPath();
      try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
        // Memory-map the file
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

        // Wrap the MappedByteBuffer in a SeekableByteChannel
        SeekableByteChannel seekableByteChannel = new SeekableByteChannel() {
          private int position = 0;

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
            return position;
          }

          @Override
          public SeekableByteChannel position(long newPosition) throws IOException {
            mappedByteBuffer.position((int) newPosition);
            position = (int) newPosition;
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
        _dataReaderList.add(reader);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }


  private Pair<ArrowFileReader, VarCharVector> createReaderAndSortColumn(String filePath) {
    try (FileChannel fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {
      // Memory-map the file
      MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

      // Wrap the MappedByteBuffer in a SeekableByteChannel
      SeekableByteChannel seekableByteChannel = new SeekableByteChannel() {
        private int position = 0;

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
          return position;
        }

        @Override
        public SeekableByteChannel position(long newPosition) throws IOException {
          mappedByteBuffer.position((int) newPosition);
          position = (int) newPosition;
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
      ArrowBlock arrowBlock = reader.getRecordBlocks().get(0);
      reader.loadRecordBatch(arrowBlock);
      VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
      VarCharVector sortColumn = (VarCharVector) vectorSchemaRoot.getVector(sortColumnName);
      return Pair.of(reader, sortColumn);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  private void initializeFileInputStreams() {
    _fileInputStreams = new ArrayList<>();
    for (File file : _fileList) {
      try {
        FileInputStream fileInputStream = new FileInputStream(file);
        _fileInputStreams.add(fileInputStream);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void initializeArrowFileReaders() {
    initializeFileInputStreams();
    for (FileInputStream fileInputStream : _fileInputStreams) {
      SeekableReadChannel readChannel = new SeekableReadChannel(fileInputStream.getChannel());
      ArrowFileReader reader = new ArrowFileReader(readChannel, _rootAllocator);
      _arrowFileReaders.add(reader);
    }
  }

  List<File> getFileListFromDirectoryPath(String dirPath) {
    List<File> fileList = new ArrayList<>();
    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(dirPath))) {
      for (Path path : directoryStream) {
        fileList.add(path.toFile());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return fileList;
  }

  Comparator<Pair<Integer, Integer>> getCustomComparator() {
    return (o1, o2) -> {

      // Get chunk ID
      String cmp1 = "";
      String cmp2 = "";
      VarCharVector sortColumn1 = (VarCharVector)  _vectorSchemaRoots.get(o1.left()).getVector(sortColumnName);
      cmp1 = sortColumn1.getObject(o1.right()).toString();
      VarCharVector sortColumn2 = (VarCharVector)  _vectorSchemaRoots.get(o2.left()).getVector(sortColumnName);
      cmp2 = sortColumn2.getObject(o2.right()).toString();
      return cmp1.compareTo(cmp2);
    };
  }

  private void addToPriorityQueue(int chunkId, int index) {
    _priorityQueue.add(Pair.of(chunkId, index));
  }

  private Pair<Integer, Integer> extractMinFromPriorityQueue() {
    Pair<Integer, Integer> element = _priorityQueue.poll();
    if (element.right() < 24999) {
      addToPriorityQueue(element.left(), element.right() + 1);
    }
//    System.out.println("Extracted element: " + element.left() + " " + element.right());
    return element;
  }

  private void addRecordToVectorSchemaRoot() {
    Pair<Integer, Integer> element = extractMinFromPriorityQueue();
    int chunkId = element.left();
    int index = element.right();
    String filePath = _dataFileList.get(chunkId).toString();
    //      System.out.println("Record batches in file: " + reader.getRecordBlocks().size());;
    for (FieldVector fieldVector : _vectorSchemaRootsForData.get(chunkId).getFieldVectors()) {
      FieldVector newFieldVector = _vectorSchemaRoot.getVector(fieldVector.getName());
      newFieldVector.setInitialCapacity(newFieldVector.getValueCount() + 1);
      newFieldVector.setValueCount(newFieldVector.getValueCount() + 1);
      // set the value of the index in the new vector based on vector type
      if (fieldVector instanceof IntVector) {
        ((IntVector) newFieldVector).setSafe(newFieldVector.getValueCount() - 1,
            ((IntVector) fieldVector).get(index));
      } else if (fieldVector instanceof BigIntVector) {
        ((BigIntVector) newFieldVector).setSafe(newFieldVector.getValueCount() - 1,
            ((BigIntVector) fieldVector).get(index));
      } else if (fieldVector instanceof Float4Vector) {
        ((Float4Vector) newFieldVector).setSafe(newFieldVector.getValueCount() - 1,
            ((Float4Vector) fieldVector).get(index));
      } else if (fieldVector instanceof Float8Vector) {
        ((Float8Vector) newFieldVector).setSafe(newFieldVector.getValueCount() - 1,
            ((Float8Vector) fieldVector).get(index));
      } else if (fieldVector instanceof VarCharVector) {
        ((VarCharVector) newFieldVector).setSafe(newFieldVector.getValueCount() - 1,
            ((VarCharVector) fieldVector).getObject(index));
      } else if (fieldVector instanceof VarBinaryVector) {
        ((VarBinaryVector) newFieldVector).setSafe(newFieldVector.getValueCount() - 1,
            ((VarBinaryVector) fieldVector).getObject(index));
      } else {
        throw new UnsupportedOperationException("Unsupported vector type");
      }
    }
    _vectorSchemaRoot.setRowCount(_vectorSchemaRoot.getRowCount() + 1);
  }

  public void readAllRecordsAndDumpToFile()
      throws IOException {
    AsyncProfiler profiler = AsyncProfiler.getInstance();
    String profilerFileName = "ArrowSorterWallUnsafeNew";
    profiler.execute(String.format("start,event=wall,file=%s.html", profilerFileName));
    long startTime = System.currentTimeMillis();
    while (_isFirstTime || !_priorityQueue.isEmpty()) {
      addRecordToVectorSchemaRoot();
      _isFirstTime = false;
    }
    long endTime = System.currentTimeMillis();
    profiler.execute(String.format("stop,file=%s.html", profilerFileName));
    System.out.println("Time taken to read all records: " + (endTime - startTime) + " ms");
    try (FileOutputStream fileOutputStream = new FileOutputStream(_outputFile);
        ArrowFileWriter arrowFileWriter = new ArrowFileWriter(_vectorSchemaRoot, null, fileOutputStream.getChannel())) {
      arrowFileWriter.start();
      arrowFileWriter.writeBatch();
      arrowFileWriter.end();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
