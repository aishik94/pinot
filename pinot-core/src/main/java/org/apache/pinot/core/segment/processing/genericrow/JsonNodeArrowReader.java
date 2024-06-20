package org.apache.pinot.core.segment.processing.genericrow;

import it.unimi.dsi.fastutil.Pair;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
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
import org.apache.arrow.vector.ipc.message.ArrowBlock;


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
    initializeArrowFileReaders();
    _customComparator = getCustomComparator();
    initializeMinHeap();
  }

  public void initializeMinHeap() {
    for (int i = 0; i < _fileList.size(); i++) {
      addToPriorityQueue(i, 0);
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
      ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), _rootAllocator);
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
      try {
        ArrowFileReader reader = _arrowFileReaders.get(o1.left());
        ArrowBlock arrowBlock = reader.getRecordBlocks().get(0);
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot vectorSchemaRoot1 = reader.getVectorSchemaRoot();
        VarCharVector sortColumn = (VarCharVector) vectorSchemaRoot1.getVector(sortColumnName);
        cmp1 = sortColumn.getObject(o1.right()).toString();
      } catch (IOException e) {
        e.printStackTrace();
      }
      try {
        ArrowFileReader reader = _arrowFileReaders.get(o2.left());
        ArrowBlock arrowBlock = reader.getRecordBlocks().get(0);
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot vectorSchemaRoot1 = reader.getVectorSchemaRoot();
        VarCharVector sortColumn = (VarCharVector) vectorSchemaRoot1.getVector(sortColumnName);
        cmp1 = sortColumn.getObject(o2.right()).toString();
      } catch (IOException e) {
        e.printStackTrace();
      }
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
    try (BufferAllocator rootAllocator1 = new RootAllocator();
        FileInputStream fileInputStream = new FileInputStream(filePath);
        ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator1)) {
//      System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
      ArrowBlock arrowBlock = reader.getRecordBlocks().get(0);
      reader.loadRecordBatch(arrowBlock);
      VectorSchemaRoot vectorSchemaRoot1 = reader.getVectorSchemaRoot();
      for (FieldVector fieldVector : vectorSchemaRoot1.getFieldVectors()) {
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
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void readAllRecordsAndDumpToFile() {
    long startTime = System.currentTimeMillis();
    while (_isFirstTime || !_priorityQueue.isEmpty()) {
      addRecordToVectorSchemaRoot();
      _isFirstTime = false;
    }
    long endTime = System.currentTimeMillis();
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
