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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Manager for generic row files.
 */
public class GenericRowFileManager {
  public static final String OFFSET_FILE_NAME = "record.offset";
  public static final String DATA_FILE_NAME = "record.data";

  private final File _offsetFile;
  private final File _dataFile;
  private final List<FieldSpec> _fieldSpecs;
  private final boolean _includeNullFields;
  private final int _numSortFields;
  private GenericRowArrowFileWriter _arrowFileWriter;
  File _outputDir;

  private GenericRowFileWriter _fileWriter;
  private MapperOutputReader _fileReader;
  private List<Integer> _chunksRowCount;
  private Schema _arrowSchema;
  private int _totalNumRows;

  public GenericRowFileManager(File outputDir, List<FieldSpec> fieldSpecs, boolean includeNullFields,
      int numSortFields) {
    _offsetFile = new File(outputDir, OFFSET_FILE_NAME);
    _dataFile = new File(outputDir, DATA_FILE_NAME);
    _fieldSpecs = fieldSpecs;
    _includeNullFields = includeNullFields;
    _numSortFields = numSortFields;
    _outputDir = outputDir;
  }

  /**
   * Returns the field specs for the files.
   */
  public List<FieldSpec> getFieldSpecs() {
    return _fieldSpecs;
  }

  /**
   * Returns {@code true} if the file contains null fields, {@code false} otherwise.
   */
  public boolean isIncludeNullFields() {
    return _includeNullFields;
  }

  /**
   * Returns the number of sort fields.
   */
  public int getNumSortFields() {
    return _numSortFields;
  }

  /**
   * Returns the file writer. Creates one if not exists.
   */
  public GenericRowFileWriter getFileWriter()
      throws IOException {
    if (_fileWriter == null) {
      Preconditions.checkState(!_offsetFile.exists(), "Record offset file: %s already exists", _offsetFile);
      Preconditions.checkState(!_dataFile.exists(), "Record data file: %s already exists", _dataFile);
      _fileWriter = new GenericRowFileWriter(_offsetFile, _dataFile, _fieldSpecs, _includeNullFields);
    }
    return _fileWriter;
  }

  /**
   * Returns the file writer. Creates one if not exists.
   */
  public GenericRowArrowFileWriter getFileWriterForTest()
      throws IOException {
    if (_arrowFileWriter == null) {
      _arrowFileWriter = new GenericRowArrowFileWriter(_outputDir, _fieldSpecs, _includeNullFields);
    }
    return _arrowFileWriter;
  }

  /**
   * Closes the file writer.
   */
  public void closeFileWriter()
      throws IOException {
    if (_arrowFileWriter != null) {
      _arrowFileWriter.writeToFile();
      _chunksRowCount = _arrowFileWriter.getChunkRowCounts();
      _arrowSchema = _arrowFileWriter.getArrowSchema();
      _totalNumRows = _arrowFileWriter.getTotalNumRows();
      _arrowFileWriter.close();
      _arrowFileWriter = null;
    }
    if (_fileWriter != null) {
      _fileWriter.close();
      _fileWriter = null;
    }
  }

  List<File> getFileListFromDirectoryPath(String dirPath) {
    List<File> fileList = new ArrayList<>();
    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(dirPath))) {
      for (Path path : directoryStream) {
        fileList.add(path.toFile());
      }
      Collections.sort(fileList, new Comparator<File>() {
        @Override
        public int compare(File f1, File f2) {
          int n1 = extractNumber(f1.getName());
          int n2 = extractNumber(f2.getName());
          return Integer.compare(n1, n2);
        }

        private int extractNumber(String name) {
          String number = name.replaceAll("\\D", "");
          return number.isEmpty() ? 0 : Integer.parseInt(number);
        }
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
    return fileList;
  }

  /**
   * Returns the file reader. Creates one if not exists.
   */
  public MapperOutputReader getFileReader()
      throws IOException {
    if (_fileReader == null) {
      Preconditions.checkState(_offsetFile.exists(), "Record offset file: %s does not exist", _offsetFile);
      Preconditions.checkState(_dataFile.exists(), "Record data file: %s does not exist", _dataFile);
      Map<String, Object> params = new HashMap<>();
      params.put("offsetFile", _offsetFile);
      params.put("dataFile", _dataFile);
      params.put("fieldSpecs", _fieldSpecs);
      params.put("includeNullFields", _includeNullFields);
      params.put("numSortFields", _numSortFields);
      _fileReader = MapperOutputReaderFactory.getMapperOutputReader("GenericRowFileReader", params);
    }
    return _fileReader;
  }

  public MapperOutputReader getFileReaderForTest()
      throws IOException {
    if (_fileReader == null) {
//      if (_arrowFileWriter != null) {
//        _arrowFileWriter.writeToFile();
//      }

      Map<String, Object> params = new HashMap<>();
      params.put("dataFiles", getFileListFromDirectoryPath(_outputDir.toString()));
      params.put("chunkRowCounts", _chunksRowCount);
      params.put("arrowSchema", _arrowSchema);
      params.put("totalNumRows", _totalNumRows);
      params.put("sortColumnFiles", null);
      params.put("includeNullFields", _includeNullFields);
      _fileReader = MapperOutputReaderFactory.getMapperOutputReader("ArrowFileMapperOutputReader", params);
    }
    return _fileReader;
  }

  public MapperOutputReader getFileReaderFactory(String readerName, Map<String, Object> params)
      throws IOException {
    if (_fileReader == null) {
      Preconditions.checkState(_offsetFile.exists(), "Record offset file: %s does not exist", _offsetFile);
      Preconditions.checkState(_dataFile.exists(), "Record data file: %s does not exist", _dataFile);
      return MapperOutputReaderFactory.getMapperOutputReader(readerName, params);
    }
    return _fileReader;
  }

  /**
   * Closes the file reader.
   */
  public void closeFileReader()
      throws IOException {
    if (_fileReader != null) {
      _fileReader.close();
      _fileReader = null;
    }
  }

  /**
   * Cleans up the files.
   */
  public void cleanUp()
      throws IOException {
    closeFileWriter();
    closeFileReader();
    FileUtils.deleteQuietly(_offsetFile);
    FileUtils.deleteQuietly(_dataFile);
  }
}
