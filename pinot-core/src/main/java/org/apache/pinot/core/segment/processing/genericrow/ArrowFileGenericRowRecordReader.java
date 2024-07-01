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

import java.io.File;
import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


public class ArrowFileGenericRowRecordReader implements RecordReader, GenericRowMapperOutputRecordReader {
  ArrowFileGenericRowReader _arrowFileReader;
  int _startRowId;
  int _endRowId;

  public ArrowFileGenericRowRecordReader(ArrowFileGenericRowReader arrowFileReader) {
    _arrowFileReader = arrowFileReader;
    _startRowId = 0;
    _endRowId = arrowFileReader.getNumRows();
  }

  public ArrowFileGenericRowRecordReader(ArrowFileGenericRowReader arrowFileReader, int startRowId, int endRowId) {
    _startRowId = startRowId;
    _endRowId = endRowId;
    _arrowFileReader = arrowFileReader;
  }

  public ArrowFileGenericRowRecordReader getRecordReaderForRange(int startRowId, int endRowId) {
    return new ArrowFileGenericRowRecordReader(_arrowFileReader, startRowId, endRowId);
  }

  // Placeholder implementation
  public int compare(int rowId1, int rowId2) {
    return 0;
  }

  public void read(int rowId, GenericRow buffer) {
    _arrowFileReader.read(rowId, buffer);
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasNext() {
    return _arrowFileReader.getCurrentRowCount() < _endRowId;
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    reuse = _arrowFileReader.next();
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _arrowFileReader.rewind();
  }

  @Override
  public void close() {
  }
}
