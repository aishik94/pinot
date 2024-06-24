package org.apache.pinot.core.segment.processing.genericrow;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

import static java.lang.System.exit;


public class testClass {

  public static RootAllocator _rootAllocator = new RootAllocator(Long.MAX_VALUE);

  public static VectorSchemaRoot getVectorSchemaRootForFile(File file)
      throws IOException {
    ArrowFileReader reader = getArrowReaderForFile(file);
    ArrowBlock arrowBlock = reader.getRecordBlocks().get(0);
    reader.loadRecordBatch(arrowBlock);
//    VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
    return reader.getVectorSchemaRoot();
  }

  public static ArrowFileReader getArrowReaderForFile(File file)
      throws IOException {
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
      return new ArrowFileReader(seekableByteChannel, _rootAllocator);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }



  public static void main(String[] args) {

    String filePath = "/Users/aishik/Work/rawData/outfiles/finalOutput/";

    // get the number of file in the file path.
    File file = new File(filePath);
    int numFiles = file.listFiles().length;

    boolean isFirstBatch = true;
    String lastString = "";

    for (int i = 0; i < numFiles; i++) {
      File currentFile = new File(filePath + "outfile_" + i + ".arrow");
      try {
        VectorSchemaRoot vectorSchemaRoot = getVectorSchemaRootForFile(currentFile);
        VarCharVector varCharVector = (VarCharVector) vectorSchemaRoot.getVector("high_cardinality_string");

        // Check if the vector is lexicographically sorted.
        for (int j = 0; j < vectorSchemaRoot.getRowCount() - 1; j++) {
          if (!isFirstBatch && varCharVector.getObject(j).toString().compareTo(lastString) < 0) {
            System.out.println("Not sorted");
            exit(1);
            break;
          }
          if (varCharVector.getObject(j).toString().compareTo(varCharVector.getObject(j + 1).toString()) > 0){
            System.out.println("Not sorted");
            exit(1);
            break;
          }
        }
        lastString = varCharVector.getObject(vectorSchemaRoot.getRowCount() - 1).toString();
        isFirstBatch = false;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    System.out.println("Sorted");
  }
}
