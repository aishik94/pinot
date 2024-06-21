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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;


public class testClass {
  List<File> getFileListFromDirectoryPath(String dirPath) {
    List<File> fileList = new ArrayList<>();
    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(dirPath))) {
      for (Path path : directoryStream) {
        fileList.add(path.toFile());
      }
      Collections.sort(fileList);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return fileList;
  }

  public static void main(String[] args) {
    String fileP = "/Users/aishik/Work/rawData/outfiles/finalOutput/";
    List<String> elements = new ArrayList<>();

    List<File> fileList = new testClass().getFileListFromDirectoryPath(fileP);

    for (File file : fileList) {
      // declare allocator
      RootAllocator _rootAllocator = new RootAllocator(Long.MAX_VALUE);

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
        ArrowBlock arrowBlock = reader.getRecordBlocks().get(0);
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();



        // Extract the elements of "high_cardinality_string" from  vectorschemaroot to elements
        for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
          elements.add(vectorSchemaRoot.getVector("high_cardinality_string").getObject(i).toString());
        }

        // check if the strings are lexicoographically sorted



      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    for (int i = 0; i < elements.size() - 1; i++) {
      if (elements.get(i).compareTo(elements.get(i + 1)) > 0) {
        System.out.println("Not sorted");
        System.out.println(i);
        break;
      }
    }

  }
}
