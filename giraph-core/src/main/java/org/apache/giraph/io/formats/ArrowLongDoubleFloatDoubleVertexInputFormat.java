/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.io.formats;

import com.google.common.io.LineReader;
import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.mmap.MemoryMappedFileBuffer;
import io.netty.channel.local.LocalAddress;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.MaterializedField;
import org.apache.arrow.vector.types.Types;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
  * VertexInputFormat that features <code>long</code> vertex ID's,
  * <code>double</code> vertex values and <code>float</code>
  * out-edge weights, and <code>double</code> message types,
  *  specified in Protobuf format.
  */
public class ArrowLongDoubleFloatDoubleVertexInputFormat extends
  TextVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new ArrowLongDoubleFloatDoubleVertexReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
          throws IOException, InterruptedException {
    return super.getSplits(context, minSplitCountHint);
  }


  class ArrowLongDoubleFloatDoubleVertexReader extends TextVertexReader {
    private InputStream input;
    private Vertex<LongWritable, DoubleWritable, FloatWritable> vertex;
    private LongWritable id = new LongWritable();
    private DoubleWritable value = new DoubleWritable();

    public ArrowLongDoubleFloatDoubleVertexReader() {
    }

    Socket socket = null;
    private final static int ALLOCATION_SIZE = BaseValueVector.INITIAL_VALUE_ALLOCATION * 8 * 1024;
    private final static int BATCH_SIZE = 10 * 1000;
    private transient BufferAllocator allocator = null;
    private UInt8Vector vector0;
    private UInt8Vector vector2;
    private UInt8Vector vector4;
    private UInt8Vector vector6;
    private Float8Vector vector1;
    private Float8Vector vector3;
    private Float8Vector vector5;
    private Float8Vector vector7;
    private UInt8Vector.Accessor accessor0;
    private UInt8Vector.Accessor accessor2;
    private UInt8Vector.Accessor accessor4;
    private UInt8Vector.Accessor accessor6;
    private Float8Vector.Accessor accessor1;
    private Float8Vector.Accessor accessor3;
    private Float8Vector.Accessor accessor5;
    private Float8Vector.Accessor accessor7;
    private ArrowBuf buffer = null;
    private byte[] sizeBuffer = new byte[4];
    private MemoryMappedFileBuffer sharedMemory0, sharedMemory1, sharedMemory2, sharedMemory3, sharedMemory4, sharedMemory5, sharedMemory6, sharedMemory7;

    private final Logger LOG = Logger.getLogger(ArrowLongDoubleFloatDoubleVertexReader.class);

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
      this.vertex = getConf().createVertex();

      LOG.info("Arrow: begin initializing");

      int retries = 20;
      int port = Integer.parseInt(new LineReader(new InputStreamReader(new FileInputStream("/tmp/port"))).readLine());
      allocator = new RootAllocator(ALLOCATION_SIZE);
      vector0 = new UInt8Vector(MaterializedField.create("field0", new Types.MajorType(Types.MinorType.INT, Types.DataMode.REQUIRED)), allocator);
      vector2 = new UInt8Vector(MaterializedField.create("field2", new Types.MajorType(Types.MinorType.INT, Types.DataMode.REQUIRED)), allocator);
      vector4 = new UInt8Vector(MaterializedField.create("field4", new Types.MajorType(Types.MinorType.INT, Types.DataMode.REQUIRED)), allocator);
      vector6 = new UInt8Vector(MaterializedField.create("field6", new Types.MajorType(Types.MinorType.INT, Types.DataMode.REQUIRED)), allocator);
      vector1 = new Float8Vector(MaterializedField.create("field1", new Types.MajorType(Types.MinorType.FLOAT8, Types.DataMode.REQUIRED)), allocator);
      vector3 = new Float8Vector(MaterializedField.create("field3", new Types.MajorType(Types.MinorType.FLOAT8, Types.DataMode.REQUIRED)), allocator);
      vector5 = new Float8Vector(MaterializedField.create("field5", new Types.MajorType(Types.MinorType.FLOAT8, Types.DataMode.REQUIRED)), allocator);
      vector7 = new Float8Vector(MaterializedField.create("field7", new Types.MajorType(Types.MinorType.FLOAT8, Types.DataMode.REQUIRED)), allocator);
      vector0.allocateNew(BATCH_SIZE);
      vector1.allocateNew(BATCH_SIZE);
      vector2.allocateNew(BATCH_SIZE);
      vector3.allocateNew(BATCH_SIZE);
      vector4.allocateNew(BATCH_SIZE);
      vector5.allocateNew(BATCH_SIZE);
      vector6.allocateNew(BATCH_SIZE);
      vector7.allocateNew(BATCH_SIZE);

      sharedMemory0 = new MemoryMappedFileBuffer(new File("/tmp/vector0"), BATCH_SIZE * 8);
      sharedMemory1 = new MemoryMappedFileBuffer(new File("/tmp/vector1"), BATCH_SIZE * 8);
      sharedMemory2 = new MemoryMappedFileBuffer(new File("/tmp/vector2"), BATCH_SIZE * 8);
      sharedMemory3 = new MemoryMappedFileBuffer(new File("/tmp/vector3"), BATCH_SIZE * 8);
      sharedMemory4 = new MemoryMappedFileBuffer(new File("/tmp/vector4"), BATCH_SIZE * 8);
      sharedMemory5 = new MemoryMappedFileBuffer(new File("/tmp/vector5"), BATCH_SIZE * 8);
      sharedMemory6 = new MemoryMappedFileBuffer(new File("/tmp/vector6"), BATCH_SIZE * 8);
      sharedMemory7 = new MemoryMappedFileBuffer(new File("/tmp/vector7"), BATCH_SIZE * 8);

      buffer = allocator.buffer(BATCH_SIZE * 8);

      LOG.info("Arrow: done allocating");

      while(retries-- > 0) {
        try {
          socket = new Socket("localhost", port);
          this.input = socket.getInputStream();
          return;
        } catch(IOException e) {
          if(retries == 0)
            throw e;
          try {
            Thread.sleep(1000);
          } catch(InterruptedException e2) {}
        }
      }

      LOG.info("Arrow: done initializing");
    }
    
    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      hasVertex = false;
      return hasNextVertex();
    }

    @Override
    public synchronized void close() throws IOException {
      input.close();
      if(socket != null)
        socket.close();
    }

    @Override
    public Vertex<LongWritable, DoubleWritable, FloatWritable> getCurrentVertex()
           throws IOException, InterruptedException {
      return this.vertex;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    private boolean hasVertex = false;
    private int remaining = 0;
    //private int index = 0;
    ByteBuffer nioBuffer = null;

    private boolean hasNextVertex() throws IOException {
      return hasNextVertexArrowCompressed();
      //return hasNextVertexArrowSharedMemory();
    }

    private boolean hasNextVertexArrowSharedMemory() throws IOException {
      if(!hasVertex) {
        if(remaining == 0) {
          LOG.info("Arrow: clear");

          if(readSize() == -1)
            return false;

          if(nioBuffer == null)
            nioBuffer = ByteBuffer.allocate(BATCH_SIZE * 8);
          vector0.getBuffer().readerIndex(0);
          vector1.getBuffer().readerIndex(0);
          vector2.getBuffer().readerIndex(0);
          vector3.getBuffer().readerIndex(0);
          vector4.getBuffer().readerIndex(0);
          vector5.getBuffer().readerIndex(0);
          vector6.getBuffer().readerIndex(0);
          vector7.getBuffer().readerIndex(0);

          readSharedMemoryColumn(sharedMemory0, vector0.getBuffer());
          readSharedMemoryColumn(sharedMemory1, vector1.getBuffer());
          readSharedMemoryColumn(sharedMemory2, vector2.getBuffer());
          readSharedMemoryColumn(sharedMemory3, vector3.getBuffer());
          readSharedMemoryColumn(sharedMemory4, vector4.getBuffer());
          readSharedMemoryColumn(sharedMemory5, vector5.getBuffer());
          readSharedMemoryColumn(sharedMemory6, vector6.getBuffer());
          readSharedMemoryColumn(sharedMemory7, vector7.getBuffer());

          LOG.info("Arrow: done read " + vector0.getBufferSize());
          vector0.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector1.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector2.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector3.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector4.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector5.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector6.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector7.getBuffer().writerIndex(BATCH_SIZE * 8);
          //LOG.info("Arrow: set writer index");

          remaining = BATCH_SIZE;
        }

        id.set(vector0.getBuffer().readUnsignedInt());
        value.set(vector1.getBuffer().readDouble());
        this.vertex.initialize(id, value);
        this.vertex.addEdge(EdgeFactory.create(new LongWritable(vector2.getBuffer().readUnsignedInt()), new FloatWritable((float)vector3.getBuffer().readDouble())));
        this.vertex.addEdge(EdgeFactory.create(new LongWritable(vector4.getBuffer().readUnsignedInt()), new FloatWritable((float)vector5.getBuffer().readDouble())));
        this.vertex.addEdge(EdgeFactory.create(new LongWritable(vector6.getBuffer().readUnsignedInt()), new FloatWritable((float)vector7.getBuffer().readDouble())));

        remaining--;
        hasVertex = true;
      }

      return hasVertex;
    }

    private boolean hasNextVertexArrowCompressed() throws IOException {
      if(!hasVertex) {
        if(remaining == 0) {
          int size;
          LOG.info("Arrow: clear");

          if(nioBuffer == null)
            nioBuffer = ByteBuffer.allocate(BATCH_SIZE * 8);
          vector0.getBuffer().readerIndex(0);
          vector1.getBuffer().readerIndex(0);
          vector2.getBuffer().readerIndex(0);
          vector3.getBuffer().readerIndex(0);
          vector4.getBuffer().readerIndex(0);
          vector5.getBuffer().readerIndex(0);
          vector6.getBuffer().readerIndex(0);
          vector7.getBuffer().readerIndex(0);

          if(!readCompressedColumn(vector0.getBuffer()) ||
             !readCompressedColumn(vector1.getBuffer()) ||
             !readCompressedColumn(vector2.getBuffer()) ||
             !readCompressedColumn(vector3.getBuffer()) ||
             !readCompressedColumn(vector4.getBuffer()) ||
             !readCompressedColumn(vector5.getBuffer()) ||
             !readCompressedColumn(vector6.getBuffer()) ||
             !readCompressedColumn(vector7.getBuffer()))
            return false;

          LOG.info("Arrow: done read " + vector0.getBufferSize());
          vector0.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector1.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector2.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector3.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector4.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector5.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector6.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector7.getBuffer().writerIndex(BATCH_SIZE * 8);
          LOG.info("Arrow: set writer index");

          remaining = BATCH_SIZE;
        }

        id.set(vector0.getBuffer().readUnsignedInt());
        value.set(vector1.getBuffer().readDouble());
        this.vertex.initialize(id, value);
        this.vertex.addEdge(EdgeFactory.create(new LongWritable(vector2.getBuffer().readUnsignedInt()), new FloatWritable((float)vector3.getBuffer().readDouble())));
        this.vertex.addEdge(EdgeFactory.create(new LongWritable(vector4.getBuffer().readUnsignedInt()), new FloatWritable((float)vector5.getBuffer().readDouble())));
        this.vertex.addEdge(EdgeFactory.create(new LongWritable(vector6.getBuffer().readUnsignedInt()), new FloatWritable((float)vector7.getBuffer().readDouble())));
        remaining--;
        hasVertex = true;
      }

      return hasVertex;
    }

    private boolean hasNextVertexArrow() throws IOException {
      if(!hasVertex) {
        if(remaining == 0) {
          int size;
          LOG.info("Arrow: clear");

          if(nioBuffer == null)
            nioBuffer = ByteBuffer.allocate(BATCH_SIZE * 8);
          //SocketAddress address = new SocketAddress();
          //new io.netty.channel.local.LocalChannel().connect(new LocalAddress("localhost"));
          vector0.getBuffer().readerIndex(0);
          vector1.getBuffer().readerIndex(0);
          vector2.getBuffer().readerIndex(0);
          vector3.getBuffer().readerIndex(0);
          vector4.getBuffer().readerIndex(0);
          vector5.getBuffer().readerIndex(0);
          vector6.getBuffer().readerIndex(0);
          vector7.getBuffer().readerIndex(0);
          //vector0.clear();
          //vector1.clear();
          //vector2.clear();
          //vector3.clear();
          //vector4.clear();
          //vector5.clear();
          //vector6.clear();
          //vector7.clear();

          if(!readColumn(vector0.getBuffer()) ||
                  !readColumn(vector1.getBuffer()) ||
                  !readColumn(vector2.getBuffer()) ||
                  !readColumn(vector3.getBuffer()) ||
                  !readColumn(vector4.getBuffer()) ||
                  !readColumn(vector5.getBuffer()) ||
                  !readColumn(vector6.getBuffer()) ||
                  !readColumn(vector7.getBuffer()))
            return false;

          /*
          LOG.info("Arrow: begin read0");
          size = readSize();
          int r;
          if((r = input.read(nioBuffer.array(), 0, size)) != size) {
            LOG.info("Arrow: << read " + r);
            return false;
          }
          LOG.info("Arrow: read " + r);
          vector0.getBuffer().writeBytes(nioBuffer);

          if(input.read(nioBuffer.array()) != BATCH_SIZE*8)
            return false;
          vector1.getBuffer().writeBytes(nioBuffer);
          if(input.read(nioBuffer.array()) != BATCH_SIZE*8)
            return false;
          vector2.getBuffer().writeBytes(nioBuffer);
          if(input.read(nioBuffer.array()) != BATCH_SIZE*8)
            return false;
          vector3.getBuffer().writeBytes(nioBuffer);
          if(input.read(nioBuffer.array()) != BATCH_SIZE*8)
            return false;
          vector4.getBuffer().writeBytes(nioBuffer);
          if(input.read(nioBuffer.array()) != BATCH_SIZE*8)
            return false;
          vector5.getBuffer().writeBytes(nioBuffer);
          if(input.read(nioBuffer.array()) != BATCH_SIZE*8)
            return false;
          vector6.getBuffer().writeBytes(nioBuffer);
          if(input.read(nioBuffer.array()) != BATCH_SIZE*8)
            return false;
          vector7.getBuffer().writeBytes(nioBuffer);
          */

          /*
          if(input.read(vector0.getBuffer().array()) != vector0.getBufferSize())
            return false;
          LOG.info("Arrow: begin read1");
          if(input.read(vector1.getBuffer().array()) != vector0.getBufferSize())
            return false;
          if(input.read(vector2.getBuffer().array()) != vector0.getBufferSize())
            return false;
          if(input.read(vector3.getBuffer().array()) != vector0.getBufferSize())
            return false;
          if(input.read(vector4.getBuffer().array()) != vector0.getBufferSize())
            return false;
          if(input.read(vector5.getBuffer().array()) != vector0.getBufferSize())
            return false;
          if(input.read(vector6.getBuffer().array()) != vector0.getBufferSize())
            return false;
          if(input.read(vector7.getBuffer().array()) != vector0.getBufferSize())
            return false;
            */
          LOG.info("Arrow: done read " + vector0.getBufferSize());
          vector0.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector1.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector2.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector3.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector4.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector5.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector6.getBuffer().writerIndex(BATCH_SIZE * 8);
          vector7.getBuffer().writerIndex(BATCH_SIZE * 8);
          LOG.info("Arrow: set writer index");
          //accessor0 = vector0.getAccessor();
          //accessor1 = vector1.getAccessor();
          //accessor2 = vector2.getAccessor();
          //accessor3 = vector3.getAccessor();
          //accessor4 = vector4.getAccessor();
          //accessor5 = vector5.getAccessor();
          //accessor6 = vector6.getAccessor();
          //accessor7 = vector7.getAccessor();

          //index = 0;
          remaining = BATCH_SIZE;
        }

        id.set(vector0.getBuffer().readUnsignedInt());
        value.set(vector1.getBuffer().readDouble());
        this.vertex.initialize(id, value);
        this.vertex.addEdge(EdgeFactory.create(new LongWritable(vector2.getBuffer().readUnsignedInt()), new FloatWritable((float)vector3.getBuffer().readDouble())));
        this.vertex.addEdge(EdgeFactory.create(new LongWritable(vector4.getBuffer().readUnsignedInt()), new FloatWritable((float)vector5.getBuffer().readDouble())));
        this.vertex.addEdge(EdgeFactory.create(new LongWritable(vector6.getBuffer().readUnsignedInt()), new FloatWritable((float)vector7.getBuffer().readDouble())));
        //id.set(accessor0.get(index));
        //value.set(accessor1.get(index));
        //this.vertex.initialize(id, value);
        //this.vertex.addEdge(EdgeFactory.create(new LongWritable(accessor2.get(index)), new FloatWritable((float)accessor3.get(index))));
        //this.vertex.addEdge(EdgeFactory.create(new LongWritable(accessor4.get(index)), new FloatWritable((float)accessor5.get(index))));
        //this.vertex.addEdge(EdgeFactory.create(new LongWritable(accessor6.get(index)), new FloatWritable((float)accessor7.get(index))));
        //index++;
        remaining--;
        hasVertex = true;
      }

      return hasVertex;
    }

    private boolean readCompressedColumn(ArrowBuf buffer) throws IOException {
      int size, read = -1;

      LOG.info("Arrow: begin read");

      if((size = readSize()) == -1)
        return false;

      LOG.info("Arrow: found size " + size);

      int index = 0;
      while(size > 0) {
        if ((read = input.read(nioBuffer.array(), index, size)) == -1) {
          LOG.info("Arrow: too few read " + read);
          return false;
        }
        size -= read;
        index += read;
      }
      LOG.info("Arrow: read block " + read);

      buffer.writeBytes(decompressBuffer(nioBuffer));

      return true;
    }

    private boolean readColumn(ArrowBuf buffer) throws IOException {
      int size, read;

      LOG.info("Arrow: begin read");

      if((size = readSize()) == -1)
        return false;
      else if((read = input.read(nioBuffer.array(), 0, size)) != size) {
        LOG.info("Arrow: too few read " + read);
        return false;
      }
      LOG.info("Arrow: read block " + read);
      buffer.writeBytes(nioBuffer);
      return true;
    }

    private boolean readSharedMemoryColumn(MemoryMappedFileBuffer sharedMemory, ArrowBuf buffer) throws IOException {
      sharedMemory.buffer().readerIndex(0);
      buffer.writerIndex(0);
      buffer.writeBytes(sharedMemory.buffer(), 0, BATCH_SIZE * 8);
      return true;
    }

    private int readSize() throws IOException {
      if(this.input.read(sizeBuffer) != 4)
        return -1;

      return ByteBuffer.wrap(sizeBuffer).asIntBuffer().get();
    }

    private byte[] decompressBuffer(ByteBuffer input) {
      Inflater inflater = new Inflater();
      byte[] data = new byte[input.remaining()];
      input.get(data);
      inflater.setInput(data);

      ByteBuffer buffer = ByteBuffer.allocate(data.length + 100);
      try {
        int size = inflater.inflate(buffer.array());
        inflater.end();

        buffer.limit(size);
        return buffer.array();
      } catch(DataFormatException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
