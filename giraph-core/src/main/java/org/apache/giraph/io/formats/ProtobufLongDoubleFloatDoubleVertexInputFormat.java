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
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
  * VertexInputFormat that features <code>long</code> vertex ID's,
  * <code>double</code> vertex values and <code>float</code>
  * out-edge weights, and <code>double</code> message types,
  *  specified in Protobuf format.
  */
public class ProtobufLongDoubleFloatDoubleVertexInputFormat extends
  TextVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new ProtobufLongDoubleFloatDoubleVertexReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
          throws IOException, InterruptedException {
    return super.getSplits(context, minSplitCountHint);
  }


  class ProtobufLongDoubleFloatDoubleVertexReader extends TextVertexReader {
    private InputStream input;
    //private CodedInputStream stream;
    private Iterator<Graph.Vertex> vertices = null;
    private Vertex<LongWritable, DoubleWritable, FloatWritable> vertex;
    private LongWritable id = new LongWritable();
    private DoubleWritable value = new DoubleWritable();
    private byte[] sizeBuffer = new byte[4];

    public ProtobufLongDoubleFloatDoubleVertexReader() {
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
      //initializeProtobufOrBinary();
      initializeProtobufOrBinarySocket();

      //this.stream = CodedInputStream.newInstance(input);
      this.vertex = getConf().createVertex();

      //this.stream = new FileInputStream("/tmp/protobuf");

      /*
      FileSplit split = (FileSplit)genericSplit;
      Configuration job = context.getConfiguration();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(job);
      this.stream = fs.open(split.getPath());
      */
    }

    Socket socket = null;
    private void initializeProtobufOrBinarySocket() throws IOException {
      int retries = 20;

      while(retries-- > 0) {
        try {
          int port = Integer.parseInt(new LineReader(new InputStreamReader(new FileInputStream("/tmp/port"))).readLine());
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
    }
    
    private void initializeProtobufOrBinary() throws IOException {
      this.input = new BufferedInputStream(new FileInputStream("/tmp/protobuf"));
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return nextVertexBinary();
    }

    public boolean nextVertexBinary() throws IOException, InterruptedException {
      hasVertex = false;
      return hasNextVertex();
    }

    public boolean nextVertexProtobuf() throws IOException, InterruptedException {
      if(hasNextVertex()) {
        Graph.Vertex vertex = vertices.next();

        id.set(vertex.getId());
        value.set(vertex.getValue());
        this.vertex.initialize(id, value);

        for(Graph.Vertex.Edge edge: vertex.getEdgeList())
          this.vertex.addEdge(EdgeFactory.create(
                  new LongWritable(edge.getDestinationId()),
                  new FloatWritable((float)edge.getValue())));
      }

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

    private boolean hasNextVertex() throws IOException {
      return hasNextVertexBinary();
    }

    private int batch_size = 1;
    private byte[] buffer = new byte[batch_size * (1*8 + 4*8 + 3*4)];
    private boolean hasVertex = false;
    private int remaining = 0;
    private int index = 0;

    private boolean hasNextVertexBinary() throws IOException {
      if(!hasVertex) {
        if(remaining == 0) {
          //if (this.input.read(buffer.array()) != buffer.array().length)
          if (this.input.read(buffer) != buffer.length)
            return false;
          remaining = batch_size;
          index = 0;
        }

        id.set(Longs.fromBytes(buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++]));
        value.set(Longs.fromBytes(buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++]));
        this.vertex.initialize(id, value);
        this.vertex.addEdge(EdgeFactory.create(new LongWritable(Ints.fromBytes(buffer[index++], buffer[index++], buffer[index++], buffer[index++])), new FloatWritable((float)Longs.fromBytes(buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++]))));
        this.vertex.addEdge(EdgeFactory.create(new LongWritable(Ints.fromBytes(buffer[index++], buffer[index++], buffer[index++], buffer[index++])), new FloatWritable((float)Longs.fromBytes(buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++]))));
        this.vertex.addEdge(EdgeFactory.create(new LongWritable(Ints.fromBytes(buffer[index++], buffer[index++], buffer[index++], buffer[index++])), new FloatWritable((float)Longs.fromBytes(buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++], buffer[index++]))));
        //id.set(buffer.getLong());
        //value.set(buffer.getDouble());
        //this.vertex.initialize(id, value);
        //this.vertex.addEdge(EdgeFactory.create(new LongWritable(buffer.getInt()), new FloatWritable((float)buffer.getDouble())));
        //this.vertex.addEdge(EdgeFactory.create(new LongWritable(buffer.getInt()), new FloatWritable((float)buffer.getDouble())));
        //this.vertex.addEdge(EdgeFactory.create(new LongWritable(buffer.getInt()), new FloatWritable((float)buffer.getDouble())));
        hasVertex = true;
        remaining--;
      }

      return hasVertex;
    }

    private boolean hasNextVertexProtobuf() throws IOException {
      if(vertices == null || !vertices.hasNext()) {
        if(this.input.read(sizeBuffer) != 4)
          return false;
        
        int size = ByteBuffer.wrap(sizeBuffer).asIntBuffer().get();
        byte[] buffer = new byte[size];
        this.input.read(buffer);

        vertices = Graph.Vertices.parseFrom(buffer).getVerticesList().iterator();
      }

      return vertices.hasNext();
    }
  }
}
