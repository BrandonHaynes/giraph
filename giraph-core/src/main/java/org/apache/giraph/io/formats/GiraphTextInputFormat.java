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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Provides functionality similar to
 * {@link org.apache.hadoop.mapreduce.lib.input.TextInputFormat},
 * but allows for different data sources (vertex and edge data).
 */
public class GiraphTextInputFormat
    extends GiraphFileInputFormat<LongWritable, Text> {
  @Override
  public RecordReader<LongWritable, Text>
  createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new LineRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    CompressionCodec codec =
        new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    return codec == null;
  }
/*
  //BH
  public static class LineRecordReader extends RecordReader<LongWritable, Text> {
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    //private LineReader in;
    private InputStream stream;
    //private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;

    public LineRecordReader() {
    }

    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
      FileSplit split = (FileSplit)genericSplit;
      Configuration job = context.getConfiguration();
      //this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", 2147483647);
      this.start = split.getStart();
      this.end = this.start + split.getLength();
      Path file = split.getPath();
      this.compressionCodecs = new CompressionCodecFactory(job);
      CompressionCodec codec = this.compressionCodecs.getCodec(file);
      //FileSystem fs = file.getFileSystem(job);
      //BH FSDataInputStream fileIn = fs.open(split.getPath());
      stream = new FileInputStream("/tmp/protobuf"); //BH

//      try {
        //boolean skipFirstLine = false;
        if (codec != null) {
          //this.in = new LineReader(codec.createInputStream(fileIn), job);
          this.end = 9223372036854775807L;
        } else {
          if (this.start != 0L) {
            //skipFirstLine = true;
            --this.start;
            //fileIn.seek(this.start);
          }

          //this.in = new LineReader(fileIn, job);

//          if(skipFirstLine) {
  //          this.start += (long)this.in.readLine(new Text(), 0, (int)Math.min(2147483647L, this.end - this.start));
    //      }
        }
//      } catch(IOException e) {
      //  fileIn.close();
  //      throw e;
    //  }


      this.pos = this.start;
    }

    public boolean nextKeyValue() throws IOException {

      if(this.key == null) {
        this.key = new LongWritable();
      }

      this.key.set(this.pos);
      if(this.value == null) {
        this.value = new Text();
      }

      //int newSize = 0;

      Graph.Vertices vertices;

      try {
        vertices = Graph.Vertices.parseFrom(stream);
      } catch(InvalidProtocolBufferException e) {
        throw e;
        //return false;
      }

      Graph.Vertex vertex = vertices.getVerticesList().get(0);
      
      this.key.set(vertex.getId());

      StringBuilder builder = new StringBuilder();
      for(Graph.Vertex.Edge edge: vertex.getEdgeList())
        builder.append(edge.getDestinationId()).append(' ')
               .append(edge.getValue()).append(' ');

      this.value.set(builder.toString());

      throw new IOException(this.value.toString());

      //return true;

      *
      while(this.pos < this.end) {
        newSize = this.in.readLine(this.value, this.maxLineLength, Math.max((int)Math.min(2147483647L, this.end - this.pos), this.maxLineLength));
        if(newSize == 0) {
          break;
        }

        this.pos += (long)newSize;
        if(newSize < this.maxLineLength) {
          break;
        }
      }

      if(newSize == 0) {
        this.key = null;
        this.value = null;
        return false;
      } else {
        return true;
      }
      *
    }

    public LongWritable getCurrentKey() {
      return this.key;
    }

    public Text getCurrentValue() {
      return this.value;
    }

    public float getProgress() {
      return this.start == this.end?0.0F:Math.min(1.0F, (float)(this.pos - this.start) / (float)(this.end - this.start));
    }

    public synchronized void close() throws IOException {
      stream.close();
  *
      if(this.in != null) {
        this.in.close();
      }
*
    }
  }
*/
}
