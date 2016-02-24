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


import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static org.apache.giraph.conf.GiraphConstants.VERTEX_OUTPUT_FORMAT_SUBDIR;

/**
 * Write out Vertices' IDs and values, but not their edges nor edges' values.
 * This is a useful output format when the final value of the vertex is
 * all that's needed. The boolean configuration parameter reverse.id.and.value
 * allows reversing the output of id and value.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class NullOutputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends VertexOutputFormat<I, V, E> {
  protected GiraphTextOutputFormat textOutputFormat =
          new GiraphTextOutputFormat() {
            @Override
            protected String getSubdir() {
              return VERTEX_OUTPUT_FORMAT_SUBDIR.get(getConf());
            }
          };

  @Override
  public VertexWriter createVertexWriter(TaskAttemptContext context) {
    return new NullVertexWriter();
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException { }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return textOutputFormat.getOutputCommitter(context);
  }

  /**
   * Vertex writer used with {@link NullOutputFormat}.
   */
  protected class NullVertexWriter extends VertexWriter {
    @Override
    public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

    }

    @Override
    public void writeVertex(Vertex vertex) throws IOException, InterruptedException {

    }
  }
}
