package org.apache.giraph.io.formats;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Provides functionality similar to
 * {@link org.apache.hadoop.mapreduce.lib.input.TextInputFormat},
 * but allows for different data sources (vertex and edge data).
 */
public class GiraphProtobufInputFormat
    extends GiraphFileInputFormat<LongWritable, Text> {
  @Override
  public RecordReader<LongWritable, Text>
  createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new ProtobufRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }

  public static class ProtobufRecordReader extends RecordReader<LongWritable, Text> {
    private InputStream stream;
    private LongWritable key = null;
    private Text value = null;
    private Iterator<Graph.Vertex> vertices = null;

    public ProtobufRecordReader() {
    }

    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
      this.stream = new FileInputStream("/tmp/protobuf");
      this.key = new LongWritable();
      this.value = new Text();
    }

    public boolean nextKeyValue() throws IOException {
      if(vertices == null || !vertices.hasNext())
        vertices = Graph.Vertices.parseFrom(stream).getVerticesList().iterator();
      if(vertices.hasNext()) {
        Graph.Vertex vertex = vertices.next();

        this.key.set(vertex.getId());

        StringBuilder builder = new StringBuilder();
        for(Graph.Vertex.Edge edge: vertex.getEdgeList())
          builder.append(edge.getDestinationId()).append(' ')
                  .append(edge.getValue()).append(' ');

        this.value.set(builder.toString());

        throw new IOException(this.value.toString());
        //return true;
      } else
        return false;
    }

    public LongWritable getCurrentKey() {
      return this.key;
    }

    public Text getCurrentValue() {
      return this.value;
    }

    public float getProgress() {
      return 0;
    }

    public synchronized void close() throws IOException {
      stream.close();
    }
  }

}
