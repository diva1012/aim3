/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.assignment1;

import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.base.Joiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;


public class BookAndAuthorReduceSideJoin extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String, String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job job = new Job(new Configuration(getConf()));
    Configuration jobConf = job.getConfiguration();

    // Define two data inputs for the mapper
    MultipleInputs.addInputPath(job, authors, TextInputFormat.class, AuthorMapper.class);
    MultipleInputs.addInputPath(job, books, TextInputFormat.class, BooksMapper.class);

    // Set the types of keys and values of the output of the mapper
    job.setMapOutputKeyClass(IdAndTypePair.class);
    job.setMapOutputValueClass(Text.class);

    // Set the specific reducer
    job.setReducerClass(JoinReducer.class);

    // Set the types of the output keys and values of the reducer job and respectively of the whole job
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    // we define custom classes for secondary sorting
    job.setPartitionerClass(NaturalKeyPartitioner.class);
    job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    jobConf.set("mapred.output.dir", outputPath.toString());

    job.waitForCompletion(true);

    return 0;

  }


  /////////////////// Definition of mapper ///////////////////////

  /**
   * Abstract class for the implementation of concrete mappers for authors and books
   */
  static abstract class JoinMapper extends Mapper<Object, Text, IdAndTypePair, Text> {

    /**
     * @return Return the type of the mapper
     */
    abstract String getType();

    @Override
    protected void map(Object key, Text value,
                       Context context) throws IOException,
            InterruptedException {

      String[] fields = value.toString().split("\t");


      // Use the own writable comparable class for keys
      IdAndTypePair keyOut = new IdAndTypePair(new Text(fields[0]), new Text(getType()));


      // Transmit the tuple data as value
      // For simplicity we send the author-id again, which adds minor traffic
      context.write(keyOut, value);

      System.out.println(fields[0] + "\t" + getType() + "   ------>     " + value.toString());
    }
  }


  /**
   * Implement a concrete join mapper for authors
   */
  static class AuthorMapper extends JoinMapper {

    private static final String TYPE = "author";

    @Override
    String getType() {
      return TYPE;
    }

  }

  /**
   * Implement a concrete join mapper for books
   */
  static class BooksMapper extends JoinMapper {

    private static final String TYPE = "book";

    @Override
    String getType() {
      return TYPE;
    }

  }
  ////////////// End of the definition of mapper //////////////////////


  //////////////////// Definition of reducer  ////////////////////////

  static class JoinReducer extends Reducer<IdAndTypePair, Text, Text, NullWritable> {

    private Text out = new Text();  // single instance, to reduce allocations

    @Override
    protected void reduce(IdAndTypePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      // As long as we have one to many relationship between authors and books and the author is
      // on the first position we can just pick him at the beginning
      Iterator<Text> it = values.iterator();
      String author = it.next().toString().split("\t")[1];

      System.out.println("AUTHOR    ----->    " + author);

      // Iterate over following book values. Every value is a match
      while (it.hasNext()) {
        String[] bookFields = it.next().toString().split("\t");


        String output = author + "\t" + bookFields[2] + "\t" + bookFields[1];

        System.out.println("\t" + author + "   ---->   " + bookFields[2] + "\t" + bookFields[1]);

        context.write(new Text(output), NullWritable.get());


      }
    }

  }

  /**
   * This Partitioner considers the natural key only (first part of composite key)
   * This makes sure that all values with the same natural key arrive at the same reduce-task
   */
  static class NaturalKeyPartitioner extends Partitioner<IdAndTypePair, Text> {

    @Override
    public int getPartition(IdAndTypePair pair, Text value, int numPartitions) {
      int hashCode = pair.getLeft().hashCode();
      return hashCode % numPartitions;
    }

  }

  /**
   * This class will be used when we group the items so we can sed them tu the reducer.
   * We group only by the Id of the IdAndTypePair
   */
  static class NaturalKeyGroupingComparator extends WritableComparator {

    protected NaturalKeyGroupingComparator() {
      super(IdAndTypePair.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      // No other way than to cast
      IdAndTypePair first = (IdAndTypePair) a;
      IdAndTypePair second = (IdAndTypePair) b;
      return first.getLeft().toString().compareTo(second.getLeft().toString());
    }

  }


  /**
   * We define new Writable comparable Class for the key output of the mapper
   */
  static class IdAndTypePair implements WritableComparable<IdAndTypePair> {

    private Text id;
    private Text type;

    IdAndTypePair() {
      this.id = new Text();
      this.type = new Text();
    }

    IdAndTypePair(final Text left, final Text right) {
      this.id = left;
      this.type = right;
    }

    @Override
    public int compareTo(IdAndTypePair other) {
      // This will compare both parts of the composite key, for purpose of sorting
      int result = this.id.compareTo(other.getLeft());
      if (result == 0) {
        result = this.type.compareTo(other.getRight());
      }
      return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      id.write(out);
      type.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      id.readFields(in);
      type.readFields(in);
    }

    @Override
    public int hashCode() {
      String idAndType = id.toString() + type.toString();
      return idAndType.hashCode();
    }

    @Override
    public String toString() {
      return id + "\t" + type;
    }

    private Text getLeft() {
      return id;
    }

    private Text getRight() {
      return type;
    }
  }

}