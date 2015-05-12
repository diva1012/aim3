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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;

public class BookAndAuthorBroadcastJoin extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {

    Map<String, String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));


    Job averageTemp = prepareJob(authors, outputPath, TextInputFormat.class, MyMapper.class,
            Text.class, DoubleWritable.class, MyReducer.class, Text.class, DoubleWritable.class, TextOutputFormat.class);


    // Read the data of the file
    Configuration conf = new Configuration();
    conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/core-site.xml"));
    conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/hdfs-site.xml"));
    FileSystem fs = FileSystem.get(conf);

    FSDataInputStream inputStream = fs.open(authors);

    StringWriter writer = new StringWriter();
    IOUtils.copy(inputStream, writer, "UTF-8");
    String authorsAsString = writer.toString();

    System.out.println(authorsAsString);


    Configuration configuration = averageTemp.getConfiguration();
    configuration.set("authors", authorsAsString);


    return 0;
  }

  // Broadcast join is a map-only algorithm
  static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {


      // We have to read the broadcasted file

      String authors = ctx.getConfiguration().get("quality");
      //List<String> authorsLines =

      authors.split("\n");


      String lineAsString = line.toString();
      //String[] infos = lineAsStrig

    }
  }

  static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
            throws IOException, InterruptedException {

      int count = 0;

      for (IntWritable value : values){
        count += value.get();
      }

      ctx.write(key, new IntWritable(count));
      System.out.println(key.toString() +  " " + count);
    }
  }



}