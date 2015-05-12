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

import com.google.common.base.Charsets;
import de.tuberlin.dima.aim3.HadoopJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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


    Job booksAndAuthors = prepareJob(books, outputPath, TextInputFormat.class, BroadCastMap.class, Text.class, NullWritable.class, TextOutputFormat.class);


    // Read the data of the file
    Configuration conf = new Configuration();
    conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/core-site.xml"));
    conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/hdfs-site.xml"));
    FileSystem fs = FileSystem.get(conf);

    FSDataInputStream inputStream = fs.open(authors);

    StringWriter writer = new StringWriter();
    IOUtils.copy(inputStream, writer, Charsets.UTF_8);
    String authorsAsString = writer.toString();

    System.out.println(authorsAsString);


    Configuration configuration = booksAndAuthors.getConfiguration();
    configuration.set("authors", authorsAsString);

    booksAndAuthors.waitForCompletion(true);

    return 0;
  }

  // Broadcast join is a map-only algorithm
  static class BroadCastMap extends Mapper<Object, Text, Text, NullWritable> {
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {


      // Parse the line
      String lineAsString = line.toString();
      String[] bookInfos = lineAsString.split("\t");

      String bookAuthor = (bookInfos[0]);
      String bookYear = (bookInfos[1]);
      String bookTitle = (bookInfos[2]);

      // We have to read the authors
      String authors = ctx.getConfiguration().get("authors");
      String[] authorsLines = authors.split("\n");

      for (int i=0; i<authorsLines.length; i++){

        String[] idToAuthorPair = authorsLines[i].split("\t");

        String authorId = idToAuthorPair[0];
        String authorName = idToAuthorPair[1];

        if (bookAuthor.equals(authorId)){


          String bookInformation = authorName + "\t" + bookTitle + "\t" + bookYear;
          System.out.println(bookInformation);

          ctx.write(new Text(bookInformation), NullWritable.get());
        }


      }

    }
  }
}