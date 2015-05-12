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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.DoubleValue;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AverageTemperaturePerMonth extends HadoopJob {

  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));

    Job averageTemp = prepareJob(inputPath, outputPath, TextInputFormat.class, MyMapper.class,
            Text.class, DoubleWritable.class, MyReducer.class, Text.class, DoubleWritable.class, TextOutputFormat.class);

     averageTemp.getConfiguration().set("minQuality", String.valueOf(minimumQuality));

    averageTemp.waitForCompletion(true);

    return 0;
  }

    static class MyMapper extends Mapper<Object,Text,Text,DoubleWritable> {
        @Override
        protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
            String lineAsString = line.toString();
            String[] infos = lineAsString.split("\\s+");

            Integer year = Integer.parseInt(infos[0]);
            Integer month = Integer.parseInt(infos[1]);

            double temp = Double.parseDouble(infos[2]);
            double quality = Double.parseDouble(infos[3]);

            DoubleWritable tempAsWritable = new DoubleWritable(temp);
            
            double minQuality = Double.parseDouble(ctx.getConfiguration().get("minQuality"));

            if (quality >= minQuality) {
                String yearmonth = year.toString() + "\t" + month.toString();
                ctx.write(new Text(yearmonth), tempAsWritable);
            }
        }
    }

  static class MyReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context ctx)
            throws IOException, InterruptedException {

      Double summ = 0.0;
      Integer count = 0;

      for (DoubleWritable value : values){

        summ = summ + value.get();
        count ++;
      }

      Double avg = summ / count;
      ctx.write(key, new DoubleWritable(avg));
    }
  }
}