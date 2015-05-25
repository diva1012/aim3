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

package de.tuberlin.dima.aim3.assignment2;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.regex.Pattern;

public class AverageFriendFoeRatio {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> input = env.readTextFile(Config.pathToSlashdotZoo());

    /* Convert the input to edges, consisting of (source, target, isFriend ) */
    DataSet<Tuple3<Long, Long, Boolean>> edges = input.flatMap(new EdgeReader());


    /* Create a dataset of all vertex ids and count them */
    // We join all src and trg vertices and then we count them ---->>> Number of all vertices
    DataSet<Long> numVertices =
        edges.project(0).types(Long.class)
                .union(edges.project(1).types(Long.class))
            .distinct().reduceGroup(new CountVertices());

    /* Compute the degree of every vertex */
    // We get all src vertices and group them by id then we count them ------> Count src vertices pro id
    DataSet<Tuple3<Long, Long, Long>> verticesWithDegree =
        edges.project(0,2).types(Long.class, Boolean.class)
             .groupBy(0).reduceGroup(new DegreeOfPositiveVertex());


    /* Compute the degree distribution */
    // We goup by count of indeces and then (we pass count of vertices as parameter)
    DataSet<Tuple2<Long, Double>> degreeDistribution =
        verticesWithDegree.groupBy(1).reduceGroup(new DistributionElementPositive())
                                     .withBroadcastSet(numVertices, "numVertices");


    DataSet<Tuple2<Long, Double>> degreeDistribution2 =
            verticesWithDegree.groupBy(2).reduceGroup(new DistributionElementNegative())
                    .withBroadcastSet(numVertices, "numVertices");


    DataSet<Tuple3<Long, Double, Double>> posAndNegDistr = degreeDistribution.join(degreeDistribution2).where(0).equalTo(0).projectFirst(0,1).projectSecond(1).types(Long.class,Double.class,Double.class);


    posAndNegDistr.writeAsText(Config.outputPath(), FileSystem.WriteMode.OVERWRITE);
    posAndNegDistr.filter(new MyFilter());

    

    env.execute();
  }


  public static final class MyFilter implements FilterFunction<Tuple3<Long, Double, Double>> {

    @Override
    public boolean filter(Tuple3<Long, Double, Double> value) {
      return true;
    }
  }

  public static class EdgeReader implements FlatMapFunction<String, Tuple3<Long, Long, Boolean>> {

    private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

    @Override
    public void flatMap(String s, Collector<Tuple3<Long, Long, Boolean>> collector) throws Exception {
      if (!s.startsWith("%")) {
        String[] tokens = SEPARATOR.split(s);

        long source = Long.parseLong(tokens[0]);
        long target = Long.parseLong(tokens[1]);
        boolean isFriend = "+1".equals(tokens[2]);

        collector.collect(new Tuple3<Long, Long, Boolean>(source, target, isFriend));
      }
    }
  }

  public static class CountVertices implements GroupReduceFunction<Tuple1<Long>, Long> {
    @Override
    public void reduce(Iterable<Tuple1<Long>> vertices, Collector<Long> collector) throws Exception {
      collector.collect(new Long(Iterables.size(vertices)));
    }
  }


  public static class DegreeOfPositiveVertex implements GroupReduceFunction<Tuple2<Long, Boolean>, Tuple3<Long, Long, Long>> {
    @Override
    public void reduce(Iterable<Tuple2<Long, Boolean>> tuples, Collector<Tuple3<Long, Long, Long>> collector) throws Exception {

      Iterator<Tuple2<Long, Boolean>> iterator = tuples.iterator();

      Tuple2<Long, Boolean> vertex = iterator.next();

      Long vertexId = vertex.f0;

      long countPositive = 0L;
      long countNegative = 0L;

      if(vertex.f1){
        countPositive++;
      } else {
        countNegative++;
      }

      while (iterator.hasNext()) {
        Tuple2<Long, Boolean> vert = iterator.next();
        if(vert.f1.equals(true)){
          countPositive++;
        } else {
          countNegative++;
        }
      }

      collector.collect(new Tuple3<Long, Long, Long>(vertexId, countPositive, countNegative));
    }
  }

  public static class DistributionElementPositive extends RichGroupReduceFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Double>> {

    private long numVertices;
    // private long isPositive;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      numVertices = getRuntimeContext().<Long>getBroadcastVariable("numVertices").get(0);
      // isPositive = getRuntimeContext().<Long>getBroadcastVariable("isPositive").get(0);
    }

    @Override
    public void reduce(Iterable<Tuple3<Long, Long, Long>> verticesWithDegree, Collector<Tuple2<Long, Double>> collector) throws Exception {

      Iterator<Tuple3<Long, Long, Long>> iterator = verticesWithDegree.iterator();
      Long degree = iterator.next().f1;

      long count = 1L;
      while (iterator.hasNext()) {
        iterator.next();
        count++;
      }

      collector.collect(new Tuple2<Long, Double>(degree, (double) count / numVertices));
    }
  }

  public static class DistributionElementNegative extends RichGroupReduceFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Double>> {

    private long numVertices;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      numVertices = getRuntimeContext().<Long>getBroadcastVariable("numVertices").get(0);
    }

    @Override
    public void reduce(Iterable<Tuple3<Long, Long, Long>> verticesWithDegree, Collector<Tuple2<Long, Double>> collector) throws Exception {

      Iterator<Tuple3<Long, Long, Long>> iterator = verticesWithDegree.iterator();
      Long degree = iterator.next().f2;

      long count = 1L;
      while (iterator.hasNext()) {
        iterator.next();
        count++;
      }

      collector.collect(new Tuple2<Long, Double>(degree, (double) count / numVertices));
    }
  }

}
