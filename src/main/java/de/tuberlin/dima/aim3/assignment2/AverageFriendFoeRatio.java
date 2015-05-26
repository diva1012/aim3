/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter
 * <p/>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p/>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.assignment2;

import com.google.common.collect.Iterables;
import org.apache.commons.math.stat.descriptive.summary.Sum;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
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
        // We get all src vertices and group them by id then we count them ------> Count src vertices pro i d
        DataSet<Tuple3<Long, Long, Long>> verticesWithDegree =
                edges.project(0, 2).types(Long.class, Boolean.class)
                        .groupBy(0).reduceGroup(new PositiveAndNegativeDegreeVertex());

        // Get the count of the vertices
        DataSet<Long> countNotNullVertices = verticesWithDegree.map(new MapForComputingCount()).reduceGroup(new CountVertices());

        // get the ratios
        DataSet<Tuple1<Double>> friendFoesRatios = verticesWithDegree.map(new FriendsFoesRatioMap());

        // calculate the summ of ratios
        DataSet<Tuple1<Double>> ratioSumm = friendFoesRatios.aggregate(Aggregations.SUM, 0);

        // Divide sum by the count
        DataSet<Tuple1<Double>> avgRatio = ratioSumm.groupBy(0).reduceGroup(new CalculateResult()).withBroadcastSet(countNotNullVertices, "filteredNumVertices");

        avgRatio.writeAsText(Config.outputPath(), FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }

    private static class CalculateResult extends RichGroupReduceFunction<Tuple1<Double>, Tuple1<Double>> {


        private long filteredNumVertices;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            filteredNumVertices = getRuntimeContext().<Long>getBroadcastVariable("filteredNumVertices").get(0);
        }

        @Override
        public void reduce(Iterable<Tuple1<Double>> sumRatio, Collector<Tuple1<Double>> collector) throws Exception {

         Double sumRatioDouble =   sumRatio.iterator().next().f0;



            collector.collect(new Tuple1<Double>( sumRatioDouble / filteredNumVertices));
        }
    }

    public static class FriendsFoesRatioMap implements MapFunction<Tuple3<Long, Long, Long>, Tuple1<Double>> {
        @Override
        public Tuple1<Double> map(Tuple3<Long, Long, Long> in) {
            return new Tuple1<Double>(in.f1.doubleValue() / in.f2.doubleValue());
        }
    }

    public static class MapForComputingCount implements MapFunction<Tuple3<Long, Long, Long>, Tuple1<Long>> {
        @Override
        public Tuple1<Long> map(Tuple3<Long, Long, Long> in) {
            return new Tuple1<Long>(1L);
        }
    }

    // ReduceFunction that sums Integers
    public class Summer implements ReduceFunction<Integer> {
        @Override
        public Integer reduce(Integer num1, Integer num2) {
            return num1 + num2;
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

    public static class PositiveAndNegativeDegreeVertex implements GroupReduceFunction<Tuple2<Long, Boolean>, Tuple3<Long, Long, Long>> {
        @Override
        public void reduce(Iterable<Tuple2<Long, Boolean>> tuples, Collector<Tuple3<Long, Long, Long>> collector) throws Exception {

            Iterator<Tuple2<Long, Boolean>> iterator = tuples.iterator();

            Tuple2<Long, Boolean> vertex = iterator.next();

            Long vertexId = vertex.f0;

            long countPositive = 0L;
            long countNegative = 0L;

            if (vertex.f1) {
                countPositive++;
            } else {
                countNegative++;
            }

            while (iterator.hasNext()) {
                Tuple2<Long, Boolean> vert = iterator.next();
                if (vert.f1.equals(true)) {
                    countPositive++;
                } else {
                    countNegative++;
                }
            }

            if (countNegative != 0 && countPositive != 0) {
                collector.collect(new Tuple3<Long, Long, Long>(vertexId, countPositive, countNegative));
            }
        }
    }

}
