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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.regex.Pattern;

public class ChainLetter {

  public static final double INITIATOR_RATIO = 0.00125;
  public static final double FORWARDING_PROBABILITY = 0.5;

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> input = env.readTextFile(Config.pathToSlashdotZoo());

    DataSet<Tuple2<Long, Long>> edges = input.flatMap(new EdgeReader());

    DataSet<Tuple1<Long>> edgesToVertices = edges.project(0).types(Long.class);

    DataSet<Tuple1<Long>> uniqueVertexIds = edgesToVertices.distinct();

    DataSet<Tuple2<Long, Boolean>> selectedInitiators = uniqueVertexIds.map(new SelectInitiators());  // Zufällig nimm 0.00125 der Vertices

    DataSet<Tuple2<Long, Long>> initialForwards =
        selectedInitiators.join(edges).where(0).equalTo(0)
                          .flatMap(new InitialForwards()); // Nimm alle Daten für die Initiators  (Initial Forwards)

    DeltaIteration<Tuple2<Long, Boolean>, Tuple2<Long, Long>> deltaIteration =
        selectedInitiators.iterateDelta(initialForwards, 3, 0); // Itterieren auf initialForwards (bis 3 mal bei Errors) Lies erste Spalte

    DataSet<Tuple1<Long>> deliverMessage =
        deltaIteration.getWorkset().project(1).types(Long.class).distinct(); // getWorkset -> Daten na erster Itteration. 1 Spalte daraus nehmen (Empfänger)

    DataSet<Tuple2<Long, Boolean>> nextRecipientStates = // getSolutionSet --> Nachdem Itteration fertig ist
        deltaIteration.getSolutionSet()
                          .join(deliverMessage).where(0).equalTo(0)
                          .flatMap(new ReceiveMessage());

    DataSet<Tuple2<Long, Long>> nextForwards =        /// Nimm alle edges die processiert wurden
        nextRecipientStates.join(edges).where(0).equalTo(0)
                           .flatMap(new ForwardToFriend());

    DataSet<Tuple2<Long, Boolean>> result =             // RUf die nächste itteration auf
        deltaIteration.closeWith(nextRecipientStates, nextForwards);

    result.print();

    env.execute();
  }

  public static class EdgeReader implements FlatMapFunction<String, Tuple2<Long, Long>> {

    private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

    @Override
    public void flatMap(String s, Collector<Tuple2<Long, Long>> collector) throws Exception {
      if (!s.startsWith("%")) {
        String[] tokens = SEPARATOR.split(s);

        long source = Long.parseLong(tokens[0]);
        long target = Long.parseLong(tokens[1]);

        collector.collect(new Tuple2<Long, Long>(source, target));
      }
    }
  }

  public static class SelectInitiators implements MapFunction<Tuple1<Long>,Tuple2<Long,Boolean>> {

    private final Random random = new Random(Config.randomSeed());

    @Override
    public Tuple2<Long, Boolean> map(Tuple1<Long> vertex) throws Exception {

      boolean isSeedVertex = random.nextDouble() < ChainLetter.INITIATOR_RATIO;
      return new Tuple2<Long, Boolean>(vertex.f0, isSeedVertex);
    }
  }

  public static class InitialForwards implements FlatMapFunction<Tuple2<Tuple2<Long, Boolean>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

    private final Random random = new Random(Config.randomSeed());

    @Override
    public void flatMap(Tuple2<Tuple2<Long, Boolean>, Tuple2<Long, Long>> vertexWithEdge, Collector<Tuple2<Long, Long>> collector) throws Exception {

      Tuple2<Long, Boolean> vertex = vertexWithEdge.f0;
      Tuple2<Long, Long> edge = vertexWithEdge.f1;
      boolean isSeedVertex = vertex.f1;

      if (isSeedVertex && random.nextDouble() < ChainLetter.FORWARDING_PROBABILITY) {
        collector.collect(edge);
      }

    }
  }

  public static class ReceiveMessage implements FlatMapFunction<Tuple2<Tuple2<Long, Boolean>, Tuple1<Long>>, Tuple2<Long, Boolean>> {

    @Override
    public void flatMap(Tuple2<Tuple2<Long, Boolean>, Tuple1<Long>> recipients, Collector<Tuple2<Long, Boolean>> collector) throws Exception {
      Tuple2<Long, Boolean> recipient = recipients.f0;

      boolean alreadyReceived = recipient.f1;
      if (!alreadyReceived) {
        collector.collect(new Tuple2<Long, Boolean>(recipient.f0, true));
      }
    }
  }

  public static class ForwardToFriend implements FlatMapFunction<Tuple2<Tuple2<Long, Boolean>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

    private final Random random = new Random(Config.randomSeed());

    @Override
    public void flatMap(Tuple2<Tuple2<Long, Boolean>, Tuple2<Long, Long>> recipientsAndEdge,
                        Collector<Tuple2<Long, Long>> collector) throws Exception {

      if (random.nextDouble() < ChainLetter.FORWARDING_PROBABILITY) {
        Tuple2<Long, Long> edge = recipientsAndEdge.f1;
        collector.collect(edge);
      }
    }
  }
}
