package de.tuberlin.dima.aim3.assigntment4bonus;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;

/**
 * Created by vassil on 23.06.15.
 */

public class matrixCalc {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Input should be in this form:
        // rowId, colId, value

        String m1PathCSV = "/home/vassil/Documents/Studium/Master/AIM3/aim3/src/main/java/de/tuberlin/dima/aim3/assigntment4bonus/matrix1.csv";
        String m2PathCSV = "/home/vassil/Documents/Studium/Master/AIM3/aim3/src/main/java/de/tuberlin/dima/aim3/assigntment4bonus/matrix2.csv";

        String path =  "/home/vassil/Documents/Studium/Master/AIM3/aim3/src/main/java/de/tuberlin/dima/aim3/assigntment4bonus/output/";

        DataSource<Tuple3<Integer, Integer, Integer>> matrix1 = readMatrix(env, m1PathCSV);
        DataSource<Tuple3<Integer, Integer, Integer>> matrix2 = readMatrix(env, m2PathCSV);

        matrix1.join(matrix2).where(0).equalTo(1)
                .map(new MatrixMultiplicationMap()).groupBy(0,1).sum(2)
                .writeAsCsv(path, FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }

    public static final class MatrixMultiplicationMap implements
            MapFunction<Tuple2<Tuple3<Integer, Integer, Integer>,
                    Tuple3<Integer, Integer, Integer>>,
                    Tuple3<Integer, Integer, Integer>> {
        @Override
        public Tuple3<Integer, Integer, Integer> map(
                Tuple2<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> value)
                throws Exception {
            Integer rowId = value.f0.f0;
            Integer columnId = value.f1.f1;
            Integer newValue = value.f0.f2 * value.f1.f2;
            return new Tuple3<Integer, Integer, Integer>(rowId, columnId, newValue);
        }
    }

    public static DataSource<Tuple3<Integer, Integer, Integer>> readMatrix(ExecutionEnvironment env, String filePath) {
        CsvReader csvReader = env.readCsvFile(filePath);
        csvReader.fieldDelimiter(',');
        csvReader.includeFields("fttt");
        return csvReader.types(Integer.class, Integer.class, Integer.class);
    }


}
