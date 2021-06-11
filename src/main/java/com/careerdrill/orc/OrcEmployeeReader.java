package com.careerdrill.orc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class OrcEmployeeReader {
    private static final int BATCH_SIZE = 2048;

    public static void main(String[] args) throws IOException {
        List<Map<String, Object>> rows = read(new Configuration(), "employee.orc");
        for (Map<String, Object> row : rows) {
            System.out.println(row);
        }
    }

    public static List<Map<String, Object>> read(Configuration configuration, String path)
            throws IOException {
        List<Map<String, Object>> rows = new LinkedList<>();

        try (Reader reader = OrcFile.createReader(new Path(path), OrcFile.readerOptions(configuration))) {


            try (RecordReader records = reader.rows(reader.options())) {

                VectorizedRowBatch batch = reader.getSchema().createRowBatch(BATCH_SIZE);
                LongColumnVector idColumnVector = (LongColumnVector) batch.cols[0];
                BytesColumnVector nameColumnVector = (BytesColumnVector) batch.cols[1];
                LongColumnVector ageColumnVector = (LongColumnVector) batch.cols[2];

                while (records.nextBatch(batch)) {
                    for (int rowNum = 0; rowNum < batch.size; rowNum++) {

                        Map<String, Object> map = new HashMap<>();
                        map.put("employee_id", idColumnVector.vector[rowNum]);
                        map.put("name", nameColumnVector.toString(rowNum));
                        map.put("age", ageColumnVector.vector[rowNum]);
                        rows.add(map);
                    }
                }
            }
        }
        return rows;
    }
}