package com.careerdrill.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class OrcEmployeeWriter {
    public static void main(String[] args) throws IOException {
        List<Map<String, Object>> data = new LinkedList<>();
        data.add(Map.of("employee_id", 1, "name", "Siva", "age", 30));
        data.add(Map.of("employee_id", 2, "name", "Selvi", "age", 27));
        data.add(Map.of("employee_id", 3, "name", "Kavin", "age", 11));


        java.nio.file.Path fileToDeletePath = Paths.get("employee.orc");
        Files.delete(fileToDeletePath);

        Configuration configuration = new Configuration();
        configuration.set(String.valueOf(OrcConf.COMPRESS), "SNAPPY");
        write(configuration, "employee.orc", "struct<employee_id:int,name:string,age:int>", data);

        System.out.println("Done");
    }

    public static void write(Configuration configuration, String path, String struct, List<Map<String, Object>> data) throws IOException {

        TypeDescription schema = TypeDescription.fromString(struct);


        VectorizedRowBatch batch = schema.createRowBatch();


        LongColumnVector employeeIdColumnVector = (LongColumnVector) batch.cols[0];
        BytesColumnVector nameColumnVector = (BytesColumnVector) batch.cols[1];
        LongColumnVector ageColumnVector = (LongColumnVector) batch.cols[2];


        try (Writer writer = OrcFile.createWriter(new Path(path),
                OrcFile.writerOptions(configuration)
                        .setSchema(schema))) {
            for (Map<String, Object> row : data) {
                int rowNum = batch.size++;

                employeeIdColumnVector.vector[rowNum] = (Integer) row.get("employee_id");
                byte[] buffer = row.get("name").toString().getBytes(StandardCharsets.UTF_8);
                nameColumnVector.setRef(rowNum, buffer, 0, buffer.length);
                ageColumnVector.vector[rowNum] = (Integer) row.get("age");

                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }

            if (batch.size != 0) {
                writer.addRowBatch(batch);
            }
        }
    }
}