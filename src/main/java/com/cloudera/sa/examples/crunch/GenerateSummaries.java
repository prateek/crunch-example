package com.cloudera.sa.examples.crunch;

import org.apache.crunch.PCollection;
import org.apache.crunch.Target;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.util.ToolRunner;

import org.kitesdk.data.crunch.CrunchDatasets;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.Formats;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.sa.examples.EmployeeRecord;

public class GenerateSummaries extends CrunchTool {

  @Override
  public int run(String[] args) throws Exception {

    if( args.length != 1 ) {
      System.out.println("Usage: GenerateSummaries <employee-records-location> ");
      System.out.println("\n\nExample: mvn kite:run-tool -Dkite.args=\"repo:hdfs:/tmp/temp-repo\"");
      System.exit( -1 );
    }

    /* debugging flags */
    getPipeline().enableDebug();
    getPipeline().getConfiguration().set( "crunch.log.job.progress", "true" );

    DatasetRepository temp_fs_repo = DatasetRepositories.open( args[0] );

    if( !temp_fs_repo.exists( "employee_records" ) ) {
      Schema employee_record_schema = SchemaBuilder.record("EmployeeRecord")
          .fields()
          .name("id").type().stringType().noDefault()
          .name("name").type().stringType().noDefault()
          .name("age").type().intType().noDefault()
          .name("salary").type().doubleType().noDefault()
          .name("years_spent").type().intType().noDefault()
          .name("title").type().stringType().noDefault()
          .name("department").type().stringType().noDefault()
          .endRecord();

      temp_fs_repo.create("employee_records", new DatasetDescriptor.Builder()
          // FIXME: descriptor doesn't take location as arg yet -- .location( args[0] )
          .format(Formats.CSV)
          .schema(employee_record_schema)
          .build());
    }

    
    DatasetReader reader = temp_fs_repo.load( "employee_records" ).newReader();
    try {
      reader.open();
      for (Object rec : reader) {
        System.err.println("Movie: " + rec);
      }
    } finally {
      reader.close();
    }

    return 0;
  }

  /*
    Dataset<Summary> summariesDataset = repo.load("summaries");

    PCollection<Event> events = read(
        CrunchDatasets.asSource(eventsDataset, Event.class));

    PCollection<Summary> summaries = events
        .by(new GetTimeAndSourceBucket(),
            Avros.pairs(Avros.longs(), Avros.strings()))
        .groupByKey()
        .parallelDo(new MakeSummary(),
            Avros.reflects(Summary.class));

    getPipeline().write(summaries,
        CrunchDatasets.asTarget(summariesDataset),
        Target.WriteMode.APPEND);

    return run().succeeded() ? 0 : 1;
        */

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new GenerateSummaries(), args);
    System.exit(rc);
  }
}
