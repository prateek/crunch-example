package com.cloudera.sa.examples.crunch;

import java.net.URL;
import java.io.File;

import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.crunch.Target;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.io.At;
import org.apache.crunch.io.From;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.util.ToolRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaBuilder;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.base.Compiler;

import com.cloudera.sa.examples.EmployeeRecord;
import com.cloudera.sa.examples.EmployeeSummary;
import com.cloudera.sa.examples.crunch.MorphlineDoFn;

public class GenerateSummaries extends CrunchTool {

  @Override
  public int run(String[] args) throws Exception {

    if( args.length != 1 ) {
      System.out.println("Usage: GenerateSummaries <input-path> <output-path>");
      System.out.println();

      System.exit( -1 );
    }

    String[] tokens = args[0].trim().split(" ");

    /* debugging flags */
    getPipeline().enableDebug();
    getPipeline().getConfiguration().set( "crunch.log.job.progress", "true" );

    PCollection<String> lines = getPipeline().read( From.textFile( tokens[0] ) );
    PTable<String, Double> summaries = lines
        // convert PCollection< String > -> PCollection< EmployeeRecord >
        .parallelDo( "GenerateAvroRecords", new MorphlineDoFn(), Avros.reflects(EmployeeRecord.class) )

        // convert PCollection<EmployeeRecord> -> PTable< Department, EmployeeRecord >
        .by("ExtractDepartment",
            new MapFn< EmployeeRecord, String > () {
              public String map( EmployeeRecord record ) {
                return record.getDepartment();
              }
            }, Avros.strings())

        // extract salary from values
        // i.e. PTable< Department, EmployeeRecord > -> PTable< Department, Pair< Salary, 1 > >
        .mapValues( "ExtractSalaries",
            new MapFn< EmployeeRecord, Pair< Double, Long > >() {
              public Pair<Double, Long> map( EmployeeRecord e ) {
                return Pair.of( e.getSalary(), 1L);
              }
            },
            Avros.pairs(Avros.doubles(), Avros.longs()))

        // group all the department records
        // PTable< Department, Pair< Salary, 1> > -> PGroupedTable< Department, Iterable< Pair<..> >;
        .groupByKey()

        // perform aggregation
        // PGroupedTable -> PTable< String, Pair< Double, Long > >
        .combineValues(
            Aggregators.pairAggregator( Aggregators.SUM_DOUBLES(), Aggregators.SUM_LONGS()))

        // compute averages
        .mapValues( "ComputeAverage",
            new MapFn< Pair< Double, Long >, Double >() {
              public Double map( Pair< Double, Long > p ) {
                return p.first() / p.second();
              }
            },
            Avros.doubles());

    summaries.write(
        At.avroFile(tokens[1], EmployeeSummary.class), Target.WriteMode.APPEND );

    return run().succeeded() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new GenerateSummaries(), args);
    System.exit(rc);
  }

}
