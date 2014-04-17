package com.cloudera.sa.examples.crunch;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.io.File;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.FileUtils;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.sa.examples.EmployeeRecord;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Fields;

public class MorphlineDoFn
  extends DoFn< String, EmployeeRecord > implements Command
{
  private static final Logger LOGGER = LoggerFactory.getLogger( MorphlineDoFn.class );

  /*
   * Why are the objects below static, you ask?
   *
   * So, turns out the note below from Josh wasn't pointless -
   *   One place where the serializable DoFns can trip up new Crunch developers is when they specify in-line
   *   DoFns inside of methods of non-serializable outer classes. Although their pipelines compile fine and
   *   will execute locally with Crunch's MemPipeline, the MRPipeline or SparkPipeline versions will fail with
   *   Java's NotSerializableException.
   */
  private static Command morphline;
  private static EmployeeRecord employeeRecord;
  private static MorphlineContext morphlineContext;

  final static String morphLineId = "parse_employee_record";
  private static final Record record = new Record();

  public MorphlineDoFn() throws Exception {
    setup();
  }

  private void setup() throws Exception {
    URL morphlineURL   = getClass().getResource("/morphline/parse-employee-record.conf");

    File morphlineFile = File.createTempFile(
        FilenameUtils.getBaseName( morphlineURL.getFile() )
      , FilenameUtils.getExtension( morphlineURL.getFile() )
    );
    IOUtils.copy( morphlineURL.openStream(), FileUtils.openOutputStream( morphlineFile ) );

    morphlineContext = new MorphlineContext.Builder().build();

    // TODO: file Jira, compiler should accept an inputStream/inputFile
    morphline = new Compiler().compile(
        morphlineFile, morphLineId, morphlineContext, this /* LastCommand */ );
  }

  @Override
  public void process( String line, Emitter< EmployeeRecord > emitter ) {

    record.removeAll( Fields.ATTACHMENT_BODY );
    record.put( Fields.ATTACHMENT_BODY, new ByteArrayInputStream(line.toString().getBytes()) );

    if( !morphline.process(record) ) {
      LOGGER.error( "Unable to process record: {}", line );
      return;
    }
    emitter.emit( employeeRecord );
  }

  @Override
  public void notify(Record notification) {
  }

  @Override
  public Command getParent() {
    return null;
  }

  @Override
  public boolean process(Record record) {
    LOGGER.info( "Record received: {}", record );

    employeeRecord = null;
    return true;
  }
}