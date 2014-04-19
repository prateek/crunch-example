package com.cloudera.sa.examples.crunch;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.io.File;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.FileUtils;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Fields;

import com.cloudera.sa.examples.EmployeeRecord;

public class MorphlineDoFn
  extends DoFn< String, EmployeeRecord > implements Command
{
  private static final Logger LOGGER = LoggerFactory.getLogger( MorphlineDoFn.class );

  /*
   * Why are the objects below transient, you ask?
   *
   * So, turns out the note below from Josh wasn't pointless -
   *   One place where the serializable DoFns can trip up new Crunch developers is when they specify in-line
   *   DoFns inside of methods of non-serializable outer classes. Although their pipelines compile fine and
   *   will execute locally with Crunch's MemPipeline, the MRPipeline or SparkPipeline versions will fail with
   *   Java's NotSerializableException.
   */
  private transient Command morphline;
  private transient EmployeeRecord employeeRecord;
  private transient MorphlineContext morphlineContext;

  // temporary var used to sync state across Morphline and
  // Crunch's DoFn calls.
  private transient Record record ;
  final static String morphLineId = "parse_employee_record";

  public MorphlineDoFn() {
  }

  private void setup() throws Exception {
    record = new Record();
    morphlineContext = new MorphlineContext.Builder().build();
    URL morphlineURL   = getClass().getResource("/morphline/parse-employee-record.conf");

    File morphlineFile = File.createTempFile(
        FilenameUtils.getBaseName( morphlineURL.getFile() )
      , FilenameUtils.getExtension( morphlineURL.getFile() )
    );
    IOUtils.copy( morphlineURL.openStream(), FileUtils.openOutputStream( morphlineFile ) );

    // TODO: file JIRA, Compiler api should accept an InputStream
    morphline = new Compiler().compile(
        morphlineFile, morphLineId, morphlineContext, this /* LastCommand */ );
  }

  @Override
  public void initialize() {
    try {
      setup();
    } catch(Exception e){
      LOGGER.error( "Exception: {}", e );
    }
  }

  @Override
  public void process( String line, Emitter< EmployeeRecord > emitter ) {

    record.removeAll( Fields.ATTACHMENT_BODY );
    record.put( Fields.ATTACHMENT_BODY, new ByteArrayInputStream(line.toString().getBytes()) );

    if( ! morphline.process(record) ) {
      LOGGER.error( "Unable to process record: {}", line );
      return;
    }

    // the process command above parses the record
    // and stores it into the employeeRecord object
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
    employeeRecord = null;
    LOGGER.debug( "Record received: {}", record );

    List fields = record.get( Fields.ATTACHMENT_BODY ) ;
    if( fields.size() != 1 ) {
      LOGGER.error( "Record [ {} ] had incorrect number of fields - [{}]", record, fields.size() );
      return false;
    }

    try {
      byte[] byteArray = (byte[]) fields.get( 0 );
      SeekableByteArrayInput inputStream = new SeekableByteArrayInput( byteArray );

      DatumReader<EmployeeRecord> userDatumReader
        = new SpecificDatumReader<EmployeeRecord>(EmployeeRecord.class);

      DataFileReader<EmployeeRecord> dataFileReader
        = new DataFileReader<EmployeeRecord>(inputStream, userDatumReader);

      employeeRecord = dataFileReader.next();

    } catch(Exception e){
      LOGGER.error( "Unable to process {}, exception: {}", record, e);
      return false;
    }

    return true;
  }
}
