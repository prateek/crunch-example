package com.cloudera.sa.examples.crunch;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.crunch.impl.mem.emit.InMemoryEmitter;
import com.cloudera.sa.examples.EmployeeRecord;
import com.google.common.collect.ImmutableList;

public class MorphlineDoFnTest {

  @Test
  public void testDoFn() {
    String inputRecord = "1,john,22,4000,4,Hooligan,Theatre";
    EmployeeRecord expectedRecord
      = EmployeeRecord.newBuilder().setId("1").setName("john")
        .setAge( 22 ).setSalary( 4000.0 ).setYearsSpent( 4 )
        .setTitle( "Hooligan" ).setDepartment( "Theatre" ).build();

    InMemoryEmitter< EmployeeRecord > emitter = new InMemoryEmitter<EmployeeRecord>();
    MorphlineDoFn< EmployeeRecord > doFn = new MorphlineDoFn< EmployeeRecord >(
      "parse_employee_record", "/morphline/parse-employee-record.conf", EmployeeRecord.class
    );
    doFn.initialize();
    doFn.process(inputRecord, emitter);

    assertEquals(ImmutableList.of( expectedRecord ), emitter.getOutput());
  }
}
