package org.gbif.occurrence.download.hive;

import org.apache.avro.Schema;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.ObisTerm;
import org.gbif.dwc.terms.Term;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EventTableDefinitionTest {

  @Test
  public void eventTableDefinitionTest() {
    EventHDFSTableDefinition.definition().forEach(f -> System.out.println(f.getColumnName()));
  }

  @Test
  public void newTermsTest() {
    assertContainsTerm(DwcTerm.fundingAttribution);
    assertContainsTerm(DwcTerm.projectID);
    assertContainsTerm(DwcTerm.measurementType);
    assertContainsTerm(ObisTerm.measurementTypeID);
    assertTermDataType(GbifInternalTerm.datasetCategory, HiveDataTypes.TYPE_ARRAY_STRING);
    assertAvroArrayStringField(EventAvroHdfsTableDefinition.avroDefinition(), GbifInternalTerm.datasetCategory);
  }

  private void assertContainsTerm(Term term) {
    Assertions.assertTrue(
        EventHDFSTableDefinition.definition().stream()
            .anyMatch(t -> t.getColumnName().equals(term.simpleName().toLowerCase())));
  }

  private void assertTermDataType(Term term, String dataType) {
    Assertions.assertTrue(
        EventHDFSTableDefinition.definition().stream()
            .anyMatch(t -> t.getColumnName().equals(term.simpleName().toLowerCase())
                && t.getHiveDataType().equals(dataType)));
  }

  private void assertAvroArrayStringField(Schema schema, Term term) {
    Schema fieldSchema = schema.getField(term.simpleName().toLowerCase()).schema();
    Assertions.assertTrue(
        fieldSchema.getTypes().stream()
            .anyMatch(type -> type.getType() == Schema.Type.ARRAY && isNullableString(type.getElementType())));
  }

  private boolean isNullableString(Schema schema) {
    return schema.getType() == Schema.Type.STRING
        || schema.getType() == Schema.Type.UNION
            && schema.getTypes().stream().anyMatch(type -> type.getType() == Schema.Type.STRING);
  }
}
