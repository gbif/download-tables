/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.Term;

import java.util.function.Predicate;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OccurrenceAvroHdfsTableDefinitionTest {

  @Test
  public void avroSchemaTest() {
    System.out.println(OccurrenceAvroHdfsTableDefinition.avroDefinition());
  }

  @Test
  public void newTermsTest() {
    assertAvroArrayStringField(OccurrenceAvroHdfsTableDefinition.avroDefinition(), GbifInternalTerm.datasetCategory);
  }

  @Test
  public void dnaTermsTest() {
    Schema avroSchema = OccurrenceAvroHdfsTableDefinition.avroDefinition();

    assertAvroMapField(avroSchema, GbifInternalTerm.dnaClassifications, this::isStringArray);
    assertAvroMapField(avroSchema, GbifInternalTerm.dnaTaxonomicStatuses, this::isString);
    assertAvroMapField(avroSchema, GbifInternalTerm.dnaTaxonomicIssues, this::isStringArray);
    assertAvroMapField(
      avroSchema,
      GbifInternalTerm.dnaClassificationDetails,
      values -> values.getType() == Schema.Type.MAP && isString(values.getValueType()));
  }

  private void assertAvroArrayStringField(Schema schema, Term term) {
    Schema fieldSchema = schema.getField(term.simpleName().toLowerCase()).schema();
    Assertions.assertTrue(
      fieldSchema.getTypes().stream()
        .anyMatch(type -> type.getType() == Schema.Type.ARRAY && isNullableString(type.getElementType())));
  }

  private void assertAvroMapField(Schema schema, Term term, Predicate<Schema> valuesCheck) {
    Schema.Field field = schema.getField(term.simpleName().toLowerCase());
    Assertions.assertNotNull(field, "Missing Avro field for " + term.simpleName());
    Assertions.assertTrue(
      field.schema().getTypes().stream()
        .anyMatch(type -> type.getType() == Schema.Type.MAP && valuesCheck.test(type.getValueType())));
  }

  private boolean isString(Schema schema) {
    return schema.getType() == Schema.Type.STRING;
  }

  private boolean isStringArray(Schema schema) {
    return schema.getType() == Schema.Type.ARRAY && isString(schema.getElementType());
  }

  private boolean isNullableString(Schema schema) {
    return schema.getType() == Schema.Type.STRING
      || schema.getType() == Schema.Type.UNION
        && schema.getTypes().stream().anyMatch(type -> type.getType() == Schema.Type.STRING);
  }
}
