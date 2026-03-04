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

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.ObisTerm;
import org.gbif.dwc.terms.Term;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OccurrenceAvroHdfsTableDefinitionTest {

  @Test
  public void avroSchemaTest() {
    System.out.println(OccurrenceAvroHdfsTableDefinition.avroDefinition());
  }

  @Test
  public void hiveSchemaTest() {
    System.out.println(OccurrenceHDFSTableDefinition.definition());
  }

  @Test
  public void newTermsTest() {
    assertNotContainsTerm(DwcTerm.fundingAttribution);
    assertContainsColumn("v_" + DwcTerm.fundingAttribution.simpleName().toLowerCase());
    assertContainsTerm(DwcTerm.projectID);
    assertContainsTerm(GbifTerm.dnaSequenceID);
    assertContainsTerm(DwcTerm.measurementType);
    assertContainsTerm(ObisTerm.measurementTypeID);
  }

  private void assertContainsTerm(Term term) {
    Assertions.assertTrue(
      OccurrenceHDFSTableDefinition.definition().stream()
        .anyMatch(t -> t.getColumnName().equals(term.simpleName().toLowerCase())));
  }

  private void assertNotContainsTerm(Term term) {
    Assertions.assertTrue(
      OccurrenceHDFSTableDefinition.definition().stream()
        .noneMatch(t -> t.getColumnName().equals(term.simpleName().toLowerCase())));
  }

  private void assertContainsColumn(String column) {
    Assertions.assertTrue(
      OccurrenceHDFSTableDefinition.definition().stream()
        .anyMatch(t -> t.getColumnName().equals(column)));
  }
}
