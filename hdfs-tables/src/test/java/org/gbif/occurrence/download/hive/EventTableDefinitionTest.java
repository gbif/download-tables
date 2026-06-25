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
  }

  private void assertContainsTerm(Term term) {
    Assertions.assertTrue(
        EventHDFSTableDefinition.definition().stream()
            .anyMatch(t -> t.getColumnName().equals(term.simpleName().toLowerCase())));
  }
}
