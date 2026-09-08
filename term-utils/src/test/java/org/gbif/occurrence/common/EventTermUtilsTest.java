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
package org.gbif.occurrence.common;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.ObisTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.terms.utils.EventTermUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
public class EventTermUtilsTest {

  @Test
  public void testIsInterpretedSourceTerm() throws Exception {
    assertTrue(EventTermUtils.isInterpretedSourceTerm(DwcTerm.country));
    assertTrue(EventTermUtils.isInterpretedSourceTerm(DwcTerm.countryCode));
    assertTrue(EventTermUtils.isInterpretedSourceTerm(DwcTerm.eventDate));
    assertFalse(EventTermUtils.isInterpretedSourceTerm(DwcTerm.occurrenceID));
    assertFalse(EventTermUtils.isInterpretedSourceTerm(DwcTerm.catalogNumber));
  }

  @Test
  public void testInterpretedTerms() throws Exception {
    System.out.println("\n" + "\nINTERPRETED TERMS");
    Set<Term> terms = new HashSet<>();
    for (Term t : EventTermUtils.interpretedTerms()) {
      System.out.println(t.toString());
      assertFalse(terms.contains(t), "Interpreted term exists twice: " + t);
      terms.add(t);
    }
  }

  @Test
  public void testVerbatimTerms() throws Exception {
    System.out.println("\n\nVERBATIM TERMS");
    Set<Term> terms = new HashSet<>();
    for (Term t : EventTermUtils.verbatimTerms()) {
      System.out.println(t.toString());
      assertFalse(terms.contains(t), "Verbatim term exists twice: " + t);
      terms.add(t);
    }
  }

  @Test
  public void interpretedTermsKeepTheirCurrentOrder() {
    assertInOrder(
        List.copyOf(EventTermUtils.interpretedTerms()),
        GbifTerm.gbifID,
        DcTerm.accessRights,
        DcTerm.bibliographicCitation,
        DcTerm.language,
        DcTerm.publisher,
        DcTerm.references,
        DcTerm.rightsHolder,
        DcTerm.type,
        DwcTerm.decimalLatitude,
        GbifTerm.datasetKey,
        GbifTerm.publishingCountry,
        GbifTerm.lastInterpreted,
        DcTerm.modified,
        GbifTerm.depth,
        GbifTerm.issue,
        DwcTerm.projectID,
        DwcTerm.eventType,
        DwcTerm.measurementType,
        ObisTerm.measurementTypeID);
  }

  private static void assertInOrder(List<Term> actual, Term... expectedOrder) {
    int previousIndex = -1;
    for (Term term : expectedOrder) {
      int index = actual.indexOf(term);
      assertTrue(index >= 0, "Missing term: " + term);
      assertTrue(index > previousIndex, "Wrong order for term: " + term);
      previousIndex = index;
    }
  }
}
