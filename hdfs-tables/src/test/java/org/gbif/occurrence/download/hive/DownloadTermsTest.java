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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DownloadTermsTest {

  @Test
  public void newEventTermsTest() {
    Assertions.assertTrue(
        DownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(DwcTerm.fundingAttribution));
    Assertions.assertTrue(
        EventDownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(DwcTerm.fundingAttribution));
    Assertions.assertFalse(DownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(GbifTerm.dnaSequenceID));
    Assertions.assertFalse(DownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(DwcTerm.measurementType));
    Assertions.assertFalse(
        DownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(ObisTerm.measurementTypeID));
    Assertions.assertFalse(
        EventDownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(DwcTerm.measurementType));
    Assertions.assertFalse(
        EventDownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(ObisTerm.measurementTypeID));
  }
}
