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


import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.terms.utils.TermUtils;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

/**
 * Tests that the insertion/iteration order of the DownloadTerms sets is preserved and matches the
 * order of the underlying TermUtils lists after applying the same filters.
 */
public class TestTermOrder {

    @Test
    public void downloadInterpretedTermsHdfsOrderPreserved() {
        List<Term> expected = TermUtils.interpretedTerms().stream()
                .filter(t -> !DownloadTerms.EXCLUSIONS_HDFS.contains(t))
                .collect(Collectors.toList());

        List<Term> actual = DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS.stream().collect(Collectors.toList());

        assertIterableEquals(expected, actual, "DOWNLOAD_INTERPRETED_TERMS_HDFS iteration order should match TermUtils.interpretedTerms() after filtering");
    }

    @Test
    public void downloadInterpretedTermsOrderPreserved() {
        List<Term> expected = TermUtils.interpretedTerms().stream()
                .filter(t -> !DownloadTerms.EXCLUSIONS_DOWNLOAD.contains(t))
                .collect(Collectors.toList());

        List<Term> actual = DownloadTerms.DOWNLOAD_INTERPRETED_TERMS.stream().collect(Collectors.toList());

        assertIterableEquals(expected, actual, "DOWNLOAD_INTERPRETED_TERMS iteration order should match TermUtils.interpretedTerms() after filtering");
    }

    @Test
    public void downloadInterpretedTermsWithGbifIdOrderPreserved() {
        List<Term> expected = TermUtils.interpretedTerms().stream()
                .filter(t -> !DownloadTerms.EXCLUSIONS_DWCA_DOWNLOAD.contains(t))
                .collect(Collectors.toList());

        List<Term> actual = DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_WITH_GBIFID.stream().collect(Collectors.toList());

        assertIterableEquals(expected, actual, "DOWNLOAD_INTERPRETED_TERMS_WITH_GBIFID iteration order should match TermUtils.interpretedTerms() after filtering");
    }

    @Test
    public void downloadVerbatimTermsOrderPreserved() {
        List<Term> expected = TermUtils.verbatimTerms().stream()
                .filter(t -> !DownloadTerms.EXCLUSIONS_HDFS.contains(t))
                .collect(Collectors.toList());

        List<Term> actual = DownloadTerms.DOWNLOAD_VERBATIM_TERMS.stream().collect(Collectors.toList());

        assertIterableEquals(expected, actual, "DOWNLOAD_VERBATIM_TERMS iteration order should match TermUtils.verbatimTerms() after filtering");
    }

    @Test
    public void gbifIdAlwaysFirstInWithGbifIdSet() {
        Term first = DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_WITH_GBIFID.iterator().next();
        assertEquals(GbifTerm.gbifID, first, "GbifTerm.gbifID should be the first term in DOWNLOAD_INTERPRETED_TERMS_WITH_GBIFID");
    }
}
