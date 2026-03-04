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

import org.apache.commons.lang3.tuple.Pair;
import org.gbif.dwc.terms.*;
import org.gbif.terms.utils.EventTermUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.gbif.occurrence.download.hive.DownloadTerms.Group;

/**
 * Definitions of terms used in downloading, and in create tables used during the download process.
 */
public class EventDownloadTerms {

  /** Terms not used in HDFS or downloads. */
  private static final Set<Term> EXCLUSIONS_INTERPRETED =
      Set.of(
          GbifTerm.gbifID, // returned multiple times, so excluded and treated by adding once at the
          // beginning
          GbifInternalTerm.fragmentHash, // omitted entirely
          GbifInternalTerm.fragment, // omitted entirely
          GbifInternalTerm.humboldtEventDurationValueInMinutes);

  /** This set is used for the HDFS table definition */
  public static final Set<Term> EXCLUSIONS_HDFS =
      Stream.concat(EXCLUSIONS_INTERPRETED.stream(), Stream.of(GbifTerm.verbatimScientificName)).collect(Collectors.toSet());;

  /** Terms present in HDFS but not included in a DWCA download */
  private static final Set<Term> EXCLUSIONS_DWCA_DOWNLOAD =
          Set.of(
          GbifTerm.geologicalTime,
          GbifTerm.lithostratigraphy,
          GbifTerm.biostratigraphy,
          GbifTerm.dnaSequenceID,
          GbifTerm.checklistKey,
          DwcTerm.measurementType,
          ObisTerm.measurementTypeID);

  /** Terms not used in DWCA downloads. */
  private static final Set<Term> EXCLUSIONS_DOWNLOAD =
          Stream.concat(EXCLUSIONS_INTERPRETED.stream(),
                  EXCLUSIONS_DWCA_DOWNLOAD.stream()).collect(Collectors.toSet());

  private static Set<Term> difference(List<Term> source, Set<Term> exclusions) {
    return source.stream()
            .filter(t -> !exclusions.contains(t))
            .collect(Collectors.toUnmodifiableSet());
  }

  public static final Set<Term> DOWNLOAD_INTERPRETED_TERMS_HDFS =
          difference(EventTermUtils.interpretedTerms(), EXCLUSIONS_HDFS);

  public static final Set<Term> DOWNLOAD_INTERNAL_TERMS_HDFS =
          difference(EventTermUtils.internalTerms(), EXCLUSIONS_HDFS);

  /** The interpreted terms included in a DWCA download. */
  public static final Set<Term> DOWNLOAD_INTERPRETED_TERMS =
          difference(EventTermUtils.interpretedTerms(), EXCLUSIONS_DOWNLOAD);

  /** The interpreted terms included in a DWCA download, with GBIFID first. */
  public static final Set<Term> DOWNLOAD_INTERPRETED_TERMS_WITH_GBIFID =
          difference(EventTermUtils.interpretedTerms(), EXCLUSIONS_DWCA_DOWNLOAD);

  /*
   * The verbatim terms included in a DWCA download.
   */
  public static final Set<Term> DOWNLOAD_VERBATIM_TERMS =
        difference(EventTermUtils.verbatimTerms(), EXCLUSIONS_HDFS);

  public static final Set<Pair<Group, Term>> SIMPLE_DOWNLOAD_TERMS =
      Set.of(
          Pair.of(Group.INTERPRETED, GbifTerm.gbifID),
          Pair.of(Group.INTERPRETED, GbifTerm.datasetKey),
          Pair.of(Group.INTERPRETED, DwcTerm.eventID),
          Pair.of(Group.INTERPRETED, DwcTerm.parentEventID),
          Pair.of(Group.INTERPRETED, DwcTerm.countryCode),
          Pair.of(Group.INTERPRETED, DwcTerm.locality),
          Pair.of(Group.INTERPRETED, DwcTerm.stateProvince),
          Pair.of(Group.INTERPRETED, GbifInternalTerm.publishingOrgKey),
          Pair.of(Group.INTERPRETED, DwcTerm.decimalLatitude),
          Pair.of(Group.INTERPRETED, DwcTerm.decimalLongitude),
          Pair.of(Group.INTERPRETED, DwcTerm.coordinateUncertaintyInMeters),
          Pair.of(Group.INTERPRETED, DwcTerm.coordinatePrecision),
          Pair.of(Group.INTERPRETED, GbifTerm.elevation),
          Pair.of(Group.INTERPRETED, GbifTerm.elevationAccuracy),
          Pair.of(Group.INTERPRETED, GbifTerm.depth),
          Pair.of(Group.INTERPRETED, GbifTerm.depthAccuracy),
          Pair.of(Group.INTERPRETED, DwcTerm.eventDate),
          Pair.of(Group.INTERPRETED, DwcTerm.day),
          Pair.of(Group.INTERPRETED, DwcTerm.month),
          Pair.of(Group.INTERPRETED, DwcTerm.year),
          Pair.of(Group.INTERPRETED, DwcTerm.institutionCode),
          Pair.of(Group.INTERPRETED, DwcTerm.collectionCode),
          Pair.of(Group.INTERPRETED, DwcTerm.dateIdentified),
          Pair.of(Group.INTERPRETED, DcTerm.license),
          Pair.of(Group.INTERPRETED, DcTerm.rightsHolder),
          Pair.of(Group.INTERPRETED, GbifTerm.lastInterpreted),
          Pair.of(Group.INTERPRETED, GbifTerm.mediaType),
          Pair.of(Group.INTERPRETED, GbifTerm.issue));


  public static String simpleName(Pair<Group, Term> termPair) {
    Term term = termPair.getRight();
    if (termPair.getLeft().equals(Group.VERBATIM)) {
      return "verbatim"
          + Character.toUpperCase(term.simpleName().charAt(0))
          + term.simpleName().substring(1);
    } else {
      return term.simpleName();
    }
  }
}
