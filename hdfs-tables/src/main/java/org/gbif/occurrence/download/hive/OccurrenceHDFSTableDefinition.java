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

import static org.gbif.occurrence.download.hive.HiveColumns.cleanDelimitersArrayInitializer;
import static org.gbif.occurrence.download.hive.HiveColumns.cleanDelimitersInitializer;
import static org.gbif.occurrence.download.hive.HiveColumns.columnFor;
import static org.gbif.occurrence.download.hive.HiveColumns.getVerbatimColPrefix;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.*;

/**
 * This provides the definition required to construct the occurrence HDFS table, for use as a Hive table.
 * The table is populated by a query which scans the Avro files, but along the way converts some fields to
 * e.g., Hive arrays which require some UDF voodoo captured here.
 * <p/>
 * Note to developers: It is not easy to find a perfectly clean solution to this work.  Here we try and favour long
 * term code management over all else.  For that reason, Functional programming idioms are not used even though they
 * would reduce lines of code.  However, they come at a cost in that there are several levels of method chaining
 * here, and it becomes difficult to follow as they are not so intuitive on first glance.  Similarly, we don't attempt
 * to push all complexities into the freemarker templates (e.g. complex UDF chaining) as it becomes unmanageable.
 * <p/>
 * Developers please adhere to the above design goals when modifying this class, and consider developing for simple
 * maintenance.
 */
@UtilityClass
public class OccurrenceHDFSTableDefinition {

  public static void main(String[] args) {
    System.out.println(
      "CREATE TABLE IF NOT EXISTS occurrence (\n"
        + OccurrenceHDFSTableDefinition.definition().stream()
        .map(field -> field.getHiveField() + " " + field.getHiveDataType())
        .collect(Collectors.joining(", \n"))
        + ") STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")");
  }

  private static final Set<Term> ARRAYS_FROM_VERBATIM_VALUES =
      Set.of(
          DwcTerm.recordedByID,
          DwcTerm.identifiedByID,
          DwcTerm.datasetID,
          DwcTerm.datasetName,
          DwcTerm.recordedBy,
          DwcTerm.identifiedBy,
          DwcTerm.otherCatalogNumbers,
          DwcTerm.preparations,
          DwcTerm.samplingProtocol,
          DwcTerm.higherGeography,
          DwcTerm.georeferencedBy,
          DwcTerm.associatedSequences);

  /**
   * Assemble the mapping for verbatim fields.
   *
   * @return the list of fields that are used in the verbatim context
   */
  private static List<InitializableField> verbatimFields() {
    return DownloadTerms.DOWNLOAD_VERBATIM_TERMS.stream()
            .map(OccurrenceHDFSTableDefinition::verbatimField)
            .collect(Collectors.toList());
  }

  /**
   * Assemble the mapping for interpreted fields, taking note that in reality, many are mounted onto the verbatim
   * columns.
   *
   * @return the list of fields that are used in the interpreted context
   */
  private static List<InitializableField> interpretedFields() {

    // the following terms are manipulated when transposing from Avro to hive by using UDFs and custom HQL
    Map<Term, String> initializers = Map.ofEntries(
            Map.entry(GbifTerm.datasetKey, columnFor(GbifTerm.datasetKey)),
            Map.entry(GbifTerm.protocol, columnFor(GbifTerm.protocol)),
            Map.entry(GbifTerm.publishingCountry, columnFor(GbifTerm.publishingCountry)),
            Map.entry(DwcTerm.eventType, columnFor(DwcTerm.eventType)),
            Map.entry(IucnTerm.iucnRedListCategory, columnFor(IucnTerm.iucnRedListCategory)),
            Map.entry(GbifInternalTerm.classifications, columnFor(GbifInternalTerm.classifications)),
            Map.entry(GbifInternalTerm.taxonomicStatuses, columnFor(GbifInternalTerm.taxonomicStatuses)),
            Map.entry(GbifInternalTerm.classificationDetails, columnFor(GbifInternalTerm.classificationDetails)),
            Map.entry(GbifTerm.checklistKey, columnFor(GbifTerm.checklistKey))
    );

    List<InitializableField> result = new ArrayList<>();

    for (var term : DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS) {
      var initializer = initializers.get(term);
      if (initializer != null) {
        result.add(interpretedField(term, initializer));
      } else {
        result.add(interpretedField(term));
      }
    }

    return result;
  }


  /**
   * The internal fields stored in Avro which we wish to expose through Hive.  The fragment and fragment hash
   * are removed and not present.
   *
   * @return the list of fields that are exposed through Hive
   */
  private static List<InitializableField> internalFields() {

    Map<Term, String> initializers = Map.ofEntries(
            Map.entry(GbifInternalTerm.publishingOrgKey, columnFor(GbifInternalTerm.publishingOrgKey)),
            Map.entry(GbifInternalTerm.installationKey, columnFor(GbifInternalTerm.installationKey)),
            Map.entry(GbifInternalTerm.institutionKey, columnFor(GbifInternalTerm.institutionKey)),
            Map.entry(GbifInternalTerm.collectionKey, columnFor(GbifInternalTerm.collectionKey)),
            Map.entry(GbifTerm.projectId, columnFor(GbifTerm.projectId)),
            Map.entry(GbifInternalTerm.programmeAcronym, columnFor(GbifInternalTerm.programmeAcronym)),
            Map.entry(GbifInternalTerm.hostingOrganizationKey, columnFor(GbifInternalTerm.hostingOrganizationKey)),
            Map.entry(GbifInternalTerm.isInCluster, columnFor(GbifInternalTerm.isInCluster)),
            Map.entry(GbifInternalTerm.dwcaExtension, columnFor(GbifInternalTerm.dwcaExtension)),
            Map.entry(GbifInternalTerm.eventDateGte, columnFor(GbifInternalTerm.eventDateGte)),
            Map.entry(GbifInternalTerm.eventDateLte, columnFor(GbifInternalTerm.eventDateLte))
    );

    List<InitializableField> result = new ArrayList<>();

    for (var term : GbifInternalTerm.values()) {
      if (DownloadTerms.EXCLUSIONS_HDFS.contains(term)) {
        continue;
      }

      var initializer = initializers.get(term);
      if (initializer != null) {
        result.add(interpretedField(term, initializer));
      } else {
        result.add(interpretedField(term));
      }
    }

    return result;
  }


  /**
   * The fields stored in Avro which represent an extension.
   *
   * @return the list of fields that are exposed through Hive
   */
  private static List<InitializableField> extensions() {
    // only MULTIMEDIA is supported, but coded for future use
    Set<Extension> extensions = Set.of(Extension.MULTIMEDIA);

    List<InitializableField> result = new ArrayList<>();

    for (var extension : extensions) {
      result.add(new InitializableField(
              extensionTerm(extension),
              columnFor(extension),
              HiveDataTypes.TYPE_STRING // always, as it has a custom serialization
      ));
    }

    return result;
  }

  private static Term extensionTerm(Extension extension) {
    if (Extension.MULTIMEDIA == extension) {
      return GbifTerm.Multimedia;
    } else {
      return UnknownTerm.build(extension.name());
    }
  }

  /**
   * Generates the conceptual definition for the occurrence tables when used in hive.
   *
   * @return a list of fields, with the types.
   */
  public static List<InitializableField> definition() {
    return Stream.of(
            Stream.of(keyField()),
            verbatimFields().stream(),
            internalFields().stream(),
            interpretedFields().stream(),
            extensions().stream()
          ).reduce(Stream::concat).orElseGet(Stream::empty).collect(Collectors.toList());
  }

  /**
   * Constructs the field for the primary key, which is a special case in that it needs a special mapping.
   */
  private static InitializableField keyField() {
    return new InitializableField(GbifTerm.gbifID,
                                  GbifTerm.gbifID.simpleName().toLowerCase(Locale.ENGLISH),
                                  HiveDataTypes.typeForTerm(GbifTerm.gbifID, true)
                                  // verbatim context
    );
  }

  /**
   * Constructs a Field for the given term, when used in the verbatim context.
   */
  private static InitializableField verbatimField(Term term) {
    String column = getVerbatimColPrefix() + term.simpleName().toLowerCase(Locale.ENGLISH);
    return new InitializableField(term, column,
                                  // no escape needed, due to prefix
                                  HiveDataTypes.typeForTerm(term, true), // verbatim context
                                  cleanDelimitersInitializer(column) //remove delimiters '\n', '\t', etc.
    );
  }

  /**
   * Constructs a Field for the given term, when used in the interpreted context constructed with no custom
   * initializer.
   */
  private static InitializableField interpretedField(Term term) {
    if (HiveDataTypes.TYPE_STRING.equals(HiveDataTypes.typeForTerm(term, false))) {
      return interpretedField(term, cleanDelimitersInitializer(term)); // no initializer
    }
    if (HiveDataTypes.TYPE_ARRAY_STRING.equals(HiveDataTypes.typeForTerm(term, false))
        && ARRAYS_FROM_VERBATIM_VALUES.contains(term)) {
      return interpretedField(term, cleanDelimitersArrayInitializer(term)); // no initializer
    }

    return interpretedField(term, null); // no initializer
  }

  /**
   * Constructs a Field for the given term, when used in the interpreted context, and setting it up with the
   * given initializer.
   */
  private static InitializableField interpretedField(Term term, String initializer) {
    return new InitializableField(term,
                                  term.simpleName().toLowerCase(Locale.ENGLISH),
                                  // note that Columns takes care of whether this is mounted
                                  // on a verbatim or an interpreted column for us
                                  HiveDataTypes.typeForTerm(term, false),
                                  // not verbatim context
                                  initializer);
  }
}
