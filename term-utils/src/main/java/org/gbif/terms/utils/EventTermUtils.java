package org.gbif.terms.utils;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.dwc.terms.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.gbif.terms.utils.TermUtils.DwC_DC_PROPERTIES;
import static org.gbif.terms.utils.TermUtils.DwC_PROPERTIES;


/** This class customizes some of the methods and variables of TermUtils to apply them to events. */
public class EventTermUtils {

  public static final Set<Term> TERMS_POPULATED_BY_INTERPRETATION =
          Set.of(
                  DwcTerm.decimalLatitude,
                  DwcTerm.decimalLongitude,
                  DwcTerm.continent,
                  DwcTerm.waterBody,
                  DwcTerm.stateProvince,
                  DwcTerm.countryCode,
                  DwcTerm.dateIdentified,
                  DwcTerm.eventDate,
                  DwcTerm.year,
                  DwcTerm.month,
                  DwcTerm.day,
                  DwcTerm.startDayOfYear,
                  DwcTerm.endDayOfYear,
                  GbifTerm.datasetKey,
                  GbifTerm.publishingCountry,
                  GbifTerm.lastInterpreted,
                  DcTerm.modified,
                  DwcTerm.coordinateUncertaintyInMeters,
                  DwcTerm.coordinatePrecision,
                  GbifTerm.elevation,
                  GbifTerm.elevationAccuracy,
                  DwcTerm.minimumDepthInMeters,
                  DwcTerm.maximumDepthInMeters,
                  DwcTerm.minimumElevationInMeters,
                  DwcTerm.maximumElevationInMeters,
                  DwcTerm.maximumDistanceAboveSurfaceInMeters,
                  DwcTerm.minimumDistanceAboveSurfaceInMeters,
                  GbifTerm.distanceFromCentroidInMeters,
                  GbifTerm.depth,
                  GbifTerm.depthAccuracy,
                  GbifTerm.hasCoordinate,
                  GbifTerm.hasGeospatialIssues,
                  GbifTerm.repatriated,
                  GadmTerm.level0Gid,
                  GadmTerm.level0Name,
                  GadmTerm.level1Gid,
                  GadmTerm.level1Name,
                  GadmTerm.level2Gid,
                  GadmTerm.level2Name,
                  GadmTerm.level3Gid,
                  GadmTerm.level3Name,
                  DwcTerm.sampleSizeUnit,
                  DwcTerm.sampleSizeValue,
                  DwcTerm.organismQuantityType,
                  DwcTerm.organismQuantity,
                  GbifInternalTerm.unitQualifier,
                  GbifTerm.issue,
                  GbifTerm.protocol,
                  GbifTerm.lastCrawled,
                  GbifTerm.lastParsed,
                  GbifInternalTerm.installationKey,
                  GbifInternalTerm.publishingOrgKey,
                  GbifInternalTerm.networkKey,
                  GbifTerm.mediaType,
                  DcTerm.license,
                  DwcTerm.datasetID,
                  DwcTerm.datasetName,
                  DwcTerm.samplingProtocol,
                  GbifTerm.gbifRegion,
                  GbifTerm.publishedByGbifRegion,
                  DwcTerm.georeferencedBy,
                  DwcTerm.higherGeography,
                  GbifTerm.projectId,
                  DwcTerm.eventType,
                  DwcTerm.eventID,
                  DwcTerm.parentEventID,
                  DwcTerm.locality,
                  DwcTerm.locationID,
                  DwcTerm.institutionCode,
                  DwcTerm.collectionCode,
                  DwcTerm.projectTitle,
                  DwcTerm.fundingAttribution,
                  DwcTerm.fundingAttributionID
          );

  private static final Set<Term> TERMS_SUBJECT_TO_INTERPRETATION =
      Stream.concat(
              TERMS_POPULATED_BY_INTERPRETATION.stream(),
                      Set.of(
                              DwcTerm.verbatimLatitude,
                              DwcTerm.verbatimLongitude,
                              DwcTerm.verbatimCoordinates,
                              DwcTerm.geodeticDatum,
                              DwcTerm.country).stream()).collect(Collectors.toSet());

  private static final Set<Term> TERMS_REMOVED_DURING_INTERPRETATION =
          TERMS_SUBJECT_TO_INTERPRETATION.stream()
                  .filter(t -> !TERMS_POPULATED_BY_INTERPRETATION.contains(t))
                  .collect(Collectors.toUnmodifiableSet());


  public static List<Term> interpretedTerms() {
    List<Term> terms = new ArrayList<>();

    terms.add(GbifTerm.gbifID);

    // add all Dublin Core terms that are not stripped during interpretation
    DwC_DC_PROPERTIES.stream()
            .filter(t ->
                    !TERMS_REMOVED_DURING_INTERPRETATION.contains(t)
                            && !TERMS_POPULATED_BY_INTERPRETATION.contains(t))
            .forEach(terms::add);

    terms.addAll(TERMS_POPULATED_BY_INTERPRETATION);

    terms.add(DwcTerm.measurementType);
    terms.add(ObisTerm.measurementTypeID);

    return List.copyOf(terms);
  }

  public static List<Term> verbatimTerms() {
    List<Term> terms = new ArrayList<>();

    terms.add(GbifTerm.gbifID);
    terms.addAll(DwC_DC_PROPERTIES);
    terms.addAll(DwC_PROPERTIES);

    return List.copyOf(terms);
  }

  public static List<Term> internalTerms() {
    return List.of(
        GbifInternalTerm.identifierCount,
        GbifInternalTerm.crawlId,
        GbifInternalTerm.fragmentCreated,
        GbifInternalTerm.xmlSchema,
        GbifInternalTerm.publishingOrgKey,
        GbifInternalTerm.unitQualifier,
        GbifInternalTerm.networkKey,
        GbifInternalTerm.installationKey,
        GbifInternalTerm.programmeAcronym,
        GbifInternalTerm.hostingOrganizationKey,
        GbifInternalTerm.dwcaExtension,
        GbifInternalTerm.datasetTitle,
        GbifInternalTerm.eventDateGte,
        GbifInternalTerm.eventDateLte,
        GbifInternalTerm.parentEventGbifId,
        GbifInternalTerm.humboldtEventDurationValueInMinutes);
  }

  public static boolean isInterpretedSourceTerm(Term term) {
    return TERMS_SUBJECT_TO_INTERPRETATION.contains(term);
  }

  /**
   * Returns the map of term→value for all terms which, after interpretation, have the same value on
   * all occurrences. In Darwin Core Archive terms, this is a default value.
   */
  public static Map<Term, String> identicalInterpretedTerms() {
    return TERMS_IDENTICAL_AFTER_INTERPRETATION;
  }

  /**
   * The map of term→value for terms that, after interpretation, have the same value for all
   * occurrences.
   *
   * <p>For example, coordinates are reprojected to WGS84, so dwc:geodeticDatum is "WGS84" for all
   * occurrences.
   */
  private static final Map<Term, String> TERMS_IDENTICAL_AFTER_INTERPRETATION =
      Map.of(DwcTerm.geodeticDatum, Occurrence.GEO_DATUM);
}
