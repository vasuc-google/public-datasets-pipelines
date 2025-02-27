SELECT
  pivot.PatientID,
  pivot.BodyPartExamined,
  pivot.SeriesInstanceUID,
  pivot.SliceThickness,
  pivot.SeriesNumber,
  pivot.SeriesDescription,
  pivot.StudyInstanceUID,
  pivot.StudyDescription,
  pivot.StudyDate,
  pivot.SOPInstanceUID,
  pivot.Modality,
  pivot.SOPClassUID,
  pivot.collection_id,
  Internal_structure,
  Sphericity,
  Calcification,
  Lobular_Pattern,
  Spiculation,
  Margin,
  Texture,
  Subtlety_score,
  Malignancy,
  SUVbw,
  Volume,
  Diameter,
  Surface_area_of_mesh, Total_Lesion_Glycolysis,
  Standardized_Added_Metabolic_Activity,
  Percent_Within_First_Quarter_of_Intensity_Range,
  Percent_Within_Third_Quarter_of_Intensity_Range,
  Percent_Within_Fourth_Quarter_of_Intensity_Range,
  Percent_Within_Second_Quarter_of_Intensity_Range,
  Standardized_Added_Metabolic_Activity_Background,
  Glycolysis_Within_First_Quarter_of_Intensity_Range,
  Glycolysis_Within_Third_Quarter_of_Intensity_Range,
  Glycolysis_Within_Fourth_Quarter_of_Intensity_Range,
  Glycolysis_Within_Second_Quarter_of_Intensity_Range,
  pivot.AnatomicRegionSequence,
  SegmentedPropertyCategoryCodeSequence,
  SegmentedPropertyTypeCodeSequence,
  pivot.FrameOfReferenceUID,
  SegmentNumber,
  SegmentAlgorithmType,
  pivot.crdc_study_uuid,
  pivot.crdc_series_uuid,
  pivot.crdc_instance_uuid,
  Program,
  pivot.tcia_tumorLocation,
  pivot.source_DOI,
  gcs_url,
  pivot.tcia_species
FROM `PROJECT.DATASET.dicom_derived_all` pivot
JOIN `PROJECT.DATASET.dicom_all` dicom_all
ON pivot.SOPInstanceUID = dicom_all.SOPInstanceUID
