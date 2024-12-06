# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
import dlt
from pyspark.sql.functions import col, explode, flatten


# Define the path to your ADLS container
landing_folder = "abfss://xxxxx/BSM/output_data/"

# Define the header schema
header_schema = StructType([
    StructField("Version", StringType(), True),
    StructField("ReasonCode", StringType(), True),
    StructField("ShipOrderID", StringType(), True),
    StructField("ShipmentMode", StringType(), True),
    StructField("DipsatchingMode", StringType(), True),
    StructField("tripNumber", StringType(), True),
    StructField("DENumber", StringType(), True),
    StructField("CustomerReference", StringType(), True),
    StructField("DispatchNumber", StringType(), True),
    StructField("CountryCode", StringType(), True),
    StructField("ShipFromSiteADS", StringType(), True),
    StructField("ShipFromSite", StringType(), True),
    StructField("ShipFromADS", StringType(), True),
    StructField("LoadingSite", StringType(), True),
    StructField("OriginDCode", StringType(), True),
    StructField("LoadingDCode", StringType(), True),
    StructField("CarrierCode", StringType(), True),
    StructField("Incoterm", StringType(), True),
    StructField("InventoryOrg", StringType(), True),
    StructField("ShipmentDate", TimestampType(), True),
    StructField("StartDateLoading", TimestampType(), True),
    StructField("EndDateLoading", TimestampType(), True),
    StructField("VehicleType", StringType(), True),
    StructField("TruckNumber", StringType(), True),
    StructField("TrailerNumber", StringType(), True),
    StructField("ContainerNumber", StringType(), True),
    StructField("SealCode1", StringType(), True),
    StructField("SealCode2", StringType(), True),
    StructField("ESealCode1", StringType(), True),
    StructField("ESealCode2", StringType(), True),
    StructField("MotifPlanningNonRespecte", StringType(), True),
    StructField("Anomalie", StringType(), True),
    StructField("DateStartLoadTruck", TimestampType(), True),
    StructField("OperatorName", StringType(), True),
    StructField("DatePresentationQuai", TimestampType(), True),
    StructField("DateEndLoadTruck", TimestampType(), True),
    StructField("Tare", StringType(), True),
    StructField("Freeinstructions", StringType(), True)])

mix_schema    = StructType([
            StructField("MIXCOD", StringType(), True),
                StructField("MIXCLA", StringType(), True),
                StructField("MIXIND", StringType(), True),
                StructField("MIXSTU", StringType(), True),
                StructField("MIXWEI", FloatType(), True),
                StructField("MIXDAT", TimestampType(), True),
                StructField("BLKMIX", StringType(), True)])

lo_nut_schema = StructType([
            StructField("NRONUT", StringType(), True),
            StructField("CODPRO", StringType(), True),
            StructField("CODIDO", StringType(), True),
            StructField("CANIDO", FloatType(), True),
            StructField("FECFAB", TimestampType(), True),
            StructField("FECTEO", TimestampType(), True),
            StructField("ENDFAB", TimestampType(), True),
            StructField("TICKNR", StringType(), True),
            StructField("VIRTUAL_SN", StringType(), True),
            StructField("CONTID", StringType(), True),
            StructField("MIX", mix_schema, True)])

lo_hueco_nut_schema = StructType([StructField("TIPALM", StringType(), True),
        StructField("CODALM", StringType(), True),
        StructField("CODHUE", StringType(), True),
        StructField("NRONUT", StringType(), True),
        StructField("NROLIN", StringType(), True),
        StructField("CODPRO", StringType(), True),
        StructField("ACONDS", StringType(), True),
        StructField("ULTEAL", TimestampType(), True),
        StructField("ULTSLD", TimestampType(), True),
        StructField("ACOCF1", StringType(), True),
        StructField("ACOCF2", StringType(), True),
        StructField("ACOCF3", StringType(), True),
        StructField("CODCLI", StringType(), True),
        StructField("CODPRV", StringType(), True),
        StructField("FECRCP", TimestampType(), True),
        StructField("CLOSED", StringType(), True),
        StructField("BLKFAB", StringType(), True),
        StructField("NC3TIM", StringType(), True),
        StructField("NC3NUM", StringType(), True),
        StructField("DISLIN", StringType(), True),
        StructField("LOTNUM", StringType(), True),
        StructField("RECNUM", StringType(), True),
        StructField("RECLIN", StringType(), True),
        StructField("MATIND", StringType(), True),
        StructField("CODMAQ", StringType(), True),
        StructField("CODUSU", StringType(), True),
        StructField("REMARK", StringType(), True),
        StructField("DISCOM", StringType(), True),
        StructField("PALCLA", StringType(), True),
        StructField("PALIND", StringType(), True),
        StructField("WEIGRO", FloatType(), True),
        StructField("DERLOCNUM", StringType(), True),
        StructField("LONUTI", FloatType(), True),
        StructField("UMUSED", StringType(), True),
        StructField("UNIMED", StringType(), True),
        StructField("NCCLASS", StringType(), True),
        StructField("RECNUM_INIT", StringType(), True),
        StructField("MARNUM_INIT", StringType(), True),
        StructField("POIRNB_INIT", StringType(), True),
        StructField("LO_NUT", lo_nut_schema, True),])




details_schema = StructType([
    StructField("DeliveryNumber", StringType(), True),
    StructField("ShipTo", StringType(), True),
    StructField("UnloadingSite", StringType(), True),
    StructField("Nif", StringType(), True),
    StructField("MadeIn", StringType(), True),
    StructField("PackageMadeIn", StringType(), True),
    StructField("PlannedDeliveryDate", TimestampType(), True),
    StructField("DeliveryLineNumber", StringType(), True),
    StructField("DeliveryLineNumberCanonical", StringType(), True),
    StructField("ProductCode", StringType(), True),
    StructField("MFDCode", StringType(), True),
    StructField("AGP", StringType(), True),
    StructField("DeliveryLineDMC", StringType(), True),
    StructField("OTNumber", StringType(), True),
    StructField("Typmat", StringType(), True),
    StructField("HSType", StringType(), True),
    StructField("QtyPallet", StringType(), True),
    StructField("QtyERPUOM", FloatType(), True),
    StructField("UOM", StringType(), True),
    StructField("BSMPalletClass", StringType(), True),
    StructField("BSMPalletIndex", StringType(), True),
    StructField("BSMPalletStatus", StringType(), True),
    StructField("PalletContainer", StringType(), True),
    StructField("PalletContainerPL1", StringType(), True),
    StructField("Machine", StringType(), True),
    StructField("PalletNetWeight", FloatType(), True),
    StructField("PalletGrossWeight", FloatType(), True),
    StructField("PlannedShipQuantity", FloatType(), True),
    StructField("ProductPresentation", StringType(), True),
    StructField("BaseProductCode", StringType(), True),
    StructField("ManufacturerSiteActivity", StringType(), True),
    StructField("MATNOM", StringType(), True),
    StructField("MATREF", StringType(), True),
    StructField("DescriptionNonCodified", StringType(), True),
    StructField("PackagingCategory", StringType(), True),
    StructField("PackagingNetWeight", FloatType(), True),
    StructField("MATIND", StringType(), True),
    StructField("NRONUT", StringType(), True),
    StructField("InvoicingType", StringType(), True),
    StructField("CustomsProcedure", StringType(), True),
    StructField("CustomsProcedureReference", StringType(), True),
    StructField("PackagingComments", StringType(), True),
    StructField("ItemComments", StringType(), True),
    StructField("DestinationDCode", StringType(), True),
    StructField("UnloadingDCode", StringType(), True),
    StructField("ShipOrderSegmentID", StringType(), True),
    StructField("ShipOrderSegmentProductID", StringType(), True),
    StructField("LO_HUECO_NUT", lo_hueco_nut_schema, True)
    
])

# Define the DLT table
@dlt.table(
    name="shipment_confirmations",
    comment="Table to store shipment confirmations",
    table_properties={
        "quality": "silver"
    }
)
def shipment_confirmations():
    df = (
        spark.readStream.format("xml")
        .option("rowTag", "BSMERPShipmentConfirm")
        .schema(StructType([
            StructField("HEADER", header_schema, True),
            StructField("DETAIL", details_schema, True)
        ]))
        .load(landing_folder)
    )
    
    # Flatten the nested structure
    # Flatten the nested structure
    # Flatten the nested structure
    flattened_df = df.selectExpr(
        "HEADER.*",
        "DETAIL.DeliveryNumber",
        "DETAIL.ShipTo",
        "DETAIL.UnloadingSite",
        "DETAIL.Nif",
        "DETAIL.DescriptionNonCodified",
        "DETAIL.PackagingCategory",
        "DETAIL.PackagingNetWeight",
        "DETAIL.MATIND",
        "DETAIL.NRONUT as DETAIL_NRONUT",
        "DETAIL.InvoicingType",
        "DETAIL.CustomsProcedure",
        "DETAIL.CustomsProcedureReference",
        "DETAIL.PackagingComments",
        "DETAIL.ItemComments",
        "DETAIL.DestinationDCode",
        "DETAIL.UnloadingDCode",
        "DETAIL.ShipOrderSegmentID",
        "DETAIL.ShipOrderSegmentProductID",
        "DETAIL.LO_HUECO_NUT.TIPALM",
        "DETAIL.LO_HUECO_NUT.CODALM",
        "DETAIL.LO_HUECO_NUT.CODHUE",
        "DETAIL.LO_HUECO_NUT.NRONUT as LO_HUECO_NUT_NRONUT",
        "DETAIL.LO_HUECO_NUT.NROLIN",
        "DETAIL.LO_HUECO_NUT.CODPRO as LO_HUECO_NUT_CODPRO",
        "DETAIL.LO_HUECO_NUT.ACONDS",
        "DETAIL.LO_HUECO_NUT.ULTEAL",
        "DETAIL.LO_HUECO_NUT.ULTSLD",
        "DETAIL.LO_HUECO_NUT.ACOCF1",
        "DETAIL.LO_HUECO_NUT.ACOCF2",
        "DETAIL.LO_HUECO_NUT.ACOCF3",
        "DETAIL.LO_HUECO_NUT.CODCLI",
        "DETAIL.LO_HUECO_NUT.CODPRV",
        "DETAIL.LO_HUECO_NUT.FECRCP",
        "DETAIL.LO_HUECO_NUT.CLOSED",
        "DETAIL.LO_HUECO_NUT.BLKFAB",
        "DETAIL.LO_HUECO_NUT.LO_NUT.NRONUT as LO_NUT_NRONUT",
        "DETAIL.LO_HUECO_NUT.LO_NUT.CODPRO as LO_NUT_CODPRO",
        "DETAIL.LO_HUECO_NUT.LO_NUT.CODIDO",
        "DETAIL.LO_HUECO_NUT.LO_NUT.CANIDO",
        "DETAIL.LO_HUECO_NUT.LO_NUT.CONTID",
        "DETAIL.LO_HUECO_NUT.LO_NUT.MIX.MIXCOD",
        "DETAIL.LO_HUECO_NUT.LO_NUT.MIX.MIXCLA",
        "DETAIL.LO_HUECO_NUT.LO_NUT.MIX.BLKMIX"
    )
    
    return flattened_df

