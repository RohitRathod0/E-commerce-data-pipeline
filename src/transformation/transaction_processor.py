
"""
Transaction Processor - Curated Zone Transformation
Handles deduplication, validation, and standardization of transaction data
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, upper, trim, regexp_replace,
    row_number, when, current_timestamp, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from typing import Tuple
import logging

logger = logging.getLogger(__name__)


class TransactionProcessor:
    """
    Process raw transaction data into curated zone
    
    Transformations applied:
    - Remove duplicates based on transaction_id and timestamp
    - Validate and cast data types
    - Standardize formats (currency codes, dates)
    - Flag invalid records for review
    - Apply business rules
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def process(self, raw_df: DataFrame, processing_date: str) -> Tuple[DataFrame, DataFrame]:
        """
        Main processing pipeline
        
        Args:
            raw_df: Raw transaction data from landing zone
            processing_date: Date being processed (YYYY-MM-DD)
            
        Returns:
            Tuple of (valid_df, invalid_df) - valid records and flagged issues
        """
        logger.info(f"Starting transaction processing for {processing_date}")
        logger.info(f"Input row count: {raw_df.count()}")
        
        # Step 1: Remove duplicates
        dedup_df = self._deduplicate(raw_df)
        logger.info(f"After deduplication: {dedup_df.count()} rows")
        
        # Step 2: Validate and cast types
        typed_df = self._cast_types(dedup_df)
        
        # Step 3: Standardize formats
        standardized_df = self._standardize_formats(typed_df)
        
        # Step 4: Apply business rules and split valid/invalid
        valid_df, invalid_df = self._apply_business_rules(standardized_df)
        
        logger.info(f"Valid records: {valid_df.count()}")
        logger.info(f"Invalid records: {invalid_df.count()}")
        
        return valid_df, invalid_df
    
    def _deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicate transactions
        Keep the most recent record based on timestamp
        """
        window = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
        
        dedup_df = df.withColumn("row_num", row_number().over(window)) \
                     .filter(col("row_num") == 1) \
                     .drop("row_num")
        
        return dedup_df
    
    def _cast_types(self, df: DataFrame) -> DataFrame:
        """
        Cast string fields to proper data types
        Handle conversion errors gracefully
        """
        typed_df = df.withColumn(
            "amount",
            # Remove commas and convert to decimal
            regexp_replace(col("amount"), ",", "").cast(DecimalType(10, 2))
        ).withColumn(
            "transaction_date",
            to_date(col("transaction_date_string"), "yyyy-MM-dd")
        ).withColumn(
            "quantity",
            col("quantity").cast("int")
        )
        
        return typed_df
    
    def _standardize_formats(self, df: DataFrame) -> DataFrame:
        """
        Standardize text fields and formats
        """
        standardized_df = df.withColumn(
            "currency_code",
            upper(trim(col("currency_code")))
        ).withColumn(
            "transaction_type",
            upper(trim(col("transaction_type")))
        ).withColumn(
            "customer_email",
            trim(col("customer_email"))
        )
        
        return standardized_df
    
    def _apply_business_rules(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Apply business validation rules
        Flag invalid records for review
        """
        # Add validation flags
        validated_df = df.withColumn(
            "is_valid",
            when(
                # All critical fields must be non-null
                (col("transaction_id").isNotNull()) &
                (col("customer_id").isNotNull()) &
                (col("amount").isNotNull()) &
                (col("transaction_date").isNotNull()) &
                # Amount must be positive
                (col("amount") > 0) &
                # Amount must be reasonable (< $100k)
                (col("amount") < 100000) &
                # Transaction type must be valid
                (col("transaction_type").isin(["PURCHASE", "REFUND", "ADJUSTMENT"])) &
                # Currency code must be valid
                (col("currency_code").isin(["USD", "EUR", "GBP"])) &
                # Quantity must be positive
                (col("quantity") > 0),
                lit(True)
            ).otherwise(lit(False))
        ).withColumn(
            "validation_errors",
            when(col("transaction_id").isNull(), "Missing transaction_id; ")
            .when(col("customer_id").isNull(), "Missing customer_id; ")
            .when(col("amount").isNull(), "Missing amount; ")
            .when(col("amount") <= 0, "Invalid amount (must be > 0); ")
            .when(col("amount") >= 100000, "Amount exceeds limit; ")
            .when(~col("transaction_type").isin(["PURCHASE", "REFUND", "ADJUSTMENT"]), 
                  "Invalid transaction type; ")
            .when(~col("currency_code").isin(["USD", "EUR", "GBP"]), 
                  "Invalid currency code; ")
            .when(col("quantity") <= 0, "Invalid quantity; ")
            .otherwise(None)
        ).withColumn(
            "processed_timestamp",
            current_timestamp()
        )
        
        # Split into valid and invalid dataframes
        valid_df = validated_df.filter(col("is_valid") == True).drop("is_valid", "validation_errors")
        invalid_df = validated_df.filter(col("is_valid") == False).drop("is_valid")
        
        return valid_df, invalid_df


def main():
    """
    Example usage
    """
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("TransactionProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Read raw data
    raw_path = "s3://raw-zone/transactions/2024/11/20/"
    raw_df = spark.read.parquet(raw_path)
    
    # Process
    processor = TransactionProcessor(spark)
    valid_df, invalid_df = processor.process(raw_df, "2024-11-20")
    
    # Write outputs
    curated_path = "s3://curated-zone/transactions/"
    valid_df.write \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .parquet(curated_path)
    
    # Write invalid records to error table for review
    error_path = "s3://error-zone/transactions/"
    invalid_df.write \
        .mode("append") \
        .partitionBy("transaction_date") \
        .parquet(error_path)
    
    spark.stop()


if __name__ == "__main__":
    main()
