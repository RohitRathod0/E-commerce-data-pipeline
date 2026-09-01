"""
Data Quality Validator using Great Expectations
Implements comprehensive quality checks for transaction data
"""

import great_expectations as ge
from great_expectations.dataset import SparkDFDataset
from pyspark.sql import DataFrame
from typing import Dict, List
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class QualityValidator:
    """
    Data quality validation framework
    
    Implements four pillars of data quality:
    1. Completeness - all expected data is present
    2. Accuracy - data values are correct
    3. Consistency - data makes sense across tables and time
    4. Timeliness - data is fresh enough
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or self._default_config()
        self.validation_results = []
        
    def _default_config(self) -> Dict:
        """Default quality thresholds"""
        return {
            "null_threshold": 0.05,  # Max 5% nulls allowed
            "min_row_count": 10000,  # Minimum expected rows
            "max_row_count": 1000000,  # Maximum expected rows
            "amount_min": 0,
            "amount_max": 100000,
            "valid_currencies": ["USD", "EUR", "GBP"],
            "valid_transaction_types": ["PURCHASE", "REFUND", "ADJUSTMENT"]
        }
    
    def validate_transactions(self, df: DataFrame) -> Dict:
        """
        Run full quality validation suite on transaction data
        
        Returns:
            Dict with validation results and pass/fail status
        """
        logger.info("Starting data quality validation")
        
        # Wrap DataFrame for Great Expectations
        ge_df = SparkDFDataset(df)
        
        # Run all validation suites
        self._validate_completeness(ge_df)
        self._validate_accuracy(ge_df)
        self._validate_consistency(ge_df)
        self._validate_timeliness(ge_df)
        
        # Validate and get results
        results = ge_df.validate()
        
        # Process results
        summary = self._process_results(results)
        
        logger.info(f"Quality validation complete. Pass: {summary['passed']}/{summary['total']}")
        
        return summary
    
    def _validate_completeness(self, ge_df: SparkDFDataset):
        """Check for missing or incomplete data"""
        
        # Critical fields must never be null
        ge_df.expect_column_values_to_not_be_null("transaction_id")
        ge_df.expect_column_values_to_not_be_null("customer_id")
        ge_df.expect_column_values_to_not_be_null("amount")
        ge_df.expect_column_values_to_not_be_null("transaction_date")
        ge_df.expect_column_values_to_not_be_null("transaction_type")
        
        # Row count should be within expected range
        ge_df.expect_table_row_count_to_be_between(
            min_value=self.config["min_row_count"],
            max_value=self.config["max_row_count"]
        )
        
        # Check null rates don't exceed threshold
        for column in ["customer_email", "product_id", "quantity"]:
            ge_df.expect_column_values_to_not_be_null(
                column,
                mostly=1 - self.config["null_threshold"]  # Allow up to 5% nulls
            )
    
    def _validate_accuracy(self, ge_df: SparkDFDataset):
        """Check if data values are correct and in valid ranges"""
        
        # Amount validations
        ge_df.expect_column_values_to_be_between(
            "amount",
            min_value=self.config["amount_min"],
            max_value=self.config["amount_max"]
        )
        
        # Quantity must be positive
        ge_df.expect_column_values_to_be_between(
            "quantity",
            min_value=1,
            max_value=1000
        )
        
        # Categorical values must be from valid set
        ge_df.expect_column_values_to_be_in_set(
            "currency_code",
            self.config["valid_currencies"]
        )
        
        ge_df.expect_column_values_to_be_in_set(
            "transaction_type",
            self.config["valid_transaction_types"]
        )
        
        # Email format validation
        ge_df.expect_column_values_to_match_regex(
            "customer_email",
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            mostly=0.95  # 95% of emails should match pattern
        )
        
        # Transaction ID format (assuming alphanumeric)
        ge_df.expect_column_values_to_match_regex(
            "transaction_id",
            r"^[A-Z0-9]{10,20}$"
        )
    
    def _validate_consistency(self, ge_df: SparkDFDataset):
        """Check for data consistency and integrity"""
        
        # Primary key uniqueness
        ge_df.expect_column_values_to_be_unique("transaction_id")
        
        # No duplicate customer_id + transaction_date combinations
        # (customer shouldn't have exact same transaction twice on same day)
        ge_df.expect_compound_columns_to_be_unique(
            ["customer_id", "transaction_date", "amount"],
            mostly=0.99  # Allow 1% for legitimate duplicates
        )
        
        # Refund amount shouldn't exceed original transaction
        # (if we had original_transaction_id, we'd validate this)
        ge_df.expect_column_values_to_be_between(
            "amount",
            min_value=0,
            max_value=self.config["amount_max"]
        )
    
    def _validate_timeliness(self, ge_df: SparkDFDataset):
        """Check data freshness and recency"""
        
        # Transaction dates should be recent (within last 90 days)
        from datetime import datetime, timedelta
        max_age = 90
        min_date = (datetime.now() - timedelta(days=max_age)).strftime("%Y-%m-%d")
        
        ge_df.expect_column_values_to_be_between(
            "transaction_date",
            min_value=min_date,
            max_value=datetime.now().strftime("%Y-%m-%d")
        )
        
        # No future dates
        ge_df.expect_column_values_to_be_between(
            "transaction_date",
            min_value="2020-01-01",  # Reasonable lower bound
            max_value=datetime.now().strftime("%Y-%m-%d")
        )
    
    def _process_results(self, results) -> Dict:
        """Process validation results into summary"""
        
        passed = sum(1 for r in results.results if r.success)
        total = len(results.results)
        failed_checks = [r for r in results.results if not r.success]
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "passed": passed,
            "failed": total - passed,
            "total": total,
            "success_rate": passed / total if total > 0 else 0,
            "overall_success": results.success,
            "failed_expectations": []
        }
        
        # Detail failed checks
        for failure in failed_checks:
            summary["failed_expectations"].append({
                "expectation_type": failure.expectation_config.expectation_type,
                "column": failure.expectation_config.kwargs.get("column"),
                "details": str(failure.result)
            })
        
        return summary
    
    def generate_report(self, summary: Dict) -> str:
        """Generate human-readable quality report"""
        
        report = f"""
        Data Quality Report
        {'='*50}
        Timestamp: {summary['timestamp']}
        
        Overall Status: {'✅ PASS' if summary['overall_success'] else '❌ FAIL'}
        Success Rate: {summary['success_rate']:.1%}
        
        Checks Passed: {summary['passed']}/{summary['total']}
        Checks Failed: {summary['failed']}/{summary['total']}
        
        """
        
        if summary['failed_expectations']:
            report += "\nFailed Checks:\n"
            report += "-" * 50 + "\n"
            for failure in summary['failed_expectations']:
                report += f"\n❌ {failure['expectation_type']}\n"
                report += f"   Column: {failure['column']}\n"
                report += f"   Details: {failure['details']}\n"
        
        return report


def main():
    """Example usage"""
    from pyspark.sql import SparkSession
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("QualityValidator") \
        .getOrCreate()
    
    # Read curated data
    df = spark.read.parquet("s3://curated-zone/transactions/2024/11/20/")
    
    # Run quality checks
    validator = QualityValidator()
    summary = validator.validate_transactions(df)
    
    # Generate report
    report = validator.generate_report(summary)
    print(report)
    
    # If validation failed, stop pipeline
    if not summary['overall_success']:
        raise ValueError(f"Data quality validation failed! {summary['failed']} checks failed.")
    
    spark.stop()


if __name__ == "__main__":
    main()
