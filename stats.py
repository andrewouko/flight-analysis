import datetime
from enum import Enum
import logging
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)


class DividerType(Enum):
    SINGLE = "-------------------------------------------------------------------"
    DOUBLE = "==================================================================="


class FlightStatistics:
    """
    Class to analyze and report statistics from the computed flight details.
    """

    SOLUTION_ID_COL = 'Solution ID'
    BATCH_NUMBER_COL = 'Batch Number'

    def __init__(self, dataframes: list[pd.DataFrame], flight_type_enum, batch_size: int) -> None:
        """
        Initialize the flight statistics processor.

        Args:
            dataframes: List of pandas DataFrames containing flight details
            flight_type_enum: Enum containing flight type descriptions
            batch_size: The configured batch size used for queries
        """
        # Start timing for initialization
        init_start_time = datetime.datetime.now()

        self.flight_type_enum = flight_type_enum
        self.dataframes = dataframes
        self.combined_df = None
        self.stats_cache = {}
        self.batch_size = batch_size

        # Performance metrics tracking dictionary
        self.performance_metrics = {
            'initialization': {
                'start_time': init_start_time,
                'end_time': datetime.datetime.now(),
                'duration': None  # Will be calculated below
            },
            'dataframe_combine': {'start_time': None, 'end_time': None, 'duration': None},
            'stats_computation': {'start_time': None, 'end_time': None, 'duration': None},
            'report_generation': {'start_time': None, 'end_time': None, 'duration': None}
        }

        # Calculate initialization duration
        self.performance_metrics['initialization']['end_time'] = datetime.datetime.now(
        )
        self.performance_metrics['initialization']['duration'] = (
            self.performance_metrics['initialization']['end_time'] -
            self.performance_metrics['initialization']['start_time']).total_seconds()

        # Log initialization metrics
        logger.debug(
            f"FlightStatistics initialized in {self.performance_metrics['initialization']['duration']:.4f} seconds")
        logger.debug(
            f"Initialized with {len(dataframes)} dataframes and batch size {batch_size}")

    def _print_within_divider(self, divider_type: DividerType, message: str) -> None:
        """Print a message within a divider."""
        logger.info(divider_type.value)
        logger.info(message)
        logger.info(divider_type.value)

    def _get_combined_dataframe(self) -> pd.DataFrame:
        """Get a combined dataframe of all flight data."""
        # Skip if already combined or if there's nothing to combine
        if self.combined_df is not None or not self.dataframes:
            return self.combined_df if self.combined_df is not None else pd.DataFrame()
        
        # Start timing dataframe combination
        self.performance_metrics['dataframe_combine']['start_time'] = datetime.datetime.now()
        
        self.combined_df = pd.concat(self.dataframes, ignore_index=True)
        
        # End timing dataframe combination
        self.performance_metrics['dataframe_combine']['end_time'] = datetime.datetime.now()
        self.performance_metrics['dataframe_combine']['duration'] = (
            self.performance_metrics['dataframe_combine']['end_time'] - 
            self.performance_metrics['dataframe_combine']['start_time']).total_seconds()
        
        # Log metrics
        logger.debug(f"Combined {len(self.dataframes)} dataframes in " 
                    f"{self.performance_metrics['dataframe_combine']['duration']:.4f} seconds")
        logger.debug(f"Combined dataframe size: {len(self.combined_df)} rows, {len(self.combined_df.columns)} columns")
        
        return self.combined_df

    def _compute_statistics(self) -> Dict[str, Any]:
        """Compute comprehensive statistics on the flight data."""
        # Return cached stats if already computed
        if 'main_stats' in self.stats_cache:
            return self.stats_cache['main_stats']

        # Start timing statistics computation
        self.performance_metrics['stats_computation']['start_time'] = datetime.datetime.now()
        
        df = self._get_combined_dataframe()
        if df.empty:
            self.stats_cache['main_stats'] = {}
            logger.debug("Empty dataframe, no statistics to compute")
            return {}

        stats = {
            'total_segments': len(df),
            'total_itineraries': len(df[self.SOLUTION_ID_COL].unique()),
            'flight_types': {},
            'batching': {
                'total_batches': len(df[self.BATCH_NUMBER_COL].unique()),
                'batch_size': self.batch_size  # Use the configured batch size
            }
        }

        # Compute statistics for each flight type
        for flight_type in df['Flight Type'].unique():
            flight_df = df[df['Flight Type'] == flight_type]

            flight_stats = {
                'segments': len(flight_df),
                'itineraries': len(flight_df[self.SOLUTION_ID_COL].unique()),
                'description': self.flight_type_enum[flight_type].value,
                'query_types': {},
                'batches': len(flight_df[self.BATCH_NUMBER_COL].unique())
            }

            # Add avg segments per itinerary if applicable
            if flight_stats['itineraries'] > 0:
                flight_stats['avg_segments'] = flight_stats['segments'] / \
                    flight_stats['itineraries']

            # Compute statistics for each query type within this flight type
            for query_type in flight_df['Query Type'].unique():
                query_df = flight_df[flight_df['Query Type'] == query_type]

                batch_info = {
                    'count': len(query_df[self.BATCH_NUMBER_COL].unique()),
                    'distribution': query_df[self.BATCH_NUMBER_COL].value_counts().to_dict(),
                    'batch_size': self.batch_size  # Use the configured batch size
                }

                query_stats = {
                    'segments': len(query_df),
                    'itineraries': len(query_df[self.SOLUTION_ID_COL].unique()),
                    'batches': batch_info,
                    'origins': len(query_df['Origin'].unique()),
                    'destinations': len(query_df['Destination'].unique()),
                    'top_routes': self._get_top_routes(query_df)
                }

                flight_stats['query_types'][query_type] = query_stats

            stats['flight_types'][flight_type] = flight_stats

        # Calculate overall average if applicable
        if stats['total_itineraries'] > 0:
            stats['overall_avg_segments'] = stats['total_segments'] / \
                stats['total_itineraries']

        
        # End timing statistics computation
        self.performance_metrics['stats_computation']['end_time'] = datetime.datetime.now()
        self.performance_metrics['stats_computation']['duration'] = (
            self.performance_metrics['stats_computation']['end_time'] - 
            self.performance_metrics['stats_computation']['start_time']).total_seconds()
        
        # Log metrics
        logger.debug(f"Statistics computation completed in "
                    f"{self.performance_metrics['stats_computation']['duration']:.4f} seconds")
        logger.debug(f"Computed statistics for {len(stats['flight_types'])} flight types")
        self.stats_cache['main_stats'] = stats
        return stats

    def _get_top_routes(self, df: pd.DataFrame, limit: int = 3) -> List[Dict]:
        """Get the top routes from a dataframe."""
        if df.empty:
            return []

        top_routes = df.groupby(['Origin', 'Destination']).size().nlargest(
            limit).reset_index(name='Count')
        return [{'origin': row.Origin, 'destination': row.Destination, 'count': row.Count}
                for row in top_routes.itertuples()]

    def print_statistics_report(self) -> None:
        """Print a comprehensive statistics report to the logs."""
        # Start timing report generation
        self.performance_metrics['report_generation']['start_time'] = datetime.datetime.now()
        
        if not self.dataframes:
            logger.info(
                "EDUARDO STATISTICS: No flight data available to analyze")
            return

        df = self._get_combined_dataframe()
        if df.empty:
            logger.info(
                "EDUARDO STATISTICS: No flight data found in the provided dataframes")
            return

        stats = self._compute_statistics()

        # Print header
        self._print_within_divider(DividerType.DOUBLE,
                                   "EDUARDO FLIGHT STATISTICS REPORT")

        # Add batching configuration information
        if 'batching' in stats:
            batch_info = stats['batching']
            logger.info("BATCHING CONFIGURATION:")
            logger.info(f"TOTAL BATCHES: {batch_info['total_batches']}")
            logger.info(f"CONFIGURED BATCH SIZE: {batch_info['batch_size']}")
            logger.info(DividerType.SINGLE.value)

        # Print flight type statistics
        for flight_type, flight_stats in stats['flight_types'].items():
            self._print_within_divider(
                DividerType.DOUBLE, f"FLIGHT TYPE: {flight_type} | {flight_stats['description']}")
            logger.info(
                f"SEGMENTS: {flight_stats['segments']} | ITINERARIES: {flight_stats['itineraries']}")

            if 'avg_segments' in flight_stats:
                logger.info(
                    f"AVG SEGMENTS PER ITINERARY: {flight_stats['avg_segments']:.2f}")

            # Print query type breakdown - just once, with all information
            for query_type, query_stats in flight_stats['query_types'].items():
                self._print_within_divider(
                    DividerType.SINGLE, f"  {query_type} QUERY RESULTS:")
                logger.info(
                    f"  | SEGMENTS: {query_stats['segments']} | ITINERARIES: {query_stats['itineraries']}")

                # Batch information
                batch_info = query_stats['batches']
                logger.info(
                    f"  | BATCHES: {batch_info['count']} | BATCH SIZE: {self.batch_size}")

                # Origin/destination information
                logger.info(
                    f"  | ORIGINS: {query_stats['origins']} | DESTINATIONS: {query_stats['destinations']}")

                # Distribution of results across batches
                if batch_info['distribution'] and len(batch_info['distribution']) > 1:
                    batch_dist = sorted(batch_info['distribution'].items(),
                                        key=lambda x: x[1], reverse=True)[:3]
                    logger.info("  | TOP BATCHES BY SEGMENT COUNT:")
                    for batch_num, count in batch_dist:
                        logger.info(
                            f"  |   BATCH #{batch_num}: {count} segments")

                # Print top routes
                if query_stats['top_routes']:
                    logger.info("  | TOP ROUTES:")
                    for i, route in enumerate(query_stats['top_routes'], 1):
                        logger.info(
                            f"  |   #{i}: {route['origin']}->{route['destination']} ({route['count']} segments)")

        # Print overall statistics
        self._print_within_divider(DividerType.DOUBLE, "OVERALL STATISTICS")
        logger.info(f"TOTAL FLIGHT SEGMENTS: {stats['total_segments']}")
        logger.info(f"TOTAL UNIQUE ITINERARIES: {stats['total_itineraries']}")

        if 'overall_avg_segments' in stats:
            logger.info(
                f"OVERALL AVERAGE SEGMENTS PER ITINERARY: {stats['overall_avg_segments']:.2f}")

        # Print glossary
        self._print_within_divider(DividerType.DOUBLE, "GLOSSARY")
        logger.info("SEGMENT: An individual flight leg (e.g., NYC->LAX)")
        logger.info(
            "ITINERARY: A complete journey option that may include multiple segments")
        logger.info(
            "BATCH: A group of airport codes processed together in a query")
        logger.info(
            f"BATCH SIZE: Number of airport codes per query ({self.batch_size})")
        logger.info(DividerType.DOUBLE.value)
        self._print_within_divider(
            DividerType.DOUBLE, "END OF EDUARDO FLIGHT STATISTICS REPORT")
        
        # End timing report generation
        self.performance_metrics['report_generation']['end_time'] = datetime.datetime.now()
        self.performance_metrics['report_generation']['duration'] = (
            self.performance_metrics['report_generation']['end_time'] - 
            self.performance_metrics['report_generation']['start_time']).total_seconds()
        
        # Log performance summary after the main report
        self._print_performance_summary()

    def _print_performance_summary(self) -> None:
        """Print a summary of performance metrics for statistics processing."""
        self._print_within_divider(DividerType.DOUBLE, "FLIGHT STATISTICS PERFORMANCE METRICS")
        
        total_time = sum(m['duration'] for m in self.performance_metrics.values() if m['duration'] is not None)
        
        logger.info(f"TOTAL PROCESSING TIME: {total_time:.4f} seconds")
        logger.info(DividerType.SINGLE.value)
        
        for operation, metrics in self.performance_metrics.items():
            if metrics['duration'] is not None:
                percentage = (metrics['duration'] / total_time) * 100 if total_time > 0 else 0
                logger.info(f"  {operation.upper()}: {metrics['duration']:.4f} seconds ({percentage:.1f}%)")
                
        # Add memory usage if available
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
            logger.info(DividerType.SINGLE.value)
            logger.info(f"MEMORY USAGE: {memory_mb:.2f} MB")
        except ImportError:
            pass
        
        logger.info(DividerType.DOUBLE.value)


class DataProcessingLogger:
    """
    Tracks and logs statistics about flight data as it moves through the processing pipeline.
    Provides visibility into data transformations at each step.
    """

    def __init__(self):
        """Initialize the processing logger with empty stage data."""
        self.stages = {}
        self.start_time = datetime.datetime.now()

    def log_stage_start(self, stage_name: str) -> None:
        """Log the start of a processing stage."""
        logger.info(DividerType.DOUBLE.value)
        logger.info(f"Starting: {stage_name.upper()}")
        logger.info(DividerType.DOUBLE.value)
        self.stages[stage_name] = {'start_time': datetime.datetime.now()}

    def log_stage_end(self, stage_name: str, df: pd.DataFrame) -> None:
        """
        Log the end of a processing stage and capture metrics about the resulting DataFrame.

        Args:
            stage_name: Name of the processing stage
            df: DataFrame resulting from this processing stage
        """
        end_time = datetime.datetime.now()
        duration = (
            end_time - self.stages[stage_name]['start_time']).total_seconds()

        self.stages[stage_name].update({
            'end_time': end_time,
            'duration': duration,
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': set(df.columns)
        })

        logger.info(
            f"Completed: {stage_name.upper()} in {duration:.2f} seconds")
        logger.info(f"Result: {len(df)} rows, {len(df.columns)} columns")
        logger.info(DividerType.SINGLE.value)

    def log_raw_data_cleaning(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> None:
        """
        Log statistics about the raw data cleaning process.

        Args:
            before_df: DataFrame before cleaning
            after_df: DataFrame after cleaning
        """
        self.log_stage_start("Clean Raw Flight Data")

        # Calculate cleaning statistics
        removed_rows = len(before_df) - len(after_df)
        empty_origins = before_df['Origin'].isna(
        ).sum() + (before_df['Origin'] == '').sum()

        logger.info(
            f"Initial raw data: {len(before_df)} rows, {len(before_df.columns)} columns")
        logger.info(
            f"Rows removed: {removed_rows} ({removed_rows/len(before_df)*100:.1f}% of original data)")
        logger.info(f"Empty origins found: {empty_origins}")
        logger.info(
            f"Unique flight types: {before_df['Flight Type'].nunique()}")
        logger.info(
            f"Unique solution IDs: {before_df['Solution ID'].nunique()}")

        self.log_stage_end("Clean Raw Flight Data", after_df)

    def log_data_enrichment(self, before_df: pd.DataFrame, after_df: pd.DataFrame, reference_data: Dict) -> None:
        """
        Log statistics about the data enrichment process.

        Args:
            before_df: DataFrame before enrichment
            after_df: DataFrame after enrichment
            reference_data: Dictionary of reference dataframes used for enrichment
        """
        self.log_stage_start("Enrich Flight Data")

        # Log reference data statistics
        logger.info("Reference data used for enrichment:")
        for name, df in reference_data.items():
            if isinstance(df, pd.DataFrame):
                logger.info(
                    f"  - {name}: {len(df)} rows, {df.shape[1]} columns")

        # Calculate enrichment statistics
        new_columns = set(after_df.columns) - set(before_df.columns)
        rows_before = len(before_df)
        rows_after = len(after_df)

        logger.info(f"New columns added: {len(new_columns)}")
        for col in sorted(new_columns):
            logger.info(f"  - {col}")

        if rows_before != rows_after:
            logger.info(f"Rows changed: {rows_before} → {rows_after} " +
                        f"({(rows_after-rows_before)/rows_before*100:.1f}% change)")
            if rows_after < rows_before:
                logger.info(
                    f"Warning: {rows_before - rows_after} rows were lost during enrichment")

        self.log_stage_end("Enrich Flight Data", after_df)

    def log_structure_transformation(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> None:
        """
        Log statistics about the structure transformation process.

        Args:
            before_df: DataFrame before transformation
            after_df: DataFrame after transformation (grouped by itinerary)
        """
        self.log_stage_start("Transform Flight Structure")

        # Calculate transformation statistics
        segments_before = len(before_df)
        itineraries_before = before_df['Solution ID'].nunique()
        itineraries_after = len(after_df)

        logger.info(
            f"Before transformation: {segments_before} flight segments, {itineraries_before} itineraries")
        logger.info(
            f"After transformation: {itineraries_after} itinerary rows")
        logger.info(
            f"Avg segments per itinerary: {segments_before/itineraries_before:.2f}")

        # Calculate stops distribution
        if 'Num Stops' in after_df.columns:
            stops_distribution = after_df['Num Stops'].value_counts(
            ).sort_index()
            logger.info("Flight distribution by number of stops:")
            for stops, count in stops_distribution.items():
                percentage = count / len(after_df) * 100
                logger.info(
                    f"  - {stops} stop(s): {count} itineraries ({percentage:.1f}%)")

        self.log_stage_end("Transform Flight Structure", after_df)

    def log_metrics_calculation(self, before_df: pd.DataFrame, after_df: pd.DataFrame) -> None:
        """
        Log statistics about the metrics calculation process.

        Args:
            before_df: DataFrame before metrics calculation
            after_df: DataFrame after metrics calculation
        """
        self.log_stage_start("Calculate Flight Metrics")

        # Calculate metrics statistics
        new_columns = set(after_df.columns) - set(before_df.columns)

        logger.info(f"New metrics calculated: {len(new_columns)}")
        for col in sorted(new_columns):
            logger.info(f"  - {col}")

        # If duration was calculated, show statistics
        if 'duration' in after_df.columns:
            avg_duration = after_df['duration'].mean()
            min_duration = after_df['duration'].min()
            max_duration = after_df['duration'].max()

            logger.info("Flight duration statistics (hours):")
            logger.info(f"  Average: {avg_duration:.2f}")
            logger.info(f"  Minimum: {min_duration:.2f}")
            logger.info(f"  Maximum: {max_duration:.2f}")

        self.log_stage_end("Calculate Flight Metrics", after_df)

    def log_database_save(self, row_count: int, success: bool, error_msg: Optional[str] = None) -> None:
        """
        Log statistics about the database save process.

        Args:
            row_count: Number of rows saved to database
            success: Whether the save was successful
            error_msg: Error message if save failed
        """
        self.log_stage_start("Save to Database")

        if success:
            logger.info(f"Successfully saved {row_count} rows to database")
        else:
            logger.info("Failed to save data to database")
            if error_msg:
                logger.info(f"Error: {error_msg}")

        logger.info(DividerType.SINGLE.value)

    def log_processing_summary(self) -> None:
        """Log a summary of the entire data processing pipeline."""
        end_time = datetime.datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()

        logger.info(DividerType.DOUBLE.value)
        logger.info("                DATA PROCESSING SUMMARY                ")
        logger.info(DividerType.DOUBLE.value)

        logger.info(f"Total processing time: {total_duration:.2f} seconds")
        logger.info("Stage durations:")

        for stage_name, stage_data in self.stages.items():
            if 'duration' in stage_data:
                percentage = stage_data['duration'] / total_duration * 100
                logger.info(
                    f"  - {stage_name}: {stage_data['duration']:.2f} seconds ({percentage:.1f}%)")

        if 'Calculate Flight Metrics' in self.stages:
            final_rows = self.stages['Calculate Flight Metrics'].get(
                'row_count', 0)
            logger.info(f"Final dataset: {final_rows} rows")

        logger.info(DividerType.DOUBLE.value)
        logger.info(
            "                END OF DATA PROCESSING SUMMARY                ")
        logger.info(DividerType.DOUBLE.value)
