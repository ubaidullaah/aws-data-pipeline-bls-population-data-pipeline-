"""
Lambda function for Part 3: Data Analytics and Reporting.
This function processes SQS messages triggered by S3 JSON file uploads.
It performs analytics on the BLS and population data and logs the results.
"""
import boto3
import pandas as pd
import json
import io
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Setup
s3 = boto3.client('s3')
bucket_name = os.environ.get('BUCKET_NAME', 'bls-dataset-sync2')


def load_csv_from_s3(bucket, key):
    """Download CSV file from S3 and return as pandas DataFrame"""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_content), sep='\t')  # BLS files are tab-separated
        return df
    except Exception as e:
        logger.error(f"Error loading CSV from S3: {e}")
        return None


def load_json_from_s3(bucket, key):
    """Download JSON file from S3 and return as pandas DataFrame"""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        json_content = response['Body'].read().decode('utf-8')
        data = json.loads(json_content)
        # Convert JSON to DataFrame
        df = pd.DataFrame(data.get('data', []))
        return df
    except Exception as e:
        logger.error(f"Error loading JSON from S3: {e}")
        return None


def find_files_in_s3():
    """Find the BLS CSV file and most recent population JSON file in S3."""
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' not in response:
            logger.warning("No files found in S3 bucket")
            return None, None
        
        files = [obj['Key'] for obj in response['Contents']]
        
        # Find the CSV file (pr.data.0.Current)
        csv_file = None
        for file in files:
            if 'pr.data.0.Current' in file or file == 'pr.data.0.Current':
                csv_file = file
                break
        
        # Find the most recent population JSON file
        json_files = [f for f in files if f.startswith('population_data_') and f.endswith('.json')]
        json_file = sorted(json_files)[-1] if json_files else None
        
        logger.info(f"Using CSV file: {csv_file}")
        logger.info(f"Using JSON file: {json_file}")
        
        return csv_file, json_file
    except Exception as e:
        logger.error(f"Error listing S3 files: {e}")
        return None, None


def calculate_population_stats(df_population):
    """Calculate mean and standard deviation of annual US population (2013-2018)."""
    if df_population is None or df_population.empty:
        logger.warning("Population data not available")
        return None
    
    # Identify Year and Population columns
    year_col = None
    pop_col = None
    
    for col in df_population.columns:
        col_lower = col.lower()
        if 'year' in col_lower:
            year_col = col
        if 'population' in col_lower:
            pop_col = col
    
    if not year_col or not pop_col:
        logger.warning("Could not identify Year or Population columns")
        return None
    
    # Filter data for years 2013-2018
    population_filtered = df_population[
        (df_population[year_col] >= 2013) & 
        (df_population[year_col] <= 2018)
    ].copy()
    
    if len(population_filtered) == 0:
        logger.warning("No data found for years 2013-2018")
        return None
    
    pop_values = population_filtered[pop_col].dropna()
    
    if len(pop_values) == 0:
        logger.warning("No valid population values found")
        return None
    
    mean_population = pop_values.mean()
    std_population = pop_values.std()
    
    stats = {
        'mean': float(mean_population),
        'std': float(std_population),
        'years': sorted(population_filtered[year_col].unique().tolist()),
        'count': len(pop_values)
    }
    
    logger.info("=" * 60)
    logger.info("POPULATION STATISTICS (2013-2018)")
    logger.info("=" * 60)
    logger.info(f"Mean Annual US Population: {mean_population:,.2f}")
    logger.info(f"Standard Deviation: {std_population:,.2f}")
    logger.info(f"Number of years: {len(pop_values)}")
    
    return stats


def find_best_years(df_bls):
    """For each series_id, find the best year (year with max sum of values across quarters)."""
    if df_bls is None or df_bls.empty:
        logger.warning("BLS data not available")
        return None
    
    # Clean column names
    df_bls.columns = df_bls.columns.str.strip()
    
    # Trim whitespaces from string columns
    string_columns = df_bls.select_dtypes(include=['object']).columns
    for col in string_columns:
        df_bls[col] = df_bls[col].str.strip()
    
    # Ensure required columns exist
    required_cols = ['series_id', 'year', 'period', 'value']
    missing_cols = [col for col in required_cols if col not in df_bls.columns]
    
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        return None
    
    # Filter out rows with missing values
    df_clean = df_bls[required_cols].dropna()
    
    # Group by series_id and year, sum the values for all quarters in each year
    yearly_sums = df_clean.groupby(['series_id', 'year'])['value'].sum().reset_index()
    
    # For each series_id, find the year with the maximum sum
    best_years = yearly_sums.loc[
        yearly_sums.groupby('series_id')['value'].idxmax()
    ].copy()
    
    # Sort by series_id
    best_years = best_years.sort_values('series_id').reset_index(drop=True)
    
    logger.info("=" * 60)
    logger.info("BEST YEAR REPORT FOR EACH SERIES ID")
    logger.info("=" * 60)
    logger.info(f"Total number of series: {len(best_years)}")
    
    # Log first 10 and last 10 rows
    logger.info("\nFirst 10 rows:")
    for idx, row in best_years.head(10).iterrows():
        logger.info(f"  {row['series_id']}: Year {row['year']}, Sum: {row['value']:.2f}")
    
    logger.info("\nLast 10 rows:")
    for idx, row in best_years.tail(10).iterrows():
        logger.info(f"  {row['series_id']}: Year {row['year']}, Sum: {row['value']:.2f}")
    
    # Convert to dict for logging
    result = best_years.to_dict('records')
    
    return result


def generate_combined_report(df_bls, df_population):
    """Generate report for series_id = PRS30006032, period = Q01 with population data."""
    if df_bls is None or df_bls.empty:
        logger.warning("BLS data not available")
        return None
    
    # Clean BLS data
    df_bls.columns = df_bls.columns.str.strip()
    string_columns = df_bls.select_dtypes(include=['object']).columns
    for col in string_columns:
        df_bls[col] = df_bls[col].str.strip()
    
    # Filter for series_id = PRS30006032 and period = Q01
    filtered_bls = df_bls[
        (df_bls['series_id'].str.strip() == 'PRS30006032') & 
        (df_bls['period'].str.strip() == 'Q01')
    ].copy()
    
    if len(filtered_bls) == 0:
        logger.warning("No records found for PRS30006032, Q01")
        return None
    
    logger.info(f"Found {len(filtered_bls)} records for PRS30006032, Q01")
    
    # Prepare population data for joining
    if df_population is not None and not df_population.empty:
        # Identify Year and Population columns
        year_col = None
        pop_col = None
        
        for col in df_population.columns:
            col_lower = col.lower()
            if 'year' in col_lower:
                year_col = col
            if 'population' in col_lower:
                pop_col = col
        
        if year_col and pop_col:
            # Create a clean population dataframe
            pop_df = df_population[[year_col, pop_col]].copy()
            pop_df = pop_df.rename(columns={year_col: 'year', pop_col: 'Population'})
            pop_df = pop_df.dropna()
            
            # Ensure year is integer for proper joining
            filtered_bls['year'] = filtered_bls['year'].astype(int)
            pop_df['year'] = pop_df['year'].astype(int)
            
            # Left join to add population data
            combined_report = filtered_bls.merge(
                pop_df,
                on='year',
                how='left'
            )
            
            # Select columns
            report_columns = ['series_id', 'year', 'period', 'value', 'Population']
            available_columns = [col for col in report_columns if col in combined_report.columns]
            combined_report = combined_report[available_columns]
            
            # Sort by year
            combined_report = combined_report.sort_values('year').reset_index(drop=True)
            
            logger.info("=" * 60)
            logger.info("COMBINED REPORT: PRS30006032, Q01 with Population")
            logger.info("=" * 60)
            
            # Log the report
            for idx, row in combined_report.iterrows():
                pop_str = f"{row['Population']:,.0f}" if pd.notna(row.get('Population')) else "N/A"
                logger.info(f"  {row['series_id']} | {row['year']} | {row['period']} | {row['value']} | {pop_str}")
            
            records_with_pop = combined_report['Population'].notna().sum()
            logger.info(f"\nTotal records: {len(combined_report)}")
            logger.info(f"Records with population data: {records_with_pop}")
            logger.info(f"Records without population data: {len(combined_report) - records_with_pop}")
            
            return combined_report.to_dict('records')
    
    # Return report without population if population data not available
    report_columns = ['series_id', 'year', 'period', 'value']
    available_columns = [col for col in report_columns if col in filtered_bls.columns]
    final_report = filtered_bls[available_columns].sort_values('year').reset_index(drop=True)
    
    logger.info("=" * 60)
    logger.info("COMBINED REPORT: PRS30006032, Q01 (without Population)")
    logger.info("=" * 60)
    
    for idx, row in final_report.iterrows():
        logger.info(f"  {row['series_id']} | {row['year']} | {row['period']} | {row['value']}")
    
    return final_report.to_dict('records')


def handler(event, context):
    """
    Main Lambda handler that processes SQS messages and generates reports.
    """
    logger.info("=" * 60)
    logger.info("Starting report processing")
    logger.info("=" * 60)
    
    try:
        # Process each SQS record
        for record in event['Records']:
            # Parse SQS message body (which contains S3 event)
            body = json.loads(record['body'])
            
            # Extract S3 event information
            if 'Records' in body:
                for s3_record in body['Records']:
                    bucket = s3_record['s3']['bucket']['name']
                    key = s3_record['s3']['object']['key']
                    
                    logger.info(f"Processing S3 event: {bucket}/{key}")
                    
                    # Only process JSON files
                    if not key.endswith('.json'):
                        logger.info(f"Skipping non-JSON file: {key}")
                        continue
                    
                    # Find files in S3
                    csv_file, json_file = find_files_in_s3()
                    
                    if not csv_file:
                        logger.error("BLS CSV file not found in S3")
                        continue
                    
                    # Load data
                    logger.info("Loading BLS CSV data...")
                    df_bls = load_csv_from_s3(bucket_name, csv_file)
                    
                    logger.info("Loading population JSON data...")
                    df_population = None
                    if json_file:
                        df_population = load_json_from_s3(bucket_name, json_file)
                    
                    # Generate reports
                    logger.info("\n" + "=" * 60)
                    logger.info("GENERATING REPORTS")
                    logger.info("=" * 60)
                    
                    # 1. Population statistics
                    pop_stats = calculate_population_stats(df_population)
                    
                    # 2. Best year report
                    best_years = find_best_years(df_bls)
                    
                    # 3. Combined report
                    combined_report = generate_combined_report(df_bls, df_population)
                    
                    logger.info("\n" + "=" * 60)
                    logger.info("REPORT PROCESSING COMPLETED")
                    logger.info("=" * 60)
                    
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'message': 'Reports generated successfully',
                            'population_stats': pop_stats,
                            'best_years_count': len(best_years) if best_years else 0,
                            'combined_report_count': len(combined_report) if combined_report else 0
                        })
                    }
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'No records to process'})
        }
        
    except Exception as e:
        logger.error(f"Error in handler: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error: {str(e)}'
            })
        }
