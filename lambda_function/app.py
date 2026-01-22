import ast
import json
import os
import boto3
import psycopg2
import help_center_connection

TAG_NAME = "CloudsenseElasticCancelMACDRequest"
PARAMETER_NAME = f"{TAG_NAME}_parameters"
PARAMETERS = None


def lambda_handler(event, _):
    """
    Lambda handler for CloudsenseElasticCancelMACDRequest
    
    Input parameters:
    - org_id: Salesforce org ID (e.g., "00d20000000pcaj")
    - subscriptions: List of Salesforce subscription IDs (sfdc_id values)
    - region: Database region (e.g., "EU")
    - case_id: Kayako case ID for posting results
    - test: Boolean flag for test mode (optional, default False)
    """
    try:
        global PARAMETERS
        PARAMETERS = PARAMETERS or load_parameters(PARAMETER_NAME)
        
        # Update environment variables
        for key, value in PARAMETERS.items():
            if isinstance(value, dict):
                os.environ[key] = json.dumps(value)
            else:
                os.environ[key] = str(value)

        # Parse request body
        body = event.get("body", None)
        if body is None:
            body = event
        
        if not isinstance(body, dict):
            try:
                body = json.loads(body) if isinstance(body, str) else json.loads(str(body))
            except json.JSONDecodeError as e:
                return {
                    'statusCode': 400,
                    'body': json.dumps({"message": f"Invalid JSON: {str(e)}"})
                }

        print(f"📥 Parsed request body: {body}")

        # Extract query string parameters (for ?test_mode=true style invocation)
        query_params = event.get('queryStringParameters') or {}
        
        # Extract input parameters
        org_id = body.get('org_id')
        subscriptions = parse_list_parameter(body.get('subscriptions', []))
        region = body.get('region')
        case_id = body.get('case_id')
        # Check both body 'test' and query param 'test_mode'
        test_mode = parse_bool_parameter(
            body.get('test') or query_params.get('test_mode') or False
        )

        # Validate required parameters
        validation_errors = validate_inputs(org_id, subscriptions, region, case_id)
        if validation_errors:
            error_msg = "Validation errors: " + "; ".join(validation_errors)
            print(f"❌ {error_msg}")
            return {
                'statusCode': 400,
                'body': json.dumps({"message": error_msg})
            }

        print(f"🔧 Processing MACD request cancellation")
        print(f"   Org ID: {org_id}")
        print(f"   Subscriptions: {subscriptions}")
        print(f"   Region: {region}")
        print(f"   Case ID: {case_id}")
        print(f"   Test Mode: {test_mode}")

        # Get database configuration
        try:
            db_config = get_database_config(region)
        except ValueError as e:
            error_msg = str(e)
            print(f"❌ Database config error: {error_msg}")
            return {
                'statusCode': 500,
                'body': json.dumps({"message": error_msg})
            }

        # Initialize Kayako connection
        hc_agent = help_center_connection.HelpCenterConnect(is_kayako=True)

        # Execute query and perform updates
        try:
            result = process_macd_cancellation(db_config, org_id, subscriptions, test_mode)
            
            # Format results for Kayako
            result_text = format_results(result, org_id, subscriptions, test_mode)
            
            # Post results to Kayako ticket
            print(f"📝 Posting results to Kayako case {case_id}")
            hc_agent.write_internal_note(case_id, result_text)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    "message": "MACD request cancellation completed",
                    "macd_requests_found": result['total_found'],
                    "eligible_for_update": result['eligible_count'],
                    "skipped_wrong_status": result['skipped_wrong_status'],
                    "order_requests_updated": result['order_requests_updated'],
                    "macd_requests_updated": result['macd_requests_updated'],
                    "case_id": case_id,
                    "test_mode": test_mode,
                    "committed": result['committed']
                })
            }
            
        except Exception as e:
            error_msg = f"Operation failed: {str(e)}"
            print(f"❌ {error_msg}")
            import traceback
            print(f"📋 Traceback: {traceback.format_exc()}")
            
            # Post error to Kayako ticket
            hc_agent.write_internal_note(case_id, f"MACD Request Cancellation Failed:\n{error_msg}")
            
            return {
                'statusCode': 500,
                'body': json.dumps({"message": error_msg})
            }

    except Exception as e:
        error_msg = f"Lambda execution error: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        print(f"📋 Traceback: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({"message": error_msg})
        }


def parse_bool_parameter(value):
    """
    Parse a parameter that should be a boolean but might be sent as a string.
    
    Handles cases like:
    - Already a bool: True/False -> True/False
    - String "true"/"false" (case-insensitive): "true", "True", "TRUE" -> True
    - String "1"/"0": "1" -> True, "0" -> False
    - Integer 1/0: 1 -> True, 0 -> False
    - None: None -> False
    
    Args:
        value: The parameter value (bool, string, int, or None)
    
    Returns:
        A proper Python boolean
    """
    if value is None:
        return False
    
    if isinstance(value, bool):
        return value
    
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes')
    
    if isinstance(value, (int, float)):
        return bool(value)
    
    return False


def parse_list_parameter(value):
    """
    Parse a parameter that should be a list but might be sent as a string.
    
    Handles cases like:
    - Already a list: ['a', 'b'] -> ['a', 'b']
    - String representation of list: "['a', 'b']" -> ['a', 'b']
    - JSON array string: '["a", "b"]' -> ['a', 'b']
    - Empty/None: None -> []
    
    Args:
        value: The parameter value (list, string, or None)
    
    Returns:
        A proper Python list
    """
    if value is None:
        return []
    
    if isinstance(value, list):
        return value
    
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return []
        
        # Try parsing as JSON first (handles '["a", "b"]' format)
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
        
        # Try parsing Python literal (handles "['a', 'b']" format with single quotes)
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return parsed
        except (ValueError, SyntaxError):
            pass
        
        # If it looks like a list but couldn't be parsed, return as single-item list
        # This handles edge cases where someone passes a single ID as a string
        return [value]
    
    # For any other type, wrap in a list
    return [value]


def validate_inputs(org_id, subscriptions, region, case_id):
    """Validate required input parameters"""
    errors = []
    
    if not org_id:
        errors.append("Missing required field: org_id")
    
    if not subscriptions:
        errors.append("Missing required field: subscriptions (must be a non-empty list)")
    elif not isinstance(subscriptions, list):
        errors.append("subscriptions must be a list of Salesforce IDs")
    elif len(subscriptions) == 0:
        errors.append("subscriptions list cannot be empty")
    
    if not region:
        errors.append("Missing required field: region")
    
    if not case_id:
        errors.append("Missing required field: case_id")
    
    return errors


def get_database_config(region):
    """
    Get database configuration based on region from SSM parameters
    """
    region = region.upper()
    
    databases_config = os.environ.get('databases')
    
    if databases_config:
        try:
            databases = json.loads(databases_config) if isinstance(databases_config, str) else databases_config
            
            if region not in databases:
                available_regions = list(databases.keys())
                raise ValueError(f"Unsupported database region: {region}. Supported regions: {available_regions}")
            
            region_config = databases[region]
            db_config = {
                'dbname': region_config.get('sm_db_name'),
                'user': region_config.get('sm_db_user'),
                'password': region_config.get('sm_db_password'),
                'host': region_config.get('sm_db_host'),
                'port': region_config.get('sm_db_port', 5432)
            }
            
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid databases configuration: {str(e)}")
    else:
        raise ValueError(f"No databases configuration found in SSM parameters")
    
    # Check for missing credentials
    missing_params = [key for key, value in db_config.items() if not value and key != 'port']
    if missing_params:
        raise ValueError(f"Missing database configuration parameters for {region}: {', '.join(missing_params)}")
    
    return db_config


def process_macd_cancellation(db_config, org_id, subscriptions, test_mode):
    """
    Query MACD requests and perform cancellation updates
    
    Args:
        db_config: Database connection configuration
        org_id: Salesforce org ID
        subscriptions: List of Salesforce subscription IDs (sfdc_id values)
        test_mode: If True, rollback changes instead of committing
    
    Returns:
        Dictionary with operation results
    """
    conn = None
    result = {
        'total_found': 0,
        'eligible_count': 0,
        'skipped_wrong_status': 0,
        'order_requests_updated': 0,
        'macd_requests_updated': 0,
        'committed': False,
        'macd_records': [],
        'skipped_records': [],
        'updated_basket_ids': [],
        'updated_macd_ids': []
    }
    
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        # Clean org_id (remove quotes if present)
        clean_org_id = org_id.strip('"').lower()
        schema_name = f"org_{clean_org_id}"
        
        # Build the query with parameterized subscription IDs
        like_patterns = [f"{sub_id}%" for sub_id in subscriptions]
        like_conditions = " OR ".join(["sfdc_id LIKE %s" for _ in like_patterns])
        
        # Query MACD requests
        query = f"""
            SELECT id, basket_id, status
            FROM {schema_name}.macd_request
            WHERE subscription_id IN (
                SELECT id 
                FROM {schema_name}.subscription 
                WHERE {like_conditions}
            )
        """
        
        print(f"🔍 Executing query with {len(subscriptions)} subscription patterns")
        cur.execute(query, like_patterns)
        
        rows = cur.fetchall()
        result['total_found'] = len(rows)
        print(f"✅ Query returned {len(rows)} MACD request records")
        
        # Separate eligible records (status = 'posted') from ineligible ones
        eligible_records = []
        for row in rows:
            macd_id, basket_id, status = row
            record = {'id': macd_id, 'basket_id': basket_id, 'status': status}
            
            if status == 'posted':
                eligible_records.append(record)
                result['macd_records'].append(record)
            else:
                result['skipped_records'].append(record)
                result['skipped_wrong_status'] += 1
                print(f"⚠️ Skipping MACD request {macd_id} - status is '{status}' (expected 'posted')")
        
        result['eligible_count'] = len(eligible_records)
        
        if not eligible_records:
            print("ℹ️ No eligible MACD requests found (none with status 'posted')")
            cur.close()
            return result
        
        # Extract basket_ids and macd_ids for updates
        basket_ids = [r['basket_id'] for r in eligible_records if r['basket_id']]
        macd_ids = [r['id'] for r in eligible_records if r['id']]
        
        # Remove duplicates while preserving order
        basket_ids = list(dict.fromkeys(basket_ids))
        macd_ids = list(dict.fromkeys(macd_ids))
        
        result['updated_basket_ids'] = basket_ids
        result['updated_macd_ids'] = macd_ids
        
        # Update order_request status to 'Error' for matching basket_ids
        if basket_ids:
            basket_placeholders = ','.join(['%s'] * len(basket_ids))
            update_order_query = f"""
                UPDATE {schema_name}.order_request 
                SET status = 'Error' 
                WHERE basket_id IN ({basket_placeholders})
            """
            print(f"🔄 Updating order_request status to 'Error' for {len(basket_ids)} basket_ids")
            cur.execute(update_order_query, basket_ids)
            result['order_requests_updated'] = cur.rowcount
            print(f"✅ Updated {result['order_requests_updated']} order_request records")
        
        # Update macd_request status to 'posted1' for matching ids
        if macd_ids:
            macd_placeholders = ','.join(['%s'] * len(macd_ids))
            update_macd_query = f"""
                UPDATE {schema_name}.macd_request 
                SET status = 'posted1' 
                WHERE id IN ({macd_placeholders})
            """
            print(f"🔄 Updating macd_request status to 'posted1' for {len(macd_ids)} ids")
            cur.execute(update_macd_query, macd_ids)
            result['macd_requests_updated'] = cur.rowcount
            print(f"✅ Updated {result['macd_requests_updated']} macd_request records")
        
        # Commit or rollback based on test_mode
        if test_mode:
            conn.rollback()
            result['committed'] = False
            print("🧪 TEST MODE: All changes rolled back")
        else:
            conn.commit()
            result['committed'] = True
            print("✅ PRODUCTION MODE: All changes committed")
        
        cur.close()
        return result
        
    except Exception as e:
        print(f"❌ Database operation error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def format_results(result, org_id, subscriptions, test_mode):
    """
    Format operation results for posting to Kayako
    """
    lines = []
    
    if test_mode:
        lines.append("🧪 TEST MODE - Changes were NOT committed (rolled back)")
    else:
        lines.append("✅ PRODUCTION MODE - Changes were committed")
    
    lines.append("")
    lines.append("MACD Request Cancellation Summary")
    lines.append("==================================")
    lines.append(f"Org ID: {org_id}")
    lines.append(f"Subscriptions queried: {len(subscriptions)}")
    lines.append("")
    
    lines.append("Results:")
    lines.append(f"  - Total MACD requests found: {result['total_found']}")
    lines.append(f"  - Eligible for update (status='posted'): {result['eligible_count']}")
    lines.append(f"  - Skipped (wrong status): {result['skipped_wrong_status']}")
    lines.append("")
    
    lines.append("Updates Performed:")
    lines.append(f"  - order_request records updated to 'Error': {result['order_requests_updated']}")
    lines.append(f"  - macd_request records updated to 'posted1': {result['macd_requests_updated']}")
    lines.append("")
    
    if result['skipped_records']:
        lines.append("Skipped Records (wrong status):")
        for record in result['skipped_records']:
            lines.append(f"  - ID: {record['id']}, Status: {record['status']}")
        lines.append("")
    
    if result['updated_basket_ids']:
        lines.append("Basket IDs affected:")
        for basket_id in result['updated_basket_ids']:
            lines.append(f"  - {basket_id}")
        lines.append("")
    
    if result['updated_macd_ids']:
        lines.append("MACD Request IDs updated:")
        for macd_id in result['updated_macd_ids']:
            lines.append(f"  - {macd_id}")
        lines.append("")
    
    if test_mode:
        lines.append("⚠️ NOTE: This was a TEST run. No actual changes were made to the database.")
    else:
        lines.append("✅ All changes have been committed to the database.")
    
    return "\n".join(lines)


def load_parameters(parameter_name):
    """Load parameters from SSM Parameter Store"""
    response = boto3.client("ssm", region_name="us-east-1").get_parameter(
        Name=parameter_name, WithDecryption=True
    )
    return json.loads(response["Parameter"]["Value"])


if __name__ == '__main__':
    # Sample payload for testing
    sample_payload = {
        'org_id': '00d20000000pcaj',
        'subscriptions': ['a1PTt0000019WL3', 'a1PTt0000019WL4'],
        'region': 'EU',
        'case_id': '12345678',
        'test': True
    }
    
    lambda_handler(sample_payload, None)
