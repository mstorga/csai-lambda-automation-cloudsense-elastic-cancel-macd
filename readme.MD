# CloudSense Elastic Cancel MACD Request Automation

This Lambda function automates the cancellation of MACD (Move, Add, Change, Delete) requests in CloudSense Elastic. It queries MACD requests based on subscription IDs and updates their status to cancel pending orders.

## Overview

The automation performs the following operations:
1. Queries MACD requests matching provided subscription IDs
2. Filters records with status 'posted' (eligible for cancellation)
3. Updates `order_request` status to 'Error' for matching basket IDs
4. Updates `macd_request` status to 'posted1' for matching MACD IDs
5. Posts detailed results to the associated Kayako ticket

## Key Features

✅ **Subscription-based Query**: Find MACD requests by Salesforce subscription IDs  
✅ **Status Validation**: Only processes records with status 'posted'  
✅ **Multi-Region Support**: Supports multiple database regions (EU, APAC, US, etc.)  
✅ **Test Mode**: Rollback changes for safe testing without affecting production data  
✅ **Detailed Reporting**: Comprehensive summaries posted to Kayako tickets  
✅ **Error Handling**: Proper transaction handling with commit/rollback  

## Usage

### Request Format

```json
{
  "org_id": "00d20000000pcaj",
  "subscriptions": ["a1PTt0000019WL3", "a1PTt0000019WL4"],
  "region": "EU",
  "case_id": "12345678",
  "test": true
}
```

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `org_id` | Yes | Salesforce org ID (e.g., "00d20000000pcaj") |
| `subscriptions` | Yes | List of Salesforce subscription IDs (sfdc_id values) |
| `region` | Yes | Database region (e.g., "EU", "APAC", "US") |
| `case_id` | Yes | Kayako case ID for posting results |
| `test` | No | Boolean flag for test mode (default: false) |

### Test Mode

- **`"test": false`** (default): **PRODUCTION MODE** - Changes are committed to database
- **`"test": true`**: **TEST MODE** - All changes are rolled back (no permanent modifications)

**Test mode benefits:**
- ✅ **Safe Testing**: Validate operations without permanent changes
- ✅ **Dry Run**: See exactly what would happen in production
- ✅ **Debugging**: Test configurations safely
- ✅ **Clear Indicators**: All logs and responses clearly show test mode status

## Database Configuration

Configure database regions in your SSM parameter (`CloudsenseElasticCancelMACDRequest_parameters`):

```json
{
  "kayako_domain": "central-supportdesk",
  "kayako_email": "your_email@cloudsense.com",
  "kayako_password": "base64_encoded_password",
  
  "databases": {
    "EU": {
      "sm_db_name": "your_eu_db_name",
      "sm_db_user": "your_eu_db_user",
      "sm_db_password": "your_eu_db_password",
      "sm_db_host": "your_eu_db_host",
      "sm_db_port": "5432"
    },
    "APAC": {
      "sm_db_name": "your_apac_db_name",
      "sm_db_user": "your_apac_db_user",
      "sm_db_password": "your_apac_db_password",
      "sm_db_host": "your_apac_db_host",
      "sm_db_port": "5432"
    },
    "US": {
      "sm_db_name": "your_us_db_name",
      "sm_db_user": "your_us_db_user",
      "sm_db_password": "your_us_db_password",
      "sm_db_host": "your_us_db_host",
      "sm_db_port": "5432"
    }
  }
}
```

## Response Format

```json
{
  "message": "MACD request cancellation completed",
  "macd_requests_found": 5,
  "eligible_for_update": 3,
  "skipped_wrong_status": 2,
  "order_requests_updated": 3,
  "macd_requests_updated": 3,
  "case_id": "12345678",
  "test_mode": true,
  "committed": false
}
```

## Prerequisites

1. **AWS Lambda and SAM**:
   - Install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
   - Install the [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
   - Configure AWS CLI: `aws configure`

2. **AWS Permissions**:
   - You must belong to `RAM-AWS-CoreSupport-Admin` AD Group

3. **Python**: Install Python 3.x

## Building and Deploying

### Build the Code

```bash
sam build
```

### Deploy the Code

```bash
sam deploy
```

## Code Structure

- `app.py` - Main Lambda handler with MACD cancellation logic
- `help_center_connection.py` - Kayako integration for posting results
- `kayako_connection.py` - Kayako API connection utilities
- `requirements.txt` - Python dependencies

## Support

For issues or questions, contact the CloudSense support team.
