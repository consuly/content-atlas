# Workflow Management System

## Overview

The Workflow Management System enables users to create, manage, and execute automated data analysis workflows. A workflow is a series of LLM-powered SQL query steps with configurable variables, allowing users to create reusable data analysis templates that can be executed with different parameters.

## Key Concepts

### Workflows
A workflow consists of:
- **Name and Description**: Identifies the workflow and its purpose
- **Steps**: Ordered sequence of LLM prompts that generate SQL queries
- **Variables**: Configurable parameters (dates, filters, thresholds) that users can change when executing the workflow
- **Metadata**: Creation info, active status, timestamps

### Steps
Each step in a workflow:
- Has an order (1, 2, 3, ...)
- Contains an LLM prompt template with `{{variable}}` placeholders
- Generates a SQL query when executed
- Can reference results from previous steps (context passing)

### Variables
Variables make workflows reusable by allowing users to customize:
- **Date ranges**: `start_date`, `end_date`
- **Filters**: `client_name`, `product_category`, `region`
- **Thresholds**: `min_revenue`, `top_n`, `limit`
- **Options**: Dropdown selections for predefined choices

### Variable Types
- **text**: Free-form text input (names, descriptions, search terms)
- **date**: Date picker (YYYY-MM-DD format)
- **number**: Numeric input (integers or decimals)
- **select**: Dropdown with predefined options

## Database Schema

The workflow system uses 5 PostgreSQL tables:

### workflows
```sql
CREATE TABLE workflows (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_by VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);
```

### workflow_steps
```sql
CREATE TABLE workflow_steps (
    id UUID PRIMARY KEY,
    workflow_id UUID REFERENCES workflows(id) ON DELETE CASCADE,
    step_order INTEGER NOT NULL,
    name VARCHAR(255),
    prompt_template TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(workflow_id, step_order)
);
```

### workflow_variables
```sql
CREATE TABLE workflow_variables (
    id UUID PRIMARY KEY,
    workflow_id UUID REFERENCES workflows(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    display_name VARCHAR(255),
    variable_type VARCHAR(50) DEFAULT 'text',
    default_value TEXT,
    options JSONB,
    required BOOLEAN DEFAULT TRUE,
    UNIQUE(workflow_id, name)
);
```

### workflow_executions
```sql
CREATE TABLE workflow_executions (
    id UUID PRIMARY KEY,
    workflow_id UUID REFERENCES workflows(id),
    executed_by VARCHAR(255),
    executed_at TIMESTAMP DEFAULT NOW(),
    status VARCHAR(50) DEFAULT 'running',
    variables_used JSONB,
    completed_at TIMESTAMP,
    error_message TEXT
);
```

### workflow_step_results
```sql
CREATE TABLE workflow_step_results (
    id UUID PRIMARY KEY,
    execution_id UUID REFERENCES workflow_executions(id) ON DELETE CASCADE,
    step_id UUID REFERENCES workflow_steps(id),
    step_order INTEGER,
    executed_sql TEXT,
    result_csv TEXT,
    rows_returned INTEGER,
    execution_time_seconds FLOAT,
    status VARCHAR(50),
    error_message TEXT,
    executed_at TIMESTAMP DEFAULT NOW()
);
```

## API Endpoints

### Create Workflow
**POST** `/workflows`

Create a new workflow with steps and variables.

**Request Body:**
```json
{
  "name": "Monthly Revenue Report",
  "description": "Generate revenue analysis for a time period",
  "steps": [
    {
      "step_order": 1,
      "name": "Total Revenue",
      "prompt_template": "Calculate total revenue between {{start_date}} and {{end_date}}"
    },
    {
      "step_order": 2,
      "name": "Revenue by Client",
      "prompt_template": "Show revenue breakdown by client for period {{start_date}} to {{end_date}}"
    }
  ],
  "variables": [
    {
      "name": "start_date",
      "display_name": "Start Date",
      "variable_type": "date",
      "required": true
    },
    {
      "name": "end_date",
      "display_name": "End Date",
      "variable_type": "date",
      "required": true
    }
  ],
  "created_by": "user@example.com"
}
```

**Response:**
```json
{
  "success": true,
  "workflow_id": "550e8400-e29b-41d4-a716-446655440000",
  "message": "Workflow 'Monthly Revenue Report' created successfully"
}
```

### List Workflows
**GET** `/workflows?limit=50&offset=0&active_only=true`

List all workflows with pagination.

**Response:**
```json
{
  "success": true,
  "workflows": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "Monthly Revenue Report",
      "description": "Generate revenue analysis for a time period",
      "created_by": "user@example.com",
      "created_at": "2025-01-15T10:30:00Z",
      "updated_at": "2025-01-15T10:30:00Z",
      "is_active": true,
      "step_count": 2,
      "variable_count": 2
    }
  ],
  "total_count": 1
}
```

### Get Workflow Details
**GET** `/workflows/{workflow_id}`

Get detailed information about a specific workflow including all steps and variables.

**Response:**
```json
{
  "success": true,
  "workflow": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Monthly Revenue Report",
    "description": "Generate revenue analysis for a time period",
    "created_by": "user@example.com",
    "created_at": "2025-01-15T10:30:00Z",
    "updated_at": "2025-01-15T10:30:00Z",
    "is_active": true,
    "steps": [
      {
        "id": "660e8400-e29b-41d4-a716-446655440001",
        "step_order": 1,
        "name": "Total Revenue",
        "prompt_template": "Calculate total revenue between {{start_date}} and {{end_date}}"
      }
    ],
    "variables": [
      {
        "id": "770e8400-e29b-41d4-a716-446655440002",
        "name": "start_date",
        "display_name": "Start Date",
        "variable_type": "date",
        "default_value": null,
        "options": null,
        "required": true
      }
    ]
  }
}
```

### Update Workflow
**PUT** `/workflows/{workflow_id}`

Update workflow metadata (name, description, active status).

**Request Body:**
```json
{
  "name": "Updated Workflow Name",
  "description": "Updated description",
  "is_active": false
}
```

### Delete Workflow
**DELETE** `/workflows/{workflow_id}`

Delete a workflow and all associated data (steps, variables, executions).

### Execute Workflow
**POST** `/workflows/{workflow_id}/execute`

Execute a workflow with specific variable values.

**Request Body:**
```json
{
  "variables": {
    "start_date": "2025-01-01",
    "end_date": "2025-01-31",
    "client_name": "Acme Corp"
  },
  "executed_by": "user@example.com",
  "include_context": true
}
```

**Response:**
```json
{
  "success": true,
  "execution_id": "880e8400-e29b-41d4-a716-446655440003",
  "workflow_id": "550e8400-e29b-41d4-a716-446655440000",
  "workflow_name": "Monthly Revenue Report",
  "status": "completed",
  "variables_used": {
    "start_date": "2025-01-01",
    "end_date": "2025-01-31"
  },
  "step_results": [
    {
      "step_order": 1,
      "step_name": "Total Revenue",
      "executed_sql": "SELECT SUM(amount) FROM invoices WHERE date BETWEEN '2025-01-01' AND '2025-01-31'",
      "result_csv": "total\n150000.00",
      "rows_returned": 1,
      "execution_time_seconds": 0.5,
      "status": "success",
      "error_message": null,
      "response": "The total revenue for January 2025 is $150,000.00"
    }
  ],
  "total_execution_time_seconds": 0.5
}
```

### List Workflow Executions
**GET** `/workflows/{workflow_id}/executions?limit=50&offset=0`

List execution history for a specific workflow.

### Get Execution Details
**GET** `/workflows/executions/{execution_id}`

Get detailed results from a specific workflow execution.

### Generate Workflow from Description
**POST** `/workflows/generate`

Use LLM to automatically generate a workflow from a natural language description.

**Request Body:**
```json
{
  "description": "Create a workflow to analyze monthly revenue by client with date filters"
}
```

**Response:**
```json
{
  "success": true,
  "workflow": {
    "name": "Monthly Revenue by Client Analysis",
    "description": "Comprehensive revenue analysis broken down by client for a specified time period",
    "steps": [
      {
        "step_order": 1,
        "name": "Total Revenue",
        "prompt_template": "Calculate total revenue from invoices between {{start_date}} and {{end_date}}"
      },
      {
        "step_order": 2,
        "name": "Revenue by Client",
        "prompt_template": "Show revenue breakdown by client for invoices between {{start_date}} and {{end_date}}, ordered by revenue descending"
      }
    ],
    "variables": [
      {
        "name": "start_date",
        "display_name": "Start Date",
        "variable_type": "date",
        "required": true
      },
      {
        "name": "end_date",
        "display_name": "End Date",
        "variable_type": "date",
        "required": true
      }
    ]
  },
  "llm_response": "I've created a workflow that analyzes revenue by client..."
}
```

## Usage Examples

### Example 1: Monthly Revenue Report

This workflow generates a comprehensive revenue analysis for any time period.

**Workflow Definition:**
```json
{
  "name": "Monthly Revenue Report",
  "description": "Generate comprehensive revenue analysis for a time period",
  "steps": [
    {
      "step_order": 1,
      "name": "Total Revenue",
      "prompt_template": "Calculate total revenue from invoices table between {{start_date}} and {{end_date}}"
    },
    {
      "step_order": 2,
      "name": "Revenue by Client",
      "prompt_template": "Show revenue breakdown by client for invoices between {{start_date}} and {{end_date}}, ordered by revenue descending"
    },
    {
      "step_order": 3,
      "name": "Top Clients",
      "prompt_template": "List the top {{top_n}} clients by revenue for the period {{start_date}} to {{end_date}}"
    }
  ],
  "variables": [
    {
      "name": "start_date",
      "display_name": "Start Date",
      "variable_type": "date",
      "required": true
    },
    {
      "name": "end_date",
      "display_name": "End Date",
      "variable_type": "date",
      "required": true
    },
    {
      "name": "top_n",
      "display_name": "Number of Top Clients",
      "variable_type": "number",
      "default_value": "10",
      "required": false
    }
  ]
}
```

**Execution:**
```json
{
  "variables": {
    "start_date": "2025-01-01",
    "end_date": "2025-01-31",
    "top_n": 5
  },
  "executed_by": "analyst@company.com",
  "include_context": true
}
```

### Example 2: Client Performance Dashboard

This workflow analyzes a specific client's performance across multiple metrics.

**Workflow Definition:**
```json
{
  "name": "Client Performance Dashboard",
  "description": "Comprehensive performance analysis for a specific client",
  "steps": [
    {
      "step_order": 1,
      "name": "Client Overview",
      "prompt_template": "Get basic information and total revenue for client {{client_name}}"
    },
    {
      "step_order": 2,
      "name": "Monthly Trend",
      "prompt_template": "Show monthly revenue trend for {{client_name}} over the last {{months}} months"
    },
    {
      "step_order": 3,
      "name": "Product Breakdown",
      "prompt_template": "Break down {{client_name}}'s purchases by product category"
    }
  ],
  "variables": [
    {
      "name": "client_name",
      "display_name": "Client Name",
      "variable_type": "text",
      "required": true
    },
    {
      "name": "months",
      "display_name": "Number of Months",
      "variable_type": "number",
      "default_value": "12",
      "required": false
    }
  ]
}
```

### Example 3: Regional Sales Report with Filters

This workflow uses select variables for predefined options.

**Workflow Definition:**
```json
{
  "name": "Regional Sales Report",
  "description": "Sales analysis filtered by region and time period",
  "steps": [
    {
      "step_order": 1,
      "name": "Regional Summary",
      "prompt_template": "Calculate total sales for {{region}} region between {{start_date}} and {{end_date}}"
    },
    {
      "step_order": 2,
      "name": "Top Products",
      "prompt_template": "List top {{limit}} products sold in {{region}} during the period"
    }
  ],
  "variables": [
    {
      "name": "region",
      "display_name": "Region",
      "variable_type": "select",
      "options": ["North America", "Europe", "Asia Pacific", "Latin America"],
      "required": true
    },
    {
      "name": "start_date",
      "display_name": "Start Date",
      "variable_type": "date",
      "required": true
    },
    {
      "name": "end_date",
      "display_name": "End Date",
      "variable_type": "date",
      "required": true
    },
    {
      "name": "limit",
      "display_name": "Number of Products",
      "variable_type": "number",
      "default_value": "10",
      "required": false
    }
  ]
}
```

## Architecture

### Components

1. **Models Layer** (`app/domain/workflows/models.py`)
   - Database CRUD operations
   - Workflow, step, variable, and execution management
   - PostgreSQL interactions via SQLAlchemy

2. **Executor Layer** (`app/domain/workflows/executor.py`)
   - Variable substitution in prompt templates
   - Variable validation
   - Workflow execution orchestration
   - Context passing between steps
   - Integration with LLM query agent

3. **Generator Layer** (`app/domain/workflows/generator.py`)
   - LLM-powered workflow generation
   - LangChain agent for analyzing database schema
   - Automatic step and variable identification
   - Natural language to workflow conversion

4. **API Layer** (`app/api/routers/workflows.py`)
   - REST API endpoints
   - Request/response validation
   - Error handling
   - Pydantic schema validation

### Execution Flow

1. **User triggers execution** with variable values
2. **System validates** required variables are provided
3. **For each step in order:**
   - Substitute variables into prompt template
   - Add context from previous steps (if enabled)
   - Send prompt to LLM query agent
   - Agent generates and executes SQL
   - Store step results in database
4. **Mark execution as completed**
5. **Return all step results** to user

### Context Passing

When `include_context=true`, each step receives results from previous steps:

```
Step 1 Prompt: "Calculate total revenue for {{client}}"
Step 1 Result: "Total revenue: $150,000"

Step 2 Prompt: "Show revenue breakdown by product for {{client}}"
Step 2 Context: "Context from previous steps:
  Step 1 (Total Revenue): Total revenue: $150,000"
Step 2 Full Prompt: [Context] + [Step 2 Prompt]
```

This allows later steps to reference and build upon earlier results.

## Best Practices

### Workflow Design

1. **Keep steps focused**: Each step should perform one clear query
2. **Order logically**: Start with summaries, then details, then analysis
3. **Use clear names**: Step names should describe what they do
4. **Design for reusability**: Use variables for anything that might change

### Variable Design

1. **Use descriptive names**: `start_date` not `d1`
2. **Provide display names**: "Start Date" for user-friendly UI
3. **Set sensible defaults**: For optional variables
4. **Use select for fixed options**: Better UX than free text
5. **Mark required appropriately**: Only require what's truly necessary

### Prompt Templates

1. **Be specific**: "Calculate total revenue from invoices table" not "get revenue"
2. **Include table names**: Helps LLM generate accurate SQL
3. **Use clear variable syntax**: `{{variable_name}}` with descriptive names
4. **Consider context**: Later steps can reference earlier results
5. **Test prompts**: Execute workflow with sample data to verify

### Performance

1. **Limit result sets**: Use TOP/LIMIT in prompts when appropriate
2. **Avoid unnecessary steps**: Combine related queries when possible
3. **Use indexes**: Ensure database tables are properly indexed
4. **Monitor execution times**: Review step results for slow queries

## Security Considerations

1. **Read-only queries**: Workflow steps only execute SELECT queries
2. **System table protection**: Cannot query workflow system tables
3. **Variable validation**: All variables validated before execution
4. **SQL injection prevention**: LLM generates SQL, not direct user input
5. **Execution tracking**: All executions logged with user attribution

## Testing

The workflow system includes comprehensive tests in `tests/test_workflows.py`:

- Variable substitution and validation
- Workflow CRUD operations
- Execution with mocked LLM responses
- Context passing between steps
- Error handling and edge cases
- API endpoint testing
- LLM workflow generation

Run tests:
```bash
pytest tests/test_workflows.py -v
```

## Troubleshooting

### Common Issues

**Issue**: Workflow execution fails with "Variable required_var is required"
**Solution**: Ensure all required variables are provided in the execution request

**Issue**: Step generates incorrect SQL
**Solution**: Make prompt template more specific, include table names and column details

**Issue**: Context not passed between steps
**Solution**: Set `include_context: true` in execution request

**Issue**: Workflow with no steps rejected
**Solution**: Workflows must have at least one step (validated at API level)

**Issue**: Variable options not showing in UI
**Solution**: Ensure variable_type is "select" and options array is provided

## Future Enhancements

Potential improvements for the workflow system:

1. **Scheduling**: Automatic workflow execution on schedule
2. **Notifications**: Email/webhook notifications on completion
3. **Versioning**: Track workflow versions and changes
4. **Sharing**: Share workflows between users/teams
5. **Templates**: Pre-built workflow templates for common use cases
6. **Conditional steps**: Execute steps based on previous results
7. **Parallel execution**: Run independent steps in parallel
8. **Export formats**: Export results to PDF, Excel, etc.
9. **Visualization**: Built-in charts and graphs for results
10. **Permissions**: Role-based access control for workflows

## Related Documentation

- [API Reference](API_REFERENCE.md) - Complete API documentation
- [Console](CONSOLE.md) - Natural language query interface
- [Architecture](ARCHITECTURE.md) - System architecture overview
- [Testing](TESTING.md) - Testing guidelines and practices
