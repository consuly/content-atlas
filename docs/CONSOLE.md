# Console Interface Guide

Interactive console for running natural language database queries.

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
- [Features](#features)
- [Commands](#commands)
- [Usage Examples](#usage-examples)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

---

## Overview

The ContentAtlas API includes an interactive console that allows you to query your database using natural language. The console uses AI to translate your questions into SQL queries and displays the results in a formatted, easy-to-read manner.

### Key Benefits

- **Natural Language Queries**: Ask questions in plain English
- **No SQL Knowledge Required**: The AI generates SQL for you
- **Rich Formatting**: Colorized output with formatted tables
- **Query History**: Track and review your previous queries
- **Interactive Mode**: Continuous query session or single-query execution

---

## Getting Started

### Prerequisites

1. **Anthropic API Key**: Required for AI-powered query translation
2. **Running Database**: PostgreSQL must be running with data
3. **Python Environment**: Application dependencies installed

### Environment Setup

Set your Anthropic API key:

**Linux/macOS:**
```bash
export ANTHROPIC_API_KEY="your-anthropic-api-key-here"
```

**Windows (Command Prompt):**
```cmd
set ANTHROPIC_API_KEY=your-anthropic-api-key-here
```

**Windows (PowerShell):**
```powershell
$env:ANTHROPIC_API_KEY="your-anthropic-api-key-here"
```

**Using .env file:**
```bash
# Add to your .env file
ANTHROPIC_API_KEY=your-anthropic-api-key-here
```

### Getting an Anthropic API Key

1. Sign up at https://console.anthropic.com/
2. Navigate to API Keys section
3. Create a new API key
4. Copy and save the key securely

---

## Starting the Console

### Interactive Mode

Start an interactive session where you can run multiple queries:

```bash
python -m app.console
```

You'll see a welcome screen:

```
╔══════════════════════════════════════════════════════════╗
║          ContentAtlas Query Console                       ║
║                                                          ║
║  Ask questions about your data in natural language      ║
║  Type 'help' for commands, 'exit' to quit              ║
╚══════════════════════════════════════════════════════════╝

Connected to database: datamapper
Available tables: customers, products, orders

Query>
```

### Single Query Mode

Run a single query and exit:

```bash
python -m app.console "Show me all customers"
```

This executes the query, displays results, and exits immediately.

---

## Features

### Natural Language Processing

The console understands various question formats:

**Simple Queries:**
- "Show me all customers"
- "List all products"
- "What tables are available?"

**Filtered Queries:**
- "Show customers from New York"
- "Find products with price greater than 100"
- "List orders from last month"

**Aggregations:**
- "How many customers do we have?"
- "What's the average product price?"
- "Show total sales by month"

**Complex Queries:**
- "What are the top 5 products by sales?"
- "Show customers who haven't ordered in 30 days"
- "Calculate revenue by product category"

### Conversation Memory

The console now remembers your conversation history within a session, allowing for natural follow-up questions and context-aware queries:

**Follow-up Questions:**
```
Query> Show me all customers

[Results displayed...]

Query> Now filter for California only

[Filtered results - the agent remembers the previous query context]

Query> Sort them by name

[Sorted California customers - building on previous context]
```

**References to Past Results:**
```
Query> What's the total revenue for Q1?

Total revenue: $125,450.00

Query> How does that compare to Q2?

[Agent remembers Q1 total and compares with Q2]
```

**Context-Aware Queries:**
```
Query> Show me all products

[Products displayed...]

Query> Which of those have low stock?

[Agent filters the previously shown products for low stock]
```

**How It Works:**
- Each console session generates a unique thread ID
- All queries in that session share conversation history
- The agent can reference previous queries and results
- Message history is automatically trimmed to manage context window (keeps last 5-6 conversation turns)
- Memory is session-based (lost when you exit the console)

### Rich Formatting

The console provides beautiful, readable output:

**Tables:**
```
┏━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
┃ id   ┃ name         ┃ email                ┃
┡━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
│ 1    │ John Doe     │ john@example.com     │
│ 2    │ Jane Smith   │ jane@example.com     │
└──────┴──────────────┴──────────────────────┘
```

**Panels:**
```
╭─────────────────────────────────────────────╮
│ Generated SQL                               │
├─────────────────────────────────────────────┤
│ SELECT * FROM customers LIMIT 10;           │
╰─────────────────────────────────────────────╯
```

**Color Coding:**
- 🟢 Green: Success messages
- 🔵 Blue: Information and SQL queries
- 🟡 Yellow: Warnings
- 🔴 Red: Errors

### Query History

The console tracks all queries in your session:

```
Query> history

Query History:
1. Show me all customers
   SQL: SELECT * FROM customers LIMIT 10;
   
2. How many products do we have?
   SQL: SELECT COUNT(*) FROM products;
   
3. What are the top 5 orders by value?
   SQL: SELECT * FROM orders ORDER BY total DESC LIMIT 5;
```

---

## Commands

### Built-in Commands

| Command | Description | Example |
|---------|-------------|---------|
| `help` | Show available commands and usage | `help` |
| `history` | View query history | `history` |
| `clear` | Clear the screen | `clear` |
| `exit` | Exit the console | `exit` |
| `quit` | Exit the console (alias) | `quit` |

### Query Commands

Simply type your question in natural language:

```
Query> Show me all customers from California

Query> What's the total revenue this month?

Query> List products that are out of stock
```

---

## Usage Examples

### Example 1: Basic Data Exploration

```
Query> What tables are available?

╭─────────────────────────────────────────────╮
│ Available Tables                            │
├─────────────────────────────────────────────┤
│ • customers                                 │
│ • products                                  │
│ • orders                                    │
│ • order_items                               │
╰─────────────────────────────────────────────╯

Query> Show me the first 5 customers

┏━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
┃ id   ┃ name         ┃ email                ┃
┡━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
│ 1    │ John Doe     │ john@example.com     │
│ 2    │ Jane Smith   │ jane@example.com     │
│ 3    │ Bob Johnson  │ bob@example.com      │
│ 4    │ Alice Brown  │ alice@example.com    │
│ 5    │ Charlie Lee  │ charlie@example.com  │
└──────┴──────────────┴──────────────────────┘

5 rows returned
```

### Example 2: Aggregations and Analytics

```
Query> How many customers do we have?

╭─────────────────────────────────────────────╮
│ Result                                      │
├─────────────────────────────────────────────┤
│ Total customers: 1,247                      │
╰─────────────────────────────────────────────╯

Query> What's the average order value?

╭─────────────────────────────────────────────╮
│ Result                                      │
├─────────────────────────────────────────────┤
│ Average order value: $156.32                │
╰─────────────────────────────────────────────╯
```

### Example 3: Complex Queries

```
Query> Show me the top 5 products by revenue

┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃ product_name     ┃ units_sold ┃ total_revenue┃
┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
│ Premium Widget   │ 523        │ $52,300.00   │
│ Deluxe Gadget    │ 412        │ $41,200.00   │
│ Super Tool       │ 387        │ $38,700.00   │
│ Mega Device      │ 301        │ $30,100.00   │
│ Ultra Component  │ 289        │ $28,900.00   │
└──────────────────┴────────────┴──────────────┘

╭─────────────────────────────────────────────╮
│ Generated SQL                               │
├─────────────────────────────────────────────┤
│ SELECT                                      │
│   p.name as product_name,                   │
│   COUNT(*) as units_sold,                   │
│   SUM(oi.price * oi.quantity) as revenue    │
│ FROM products p                             │
│ JOIN order_items oi ON p.id = oi.product_id│
│ GROUP BY p.name                             │
│ ORDER BY revenue DESC                       │
│ LIMIT 5;                                    │
╰─────────────────────────────────────────────╯
```

### Example 4: Filtered Queries

```
Query> Show customers who ordered in the last 7 days

┏━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ id   ┃ name         ┃ email                ┃ last_order ┃
┡━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ 42   │ Sarah Connor │ sarah@example.com    │ 2024-01-20 │
│ 87   │ Kyle Reese   │ kyle@example.com     │ 2024-01-19 │
│ 156  │ John Connor  │ john@example.com     │ 2024-01-18 │
└──────┴──────────────┴──────────────────────┴────────────┘

3 rows returned
```

---

## Configuration

### Database Connection

The console uses the same database configuration as the main application:

```bash
# In .env file
DATABASE_URL=postgresql://user:password@localhost:5432/datamapper
```

### AI Model Configuration

The console uses Claude (Anthropic) for query translation. You can configure:

**Model Selection:**
```python
# In app/console.py (advanced users)
MODEL = "claude-3-5-sonnet-20241022"  # Default
# or
MODEL = "claude-3-opus-20240229"      # More powerful
```

**Temperature:**
```python
# Lower = more deterministic, Higher = more creative
TEMPERATURE = 0.0  # Default for SQL generation
```

### Display Options

**Table Width:**
```python
# Maximum width for table display
MAX_TABLE_WIDTH = 120  # Default
```

**Row Limit:**
```python
# Default number of rows to display
DEFAULT_LIMIT = 100  # Default
```

---

## Troubleshooting

### API Key Issues

**Error:** `ANTHROPIC_API_KEY environment variable not set`

**Solution:**
```bash
export ANTHROPIC_API_KEY="your-key-here"
# or add to .env file
```

### Database Connection Issues

**Error:** `Could not connect to database`

**Solutions:**
- Verify PostgreSQL is running
- Check DATABASE_URL is correct
- Ensure database exists and is accessible

### Query Translation Issues

**Error:** `Could not generate SQL for query`

**Solutions:**
- Rephrase your question more clearly
- Be more specific about table and column names
- Check that referenced tables exist
- Try breaking complex queries into simpler parts

### Display Issues

**Error:** Terminal output looks garbled

**Solutions:**
- Ensure terminal supports UTF-8 encoding
- Try a different terminal emulator
- Update terminal font to support box-drawing characters

### Performance Issues

**Slow query responses:**

**Solutions:**
- Add indexes to frequently queried columns
- Limit result sets with "LIMIT" in your question
- Break complex queries into smaller parts
- Check database performance

---

## Best Practices

### Writing Effective Queries

**Be Specific:**
```
❌ "Show me data"
✅ "Show me all customers from the customers table"
```

**Use Clear Language:**
```
❌ "gimme stuff from db"
✅ "List all products with price greater than 100"
```

**Reference Table Names:**
```
❌ "How many records?"
✅ "How many records in the orders table?"
```

**Specify Limits:**
```
❌ "Show all orders"
✅ "Show the first 10 orders"
```

### Security Considerations

- **Read-Only**: The console only executes SELECT queries
- **No Modifications**: Cannot INSERT, UPDATE, or DELETE data
- **API Key Security**: Keep your Anthropic API key secure
- **Database Credentials**: Protect your database connection string

---

## Advanced Usage

### Batch Queries

Create a file with queries (one per line):

```bash
# queries.txt
Show me all customers
How many products do we have?
What's the total revenue?
```

Run them:

```bash
while read query; do
  python -m app.console "$query"
done < queries.txt
```

### Scripting

Use the console in scripts:

```bash
#!/bin/bash
RESULT=$(python -m app.console "SELECT COUNT(*) FROM customers")
echo "Customer count: $RESULT"
```

### Integration with Other Tools

Pipe output to other commands:

```bash
python -m app.console "Show all customers" | grep "example.com"
```

---

## Related Documentation

- [API Reference](API_REFERENCE.md) - REST API endpoints
- [Setup Guide](SETUP.md) - Environment configuration
- [Architecture](ARCHITECTURE.md) - System design and components
