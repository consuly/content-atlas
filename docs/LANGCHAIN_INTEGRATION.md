# LangChain v1.0 Reference Guide

**Version:** 1.0.x  
**Last Updated:** January 2025  
**Project:** Content Atlas Database Query Agent

---

## Table of Contents

1. [Overview](#overview)
2. [Core Improvement #1: `create_agent`](#core-improvement-1-create_agent)
3. [Core Improvement #2: Middleware System](#core-improvement-2-middleware-system)
4. [Core Improvement #3: Standard Content Blocks](#core-improvement-3-standard-content-blocks)
5. [Simplified Namespace](#simplified-namespace)
6. [Claude Model Optimization](#claude-model-optimization)
7. [Quick Reference](#quick-reference)

---

## Overview

LangChain v1.0 represents a focused, production-ready foundation for building agents with three core improvements:

1. **`create_agent`** - The new standard for building agents, replacing `langgraph.prebuilt.create_react_agent`
2. **Standard content blocks** - Unified access to modern LLM features (reasoning traces, citations, etc.) across providers
3. **Simplified namespace** - Streamlined `langchain` package focused on essential building blocks

### Key Benefits for This Project

- **Simpler agent creation** with `create_agent` instead of complex LangGraph configurations
- **Middleware system** for SQL query validation, human approval, and conversation management
- **Provider-agnostic content** access for Claude's reasoning traces and extended thinking
- **Built-in features** like persistence, streaming, and human-in-the-loop without extra setup

---

## Core Improvement #1: `create_agent`

### Basic Agent Creation

The `create_agent` function is the new standard for building agents in LangChain v1.0. It provides a simpler interface while offering greater customization through middleware.

**Current Implementation Example:**

```python
from langchain.agents import create_agent, AgentState
from langchain_anthropic import ChatAnthropic
from typing_extensions import TypedDict, NotRequired

# Custom state using TypedDict (v1.0 requirement)
class DatabaseQueryState(AgentState):
    """Custom state for database query agent."""
    query_intent: NotRequired[str]
    approved_queries: NotRequired[List[str]]

# Initialize model
llm = ChatAnthropic(
    model="claude-haiku-4-5-20251001",
    api_key=settings.anthropic_api_key,
    temperature=0,
    max_tokens=4096
)

# Create agent
agent = create_agent(
    model=llm,
    tools=[get_database_schema_tool, execute_sql_query],
    system_prompt="You are an expert SQL analyst...",
    state_schema=DatabaseQueryState
)

# Invoke agent
result = agent.invoke({"messages": [HumanMessage(content="Show me all users")]})
```

### System Prompts

System prompts define the agent's behavior and capabilities. In v1.0, use the `system_prompt` parameter:

```python
system_prompt = """You are an expert SQL analyst helping users query their PostgreSQL database.

First, use the get_database_schema_tool to understand the available tables and their structure.

When generating SQL queries:
- Use proper PostgreSQL syntax
- Include appropriate JOINs when multiple tables are needed
- Use aggregate functions (SUM, AVG, COUNT, etc.) when requested
- Add WHERE clauses for filtering
- Use ORDER BY and LIMIT for sorting and pagination
- Always use double quotes for table and column names
- Generate efficient queries following database best practices

Always execute the final query using execute_sql_query and return results."""

agent = create_agent(
    model=llm,
    tools=tools,
    system_prompt=system_prompt
)
```

### Dynamic Prompts

For prompts that adapt based on runtime context, use the `@dynamic_prompt` decorator:

```python
from dataclasses import dataclass
from langchain.agents.middleware import dynamic_prompt, ModelRequest

@dataclass
class Context:
    user_role: str = "user"
    max_query_complexity: int = 5

@dynamic_prompt
def adaptive_system_prompt(request: ModelRequest) -> str:
    user_role = request.runtime.context.user_role
    base_prompt = "You are an expert SQL analyst."
    
    if user_role == "admin":
        return f"{base_prompt} You have full access to all tables and can perform complex queries."
    elif user_role == "analyst":
        return f"{base_prompt} Focus on read-only SELECT queries for reporting."
    else:
        return f"{base_prompt} Provide simple queries with clear explanations."

agent = create_agent(
    model=llm,
    tools=tools,
    middleware=[adaptive_system_prompt],
    context_schema=Context
)

# Use with context
result = agent.invoke(
    {"messages": [{"role": "user", "content": "Show sales data"}]},
    context=Context(user_role="admin")
)
```

### State Management

Custom state extends the default `AgentState` with additional fields. Always use `TypedDict` (not Pydantic models or dataclasses):

```python
from langchain.agents import AgentState
from typing_extensions import NotRequired

class DatabaseQueryState(AgentState):
    """Custom state for database query agent."""
    query_intent: NotRequired[str]  # What the user wants
    approved_queries: NotRequired[List[str]]  # Track approved SQL
    query_count: NotRequired[int]  # Number of queries executed
    total_rows_returned: NotRequired[int]  # Total rows across queries

agent = create_agent(
    model=llm,
    tools=tools,
    system_prompt=system_prompt,
    state_schema=DatabaseQueryState
)
```

**Accessing State in Tools:**

```python
from langchain.tools import tool, ToolRuntime

@tool
def execute_with_tracking(
    sql_query: str,
    runtime: ToolRuntime[DatabaseQueryState]
) -> str:
    """Execute SQL and track query count."""
    # Access state
    current_count = runtime.state.get("query_count", 0)
    
    # Execute query
    result = execute_query(sql_query)
    
    # Update state (return dict to update)
    runtime.state["query_count"] = current_count + 1
    
    return result
```

### Built-in Features

Because `create_agent` is built on LangGraph, you automatically get:

#### 1. Persistence

Conversations automatically persist across sessions with built-in checkpointing:

```python
from langgraph.checkpoint.memory import MemorySaver

# Add checkpointer for persistence
checkpointer = MemorySaver()

agent = create_agent(
    model=llm,
    tools=tools,
    system_prompt=system_prompt,
    checkpointer=checkpointer
)

# Use with thread_id for conversation continuity
config = {"configurable": {"thread_id": "user-123-session-abc"}}
result = agent.invoke({"messages": [...]}, config=config)
```

#### 2. Streaming

Stream tokens, tool calls, and reasoning traces in real-time:

```python
# Stream events
for event in agent.stream({"messages": [HumanMessage(content="Query users")]}):
    if "model" in event:
        # Model is generating response
        print(event["model"]["messages"][-1].content)
    elif "tools" in event:
        # Tool is being executed
        print(f"Executing: {event['tools']}")
```

#### 3. Human-in-the-Loop

Pause agent execution for human approval (see Middleware section for details).

#### 4. Time Travel

Rewind conversations to any point and explore alternate paths:

```python
# Get all checkpoints
checkpoints = list(agent.get_state_history(config))

# Rewind to specific checkpoint
agent.update_state(checkpoints[2].config, {"messages": [...]})
```

### Structured Output

Generate structured responses using two strategies:

#### ToolStrategy (Artificial Tool Calling)

Uses tool calling to generate structured output. Works with all models:

```python
from langchain.agents.structured_output import ToolStrategy
from pydantic import BaseModel

class QueryResult(BaseModel):
    explanation: str
    sql_query: str
    execution_time_seconds: float
    rows_returned: int

agent = create_agent(
    model=llm,
    tools=tools,
    response_format=ToolStrategy(QueryResult)
)

result = agent.invoke({"messages": [...]})
structured_output = result["structured_response"]  # QueryResult instance
```

#### ProviderStrategy (Native Structured Output)

Uses provider-native structured output generation (more efficient but provider-specific):

```python
from langchain.agents.structured_output import ProviderStrategy

agent = create_agent(
    model=llm,
    tools=tools,
    response_format=ProviderStrategy(QueryResult)
)
```

**When to Use Each:**

- **ToolStrategy**: Default choice, works with all models, more reliable
- **ProviderStrategy**: Use when provider supports native structured output (OpenAI, Anthropic) for better performance

---

## Core Improvement #2: Middleware System

Middleware is the defining feature of `create_agent`. It provides hooks at each step in an agent's execution, enabling:

- Dynamic prompts and context engineering
- Conversation summarization
- Selective tool access
- State management
- Input/output guardrails
- Human-in-the-loop approval

### Middleware Hooks

| Hook              | When it runs             | Use cases                               |
| ----------------- | ------------------------ | --------------------------------------- |
| `before_agent`    | Before calling the agent | Load memory, validate input             |
| `before_model`    | Before each LLM call     | Update prompts, trim messages           |
| `wrap_model_call` | Around each LLM call     | Intercept and modify requests/responses |
| `wrap_tool_call`  | Around each tool call    | Intercept and modify tool execution     |
| `after_model`     | After each LLM response  | Validate output, apply guardrails       |
| `after_agent`     | After agent completes    | Save results, cleanup                   |

### Built-in Middleware

#### 1. SummarizationMiddleware

Condenses conversation history when it gets too long:

```python
from langchain.agents.middleware import SummarizationMiddleware

agent = create_agent(
    model=llm,
    tools=tools,
    middleware=[
        SummarizationMiddleware(
            model="anthropic:claude-sonnet-4-5",
            max_tokens_before_summary=1000  # Summarize after 1000 tokens
        )
    ]
)
```

**Use Case:** Long database query sessions where conversation history grows large.

#### 2. PIIMiddleware

Redacts sensitive information before sending to the model:

```python
from langchain.agents.middleware import PIIMiddleware

agent = create_agent(
    model=llm,
    tools=tools,
    middleware=[
        PIIMiddleware(patterns=["email", "phone", "ssn", "credit_card"])
    ]
)
```

**Use Case:** Protecting sensitive data in database queries (user emails, phone numbers, etc.).

#### 3. HumanInTheLoopMiddleware

Requires approval for sensitive tool calls:

```python
from langchain.agents.middleware import HumanInTheLoopMiddleware

agent = create_agent(
    model=llm,
    tools=[get_schema, execute_sql_query, delete_records],
    middleware=[
        HumanInTheLoopMiddleware(
            interrupt_on={
                "execute_sql_query": {
                    "allowed_decisions": ["approve", "edit", "reject"]
                },
                "delete_records": True  # Always require approval
            }
        )
    ]
)
```

**Use Case:** Requiring human approval before executing SQL queries, especially for destructive operations.

### Custom Middleware

Create custom middleware by subclassing `AgentMiddleware` and implementing hooks:

#### Example 1: SQL Query Validation Middleware

```python
from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse
from typing import Callable
import re

class SQLValidationMiddleware(AgentMiddleware):
    """Validates SQL queries before execution."""
    
    def wrap_tool_call(
        self,
        tool_name: str,
        tool_args: dict,
        handler: Callable
    ) -> Any:
        if tool_name == "execute_sql_query":
            sql_query = tool_args.get("sql_query", "")
            
            # Validate query
            if not sql_query.strip().upper().startswith("SELECT"):
                raise ValueError("Only SELECT queries are allowed")
            
            # Check for dangerous patterns
            dangerous = ["DROP", "DELETE", "TRUNCATE", "ALTER"]
            if any(keyword in sql_query.upper() for keyword in dangerous):
                raise ValueError(f"Query contains forbidden operations")
            
            # Check query complexity (e.g., max 3 JOINs)
            join_count = sql_query.upper().count("JOIN")
            if join_count > 3:
                raise ValueError(f"Query too complex: {join_count} JOINs (max 3)")
        
        # Execute tool
        return handler(tool_name, tool_args)

agent = create_agent(
    model=llm,
    tools=tools,
    middleware=[SQLValidationMiddleware()]
)
```

#### Example 2: Query Cost Estimation Middleware

```python
from dataclasses import dataclass

class QueryCostState(AgentState):
    estimated_cost: NotRequired[float]
    query_complexity: NotRequired[str]

class QueryCostMiddleware(AgentMiddleware[QueryCostState]):
    """Estimates query cost before execution."""
    
    state_schema = QueryCostState
    
    def wrap_tool_call(
        self,
        tool_name: str,
        tool_args: dict,
        handler: Callable
    ) -> Any:
        if tool_name == "execute_sql_query":
            sql_query = tool_args.get("sql_query", "")
            
            # Estimate complexity
            join_count = sql_query.upper().count("JOIN")
            has_aggregates = any(
                agg in sql_query.upper() 
                for agg in ["SUM", "AVG", "COUNT", "GROUP BY"]
            )
            
            if join_count > 2 or has_aggregates:
                complexity = "high"
                estimated_cost = 0.05  # $0.05 per query
            elif join_count > 0:
                complexity = "medium"
                estimated_cost = 0.02
            else:
                complexity = "low"
                estimated_cost = 0.01
            
            # Update state
            self.runtime.state["estimated_cost"] = estimated_cost
            self.runtime.state["query_complexity"] = complexity
            
            print(f"Query complexity: {complexity} (est. cost: ${estimated_cost})")
        
        return handler(tool_name, tool_args)

agent = create_agent(
    model=llm,
    tools=tools,
    middleware=[QueryCostMiddleware()]
)
```

#### Example 3: Rate Limiting Middleware

```python
import time
from collections import defaultdict

class RateLimitMiddleware(AgentMiddleware):
    """Limits queries per user per time window."""
    
    def __init__(self, max_queries: int = 10, window_seconds: int = 60):
        self.max_queries = max_queries
        self.window_seconds = window_seconds
        self.query_times = defaultdict(list)
    
    def before_agent(self, state: AgentState, runtime) -> dict | None:
        user_id = runtime.context.get("user_id", "anonymous")
        current_time = time.time()
        
        # Clean old queries outside window
        self.query_times[user_id] = [
            t for t in self.query_times[user_id]
            if current_time - t < self.window_seconds
        ]
        
        # Check rate limit
        if len(self.query_times[user_id]) >= self.max_queries:
            raise ValueError(
                f"Rate limit exceeded: {self.max_queries} queries per "
                f"{self.window_seconds} seconds"
            )
        
        # Record this query
        self.query_times[user_id].append(current_time)
        return None

agent = create_agent(
    model=llm,
    tools=tools,
    middleware=[RateLimitMiddleware(max_queries=10, window_seconds=60)]
)
```

### Combining Multiple Middleware

Middleware executes in the order specified:

```python
agent = create_agent(
    model=llm,
    tools=tools,
    middleware=[
        RateLimitMiddleware(max_queries=10),
        PIIMiddleware(patterns=["email", "phone"]),
        SQLValidationMiddleware(),
        QueryCostMiddleware(),
        SummarizationMiddleware(model=llm, max_tokens_before_summary=1000),
        HumanInTheLoopMiddleware(interrupt_on={"execute_sql_query": True})
    ]
)
```

**Execution Order:**
1. Rate limit check (before_agent)
2. PII redaction (before_model)
3. SQL validation (wrap_tool_call)
4. Cost estimation (wrap_tool_call)
5. Summarization (before_model)
6. Human approval (after_model)

---

## Core Improvement #3: Standard Content Blocks

The new `content_blocks` property provides unified access to modern LLM features across providers.

### What Are Content Blocks?

Content blocks are a standardized way to access different types of content in LLM responses:

- **Text blocks**: Regular text responses
- **Reasoning blocks**: Extended thinking/reasoning traces (Claude)
- **Tool call blocks**: Tool invocations
- **Image blocks**: Images in multimodal responses
- **Citations**: Source references

### Accessing Content Blocks

```python
from langchain_anthropic import ChatAnthropic

model = ChatAnthropic(model="claude-sonnet-4-5")
response = model.invoke("Explain how database indexing works")

# Access standardized content blocks
for block in response.content_blocks:
    if block["type"] == "reasoning":
        print(f"Model reasoning: {block['reasoning']}")
    elif block["type"] == "text":
        print(f"Response: {block['text']}")
    elif block["type"] == "tool_call":
        print(f"Tool: {block['name']}({block['args']})")
```

### Content Block Types

#### Text Block

```python
text_block = {
    "type": "text",
    "text": "Here's the SQL query you requested..."
}
```

#### Reasoning Block (Claude Extended Thinking)

```python
reasoning_block = {
    "type": "reasoning",
    "reasoning": "First, I need to understand the schema..."
}
```

#### Tool Call Block

```python
tool_call_block = {
    "type": "tool_call",
    "id": "call_123",
    "name": "execute_sql_query",
    "args": {"sql_query": "SELECT * FROM users"}
}
```

#### Image Block

```python
image_block = {
    "type": "image",
    "url": "https://example.com/chart.png",
    "mime_type": "image/png"
}
```

### Using Content Blocks in This Project

**Example: Extracting SQL Query Reasoning**

```python
def query_database_with_reasoning(user_prompt: str) -> Dict[str, Any]:
    """Query database and capture Claude's reasoning."""
    agent = create_query_agent(system_prompt)
    result = agent.invoke({"messages": [HumanMessage(content=user_prompt)]})
    
    reasoning_traces = []
    sql_queries = []
    final_response = ""
    
    # Parse content blocks
    for message in result["messages"]:
        if hasattr(message, 'content_blocks'):
            for block in message.content_blocks:
                if block["type"] == "reasoning":
                    reasoning_traces.append(block["reasoning"])
                elif block["type"] == "text":
                    final_response += block["text"]
                elif block["type"] == "tool_call" and block["name"] == "execute_sql_query":
                    sql_queries.append(block["args"]["sql_query"])
    
    return {
        "response": final_response,
        "reasoning": reasoning_traces,
        "sql_queries": sql_queries
    }
```

### Serializing Content Blocks

By default, content blocks are NOT serialized into the `content` attribute. To serialize them (e.g., for API responses):

```python
# Option 1: Environment variable
export LC_OUTPUT_VERSION=v1

# Option 2: Initialization parameter
model = ChatAnthropic(
    model="claude-sonnet-4-5",
    output_version="v1"
)
```

### Multimodal Content

Create messages with multiple content types:

```python
from langchain.messages import HumanMessage

message = HumanMessage(content_blocks=[
    {"type": "text", "text": "Analyze this database schema diagram:"},
    {"type": "image", "url": "https://example.com/schema.png"},
])

response = model.invoke([message])
```

---

## Simplified Namespace

LangChain v1.0 streamlines the `langchain` package to focus on essential building blocks for agents.

### What's in `langchain`

| Module                     | What's Available                                    | Notes                             |
| -------------------------- | --------------------------------------------------- | --------------------------------- |
| `langchain.agents`         | `create_agent`, `AgentState`                        | Core agent creation               |
| `langchain.messages`       | Message types, content blocks, `trim_messages`      | Re-exported from `langchain-core` |
| `langchain.tools`          | `@tool`, `BaseTool`, injection helpers              | Re-exported from `langchain-core` |
| `langchain.chat_models`    | `init_chat_model`, `BaseChatModel`                  | Unified model initialization      |
| `langchain.embeddings`     | `init_embeddings`, `Embeddings`                     | Embedding models                  |

### Current Project Imports

```python
# ✅ Correct v1.0 imports (already in use)
from langchain.agents import create_agent, AgentState
from langchain.agents.middleware import SummarizationMiddleware, HumanInTheLoopMiddleware
from langchain.agents.structured_output import ToolStrategy
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, AIMessage
from langchain_anthropic import ChatAnthropic
```

### What Moved to `langchain-classic`

The following functionality moved to `langchain-classic`:

- Legacy chains (`LLMChain`, `ConversationChain`, etc.)
- Retrievers (`MultiQueryRetriever`, etc.)
- Indexing API
- Hub module (prompt management)
- `langchain-community` re-exports

**If you need any of these:**

```bash
pip install langchain-classic
```

```python
# Import from langchain-classic
from langchain_classic.chains import LLMChain
from langchain_classic.retrievers import MultiQueryRetriever
from langchain_classic import hub
```

---

## Claude Model Optimization

### Model Selection Guide

| Model                  | Best For                          | Cost (per 1M tokens) | Speed    | Context Window |
| ---------------------- | --------------------------------- | -------------------- | -------- | -------------- |
| Claude 3.5 Sonnet      | Complex SQL, multi-table queries  | $3 input / $15 output| Medium   | 200K           |
| Claude 3.5 Haiku       | Simple queries, fast responses    | $0.80 / $4           | Fast     | 200K           |
| Claude 3 Opus          | Critical queries, max accuracy    | $15 / $75            | Slow     | 200K           |

**Current Project Usage:**

```python
# Using Haiku for cost-effective queries
llm = ChatAnthropic(
    model="claude-haiku-4-5-20251001",
    api_key=settings.anthropic_api_key,
    temperature=0,  # Deterministic for SQL
    max_tokens=4096
)
```

### Token Usage Optimization

#### 1. Efficient System Prompts

Keep system prompts concise but informative:

```python
# ❌ Verbose (wastes tokens)
system_prompt = """
You are an expert SQL analyst with years of experience in database management.
You have deep knowledge of PostgreSQL, MySQL, and other database systems.
You understand complex queries, joins, aggregations, and performance optimization.
When a user asks you a question, you should...
"""

# ✅ Concise (saves tokens)
system_prompt = """Expert SQL analyst for PostgreSQL queries.

Process:
1. Use get_database_schema_tool for table structure
2. Generate efficient SQL with proper JOINs
3. Execute with execute_sql_query
4. Return results with explanation"""
```

#### 2. Schema Caching

Cache database schema to avoid repeated tool calls:

```python
from functools import lru_cache

@lru_cache(maxsize=1)
def get_cached_schema() -> str:
    """Cache schema for 1 hour to reduce token usage."""
    return get_database_schema()

@tool
def get_database_schema_tool() -> str:
    """Get database schema (cached)."""
    return get_cached_schema()
```

#### 3. Prompt Caching (Claude-specific)

Use Claude's prompt caching to reduce costs for repeated context:

```python
from langchain_anthropic import ChatAnthropic

llm = ChatAnthropic(
    model="claude-sonnet-4-5",
    # Enable prompt caching for system messages
    cache_control={"type": "ephemeral"}
)

# System prompt will be cached for 5 minutes
agent = create_agent(
    model=llm,
    tools=tools,
    system_prompt=long_system_prompt  # This gets cached
)
```

**Savings:** Up to 90% cost reduction on cached tokens.

#### 4. Message Trimming

Trim old messages to keep context window manageable:

```python
from langchain.messages import trim_messages

def query_with_trimming(user_prompt: str, conversation_history: list):
    """Query with automatic message trimming."""
    # Keep only last 10 messages or 2000 tokens
    trimmed_messages = trim_messages(
        conversation_history,
        max_tokens=2000,
        strategy="last",
        token_counter=llm
    )
    
    agent = create_agent(model=llm, tools=tools)
    result = agent.invoke({"messages": trimmed_messages + [HumanMessage(content=user_prompt)]})
    return result
```

#### 5. Summarization for Long Conversations

Use `SummarizationMiddleware` to condense history:

```python
agent = create_agent(
    model=llm,
    tools=tools,
    middleware=[
        SummarizationMiddleware(
            model="anthropic:claude-haiku-4-5",  # Use cheaper model for summaries
            max_tokens_before_summary=1000
        )
    ]
)
```

### Cost-Effective Prompting Techniques

#### 1. Structured Output for Parsing

Use structured output instead of parsing text responses:

```python
# ❌ Expensive: Parse text response
response = agent.invoke({"messages": [...]})
# Parse "The SQL query is: SELECT..." from text

# ✅ Efficient: Use structured output
from pydantic import BaseModel

class QueryResponse(BaseModel):
    sql_query: str
    explanation: str

agent = create_agent(
    model=llm,
    tools=tools,
    response_format=ToolStrategy(QueryResponse)
)

result = agent.invoke({"messages": [...]})
sql = result["structured_response"].sql_query  # Direct access
```

#### 2. Batch Processing

Process multiple queries in one request:

```python
def batch_query(queries: List[str]) -> List[Dict]:
    """Process multiple queries in one agent call."""
    batch_prompt = "Execute these queries:\n" + "\n".join(
        f"{i+1}. {q}" for i, q in enumerate(queries)
    )
    
    result = agent.invoke({"messages": [HumanMessage(content=batch_prompt)]})
    return parse_batch_results(result)
```

#### 3. Dynamic Model Selection

Use cheaper models for simple queries:

```python
from langchain.agents.middleware import AgentMiddleware, ModelRequest

class DynamicModelMiddleware(AgentMiddleware):
    """Select model based on query complexity."""
    
    def wrap_model_call(self, request: ModelRequest, handler):
        query = request.state.messages[-1].content
        
        # Simple query indicators
        simple_keywords = ["show", "list", "count", "get"]
        is_simple = any(kw in query.lower() for kw in simple_keywords)
        
        if is_simple:
            # Use Haiku for simple queries ($0.80/1M tokens)
            model = ChatAnthropic(model="claude-haiku-4-5")
        else:
            # Use Sonnet for complex queries ($3/1M tokens)
            model = ChatAnthropic(model="claude-sonnet-4-5")
        
        return handler(request.replace(model=model))

agent = create_agent(
    model=llm,
    tools=tools,
    middleware=[DynamicModelMiddleware()]
)
```

### Temperature and Max Tokens Tuning

#### For SQL Generation

```python
# ✅ Optimal settings for SQL
llm = ChatAnthropic(
    model="claude-haiku-4-5",
    temperature=0,  # Deterministic, no creativity needed
    max_tokens=2048  # Enough for SQL + explanation
)
```

#### For Natural Language Responses

```python
# For conversational responses
llm = ChatAnthropic(
    model="claude-sonnet-4-5",
    temperature=0.3,  # Slight variation for natural language
    max_tokens=4096  # More tokens for detailed explanations
)
```

### Monitoring Costs

Track token usage and costs:

```python
import time
from collections import defaultdict

class CostTrackingMiddleware(AgentMiddleware):
    """Track token usage and estimated costs."""
    
    def __init__(self):
        self.usage_stats = defaultdict(lambda: {"input_tokens": 0, "output_tokens": 0})
    
    def after_model(self, state: AgentState, runtime) -> dict | None:
        # Extract usage from last message
        last_message = state.messages[-1]
        if hasattr(last_message, 'usage_metadata'):
            usage = last_message.usage_metadata
            model_name = runtime.context.get("model", "unknown")
            
            self.usage_stats[model_name]["input_tokens"] += usage.get("input_tokens", 0)
            self.usage_stats[model_name]["output_tokens"] += usage.get("output_tokens", 0)
        
        return None
    
    def get_estimated_cost(self) -> float:
        """Calculate estimated cost based on usage."""
        costs = {
            "claude-haiku-4-5": {"input": 0.80, "output": 4.00},
            "claude-sonnet-4-5": {"input": 3.00, "output": 15.00},
        }
        
        total_cost = 0
        for model, usage in self.usage_stats.items():
            if model in costs:
                input_cost = (usage["input_tokens"] / 1_000_000) * costs[model]["input"]
                output_cost = (usage["output_tokens"] / 1_000_000) * costs[model]["output"]
                total_cost += input_cost + output_cost
        
        return total_cost

# Use in agent
cost_tracker = CostTrackingMiddleware()
agent = create_agent(
    model=llm,
    tools=tools,
    middleware=[cost_tracker]
)

# After queries
print(f"Estimated cost: ${cost_tracker.get_estimated_cost():.4f}")
```

---

## Quick Reference

### Agent Creation Template

```python
from langchain.agents import create_agent, AgentState
from langchain_anthropic import ChatAnthropic
from typing_extensions import NotRequired

# 1. Define custom state (optional)
class CustomState(AgentState):
    custom_field: NotRequired[str]

# 2. Initialize model
llm = ChatAnthropic(
    model="claude-haiku-4-5-20251001",
    temperature=0,
    max_tokens=4096
)

# 3. Create agent
agent = create_agent(
    model=llm,
    tools=[tool1, tool2],
    system_prompt="Your system prompt here",
    state_schema=CustomState,  # Optional
    middleware=[],  # Optional
)

# 4. Invoke agent
result = agent.invoke({"messages": [{"role": "user", "content": "Query"}]})
```

### Tool Definition Template

```python
from langchain.tools import tool, ToolRuntime

@tool
def my_tool(
    param1: str,
    param2: int,
    runtime: ToolRuntime[CustomState]  # Optional: for state access
) -> str:
    """Tool description for the LLM.
    
    Args:
        param1: Description of param1
        param2: Description of param2
    """
    # Access state (if using ToolRuntime)
    state_value = runtime.state.get("custom_field")
    
    # Tool logic here
    result = do_something(param1, param2)
    
    return result
```

### Middleware Template

```python
from langchain.agents.middleware import AgentMiddleware, ModelRequest
from typing import Callable, Any

class CustomMiddleware(AgentMiddleware):
    """Description of what this middleware does."""
    
    def before_agent(self, state: AgentState, runtime) -> dict | None:
        """Runs before agent starts."""
        # Validation, setup, etc.
        return None  # Or return dict to update state
    
    def before_model(self, request: ModelRequest) -> dict | None:
        """Runs before each model call."""
        # Modify prompts, trim messages, etc.
        return None
    
    def wrap_model_call(self, request: ModelRequest, handler: Callable) -> Any:
        """Wraps model call."""
        # Modify request
        modified_request = request.replace(...)
        
        # Call model
        response = handler(modified_request)
        
        # Modify response if needed
        return response
    
    def wrap_tool_call(self, tool_name: str, tool_args: dict, handler: Callable) -> Any:
        """Wraps tool execution."""
        # Validate, log, etc.
        if tool_name == "specific_tool":
            # Custom logic
            pass
        
        # Execute tool
        return handler(tool_name, tool_args)
    
    def after_model(self, state: AgentState, runtime) -> dict | None:
        """Runs after each model response."""
        # Validation, guardrails, etc.
        return None
    
    def after_agent(self, state: AgentState, runtime) -> dict | None:
        """Runs after agent completes."""
        # Cleanup, logging, etc.
        return None
```

### Common Patterns Cheat Sheet

#### Pattern 1: Simple Query Agent

```python
agent = create_agent(
    model="anthropic:claude-haiku-4-5",
    tools=[get_schema, execute_query],
    system_prompt="You are a SQL expert."
)

result = agent.invoke({"messages": [{"role": "user", "content": "Show users"}]})
```

#### Pattern 2: Agent with Human Approval

```python
from langchain.agents.middleware import HumanInTheLoopMiddleware

agent = create_agent(
    model=llm,
    tools=tools,
    middleware=[
        HumanInTheLoopMiddleware(
            interrupt_on={"execute_sql_query": True}
        )
    ]
)
```

#### Pattern 3: Agent with Conversation Memory

```python
from langgraph.checkpoint.memory import MemorySaver

checkpointer = MemorySaver()
agent = create_agent(model=llm, tools=tools, checkpointer=checkpointer)

config = {"configurable": {"thread_id": "user-123"}}
result = agent.invoke({"messages": [...]}, config=config)
```

#### Pattern 4: Streaming Agent

```python
for event in agent.stream({"messages": [{"role": "user", "content": "Query"}]}):
    if "model" in event:
        print(event["model"]["messages"][-1].content)
```

#### Pattern 5: Structured Output

```python
from langchain.agents.structured_output import ToolStrategy
from pydantic import BaseModel

class Output(BaseModel):
    field1: str
    field2: int

agent = create_agent(
    model=llm,
    tools=tools,
    response_format=ToolStrategy(Output)
)

result = agent.invoke({"messages": [...]})
output = result["structured_response"]  # Output instance
```

### Troubleshooting Guide

#### Issue: "Only SELECT queries allowed" error

**Cause:** SQL validation rejecting non-SELECT queries

**Solution:** Check your SQL query validation logic in tools or middleware

```python
# In execute_sql_query tool
if not sql_query.strip().upper().startswith('SELECT'):
    return "ERROR: Only SELECT queries are allowed"
```

#### Issue: Agent not using tools

**Cause:** System prompt doesn't instruct model to use tools

**Solution:** Update system prompt to explicitly mention tools

```python
system_prompt = """You are a SQL expert.

Use these tools:
1. get_database_schema_tool - Get table structure
2. execute_sql_query - Run SELECT queries

Always use get_database_schema_tool first, then execute_sql_query."""
```

#### Issue: High token usage / costs

**Cause:** Long conversation history, verbose prompts, or inefficient model selection

**Solutions:**
1. Use `SummarizationMiddleware` to condense history
2. Trim messages with `trim_messages`
3. Use dynamic model selection (Haiku for simple queries)
4. Enable prompt caching for Claude
5. Keep system prompts concise

#### Issue: Content blocks not accessible

**Cause:** Using old `content` attribute instead of `content_blocks`

**Solution:** Use `content_blocks` property

```python
# ✅ Correct
for block in response.content_blocks:
    if block["type"] == "text":
        print(block["text"])

# ❌ Old way
print(response.content)
```

#### Issue: State not persisting between calls

**Cause:** Missing checkpointer or thread_id

**Solution:** Add checkpointer and use consistent thread_id

```python
from langgraph.checkpoint.memory import MemorySaver

checkpointer = MemorySaver()
agent = create_agent(model=llm, tools=tools, checkpointer=checkpointer)

# Use same thread_id for conversation continuity
config = {"configurable": {"thread_id": "user-123"}}
result = agent.invoke({"messages": [...]}, config=config)
```

#### Issue: Middleware not executing

**Cause:** Middleware not added to agent or wrong hook implemented

**Solution:** Verify middleware is in the list and implements correct hooks

```python
agent = create_agent(
    model=llm,
    tools=tools,
    middleware=[MyMiddleware()]  # Ensure it's in the list
)

# Verify hook names match exactly
class MyMiddleware(AgentMiddleware):
    def before_model(self, request: ModelRequest) -> dict | None:  # Correct hook name
        pass
```

---

## Additional Resources

- **LangChain v1.0 Release Notes**: https://docs.langchain.com/oss/python/releases/langchain-v1
- **Migration Guide**: https://docs.langchain.com/oss/python/migrate/langchain-v1
- **Agents Documentation**: https://docs.langchain.com/oss/python/langchain/agents
- **Middleware Guide**: https://docs.langchain.com/oss/python/langchain/middleware
- **Content Blocks Reference**: https://docs.langchain.com/oss/python/langchain/messages#standard-content-blocks
- **Claude API Documentation**: https://docs.anthropic.com/claude/reference
- **LangGraph Documentation**: https://docs.langchain.com/oss/python/langgraph

---

**Document Version:** 1.0  
**Last Updated:** October 26th 2025
**Maintained by:** Content Atlas Team
