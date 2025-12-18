"""
Test message trimming logic to ensure tool_use/tool_result pairs are never split.

This test verifies the fix for the Anthropic API error:
"Each `tool_result` block must have a corresponding `tool_use` block in the previous message."
"""
import pytest
from typing import Any, Dict, List
from unittest.mock import Mock, MagicMock
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage


class MockMessage:
    """Mock message class that simulates LangChain message structure."""
    
    def __init__(self, content: Any, role: str = "human", tool_calls: List[Dict] = None, name: str = None):
        self.content = content
        self.role = role
        self.tool_calls = tool_calls or []
        self.name = name
        self.id = f"msg_{id(self)}"
    
    def __repr__(self):
        return f"MockMessage(role={self.role}, name={self.name}, has_tool_calls={bool(self.tool_calls)}, content_type={type(self.content).__name__})"


def create_tool_use_message(content: str = "thinking...") -> MockMessage:
    """Create a mock AI message with a tool_use (tool call)."""
    return MockMessage(
        content=[
            {"type": "text", "text": content},
            {"type": "tool_use", "id": "tool_123", "name": "execute_sql_query"}
        ],
        role="assistant",
        tool_calls=[{"id": "tool_123", "name": "execute_sql_query"}]
    )


def create_tool_result_message(content: str = "Query result...") -> MockMessage:
    """Create a mock user/tool message with a tool_result."""
    return MockMessage(
        content=[
            {"type": "tool_result", "tool_use_id": "tool_123", "content": content}
        ],
        role="tool",
        name="execute_sql_query"
    )


def create_regular_message(content: str, role: str = "human") -> MockMessage:
    """Create a regular message without tool interactions."""
    return MockMessage(content=content, role=role)


def test_trim_messages_preserves_tool_use_result_pairs():
    """
    Test that tool_use/tool_result pairs are never split during trimming.
    
    This simulates the scenario where large responses (10K rows) trigger
    token-based trimming that could orphan tool_result messages.
    """
    from app.domain.queries.agent import _trim_messages_impl, _message_has_tool_use, _message_has_tool_result
    
    # Create a conversation with multiple tool interactions
    # System message (index 0 - always kept)
    system_msg = create_regular_message("You are a helpful assistant", role="system")
    
    # Build a sequence that simulates a long conversation with tool calls
    messages = [
        system_msg,
        create_regular_message("What is the total revenue?", role="human"),
        create_tool_use_message("Let me query the database..."),
        create_tool_result_message("Revenue: $1M"),
        create_regular_message("The total revenue is $1M", role="assistant"),
        create_regular_message("Show me top customers", role="human"),
        create_tool_use_message("Querying customers..."),
        create_tool_result_message("Customer list with 10,000 rows..."),  # Large result
        create_regular_message("Here are your top customers...", role="assistant"),
        create_regular_message("Now show me by region", role="human"),
        create_tool_use_message("Analyzing by region..."),
        create_tool_result_message("Regional breakdown with 5,000 rows..."),  # Large result
        create_regular_message("Here's the regional breakdown...", role="assistant"),
    ]
    
    # Call trim_messages implementation
    result = _trim_messages_impl(messages)
    
    # If trimming occurred, verify tool_use/tool_result pairs are preserved
    if result is not None:
        cut_index, first_msg = result
        trimmed_messages = [first_msg] + messages[cut_index:]
        
        # Verify no orphaned tool_result messages
        for i, msg in enumerate(trimmed_messages):
            if _message_has_tool_result(msg):
                # Look backward to find the corresponding tool_use
                found_tool_use = False
                for j in range(i - 1, -1, -1):
                    if _message_has_tool_use(trimmed_messages[j]):
                        found_tool_use = True
                        break
                
                assert found_tool_use, f"Orphaned tool_result at index {i}: {msg}"
        
        # Verify system message is always kept
        assert trimmed_messages[0] == system_msg, "System message should always be preserved"


def test_trim_messages_with_large_token_count():
    """
    Test trimming behavior when messages exceed token limit.
    
    This specifically tests the token-based trimming logic that caused
    the original bug with 10K row results.
    """
    from app.domain.queries.agent import _trim_messages_impl, _message_has_tool_use, _message_has_tool_result
    
    # Create messages with large content to trigger token-based trimming
    large_content = "X" * 50000  # ~12,500 tokens
    
    messages = [
        create_regular_message("System prompt", role="system"),
        create_regular_message("Query 1", role="human"),
        create_tool_use_message("Executing..."),
        create_tool_result_message(large_content),  # Large result
        create_regular_message("Response 1", role="assistant"),
        create_regular_message("Query 2", role="human"),
        create_tool_use_message("Executing..."),
        create_tool_result_message(large_content),  # Large result
        create_regular_message("Response 2", role="assistant"),
        create_regular_message("Query 3", role="human"),
        create_tool_use_message("Executing..."),
        create_tool_result_message(large_content),  # Large result
        create_regular_message("Response 3", role="assistant"),
    ]
    
    result = _trim_messages_impl(messages)
    
    # Should have trimmed due to token limit
    assert result is not None, "Expected trimming to occur with large messages"
    
    cut_index, first_msg = result
    trimmed_messages = [first_msg] + messages[cut_index:]
    
    # Verify no orphaned tool_result messages
    for i, msg in enumerate(trimmed_messages):
        if _message_has_tool_result(msg):
            # Walk backward to find tool_use
            found_tool_use = False
            for j in range(i - 1, -1, -1):
                if _message_has_tool_use(trimmed_messages[j]):
                    found_tool_use = True
                    break
            
            assert found_tool_use, f"Orphaned tool_result found at index {i} after token-based trimming"
    
    # Verify first message (system prompt) is preserved
    assert trimmed_messages[0].content == "System prompt"


def test_trim_messages_edge_case_minimum_messages():
    """Test that we maintain at least 3 messages after the system message."""
    from app.domain.queries.agent import _trim_messages_impl
    
    # Create a scenario with minimal messages
    messages = [
        create_regular_message("System", role="system"),
        create_regular_message("User query", role="human"),
        create_tool_use_message(),
        create_tool_result_message(),
        create_regular_message("Response", role="assistant"),
    ]
    
    result = _trim_messages_impl(messages)
    
    # With only 5 messages, no trimming should occur
    assert result is None, "Should not trim when we have <= 6 messages"


def test_trim_messages_respects_tool_boundaries_at_cut_point():
    """
    Test that when cut_index lands on a tool_result, we adjust to include its tool_use.
    
    This is the core fix for the reported bug.
    """
    from app.domain.queries.agent import _trim_messages_impl, _message_has_tool_use, _message_has_tool_result
    
    # Create a conversation where natural cut point would be in the middle of a tool pair
    messages = [
        create_regular_message("System", role="system"),
        create_regular_message("Q1", role="human"),
        create_regular_message("A1", role="assistant"),
        create_regular_message("Q2", role="human"),
        create_regular_message("A2", role="assistant"),
        create_regular_message("Q3", role="human"),
        create_regular_message("A3", role="assistant"),
        create_regular_message("Q4", role="human"),
        create_tool_use_message("Tool call 1"),  # Index 8
        create_tool_result_message("Tool result 1"),  # Index 9 - potential cut point
        create_regular_message("A4", role="assistant"),
        create_regular_message("Q5", role="human"),
        create_tool_use_message("Tool call 2"),
        create_tool_result_message("Tool result 2"),
        create_regular_message("A5", role="assistant"),
    ]
    
    result = _trim_messages_impl(messages)
    
    if result is not None:
        cut_index, first_msg = result
        trimmed = [first_msg] + messages[cut_index:]
        
        # Verify no orphaned tool_result
        for i, msg in enumerate(trimmed):
            if _message_has_tool_result(msg):
                # Must have corresponding tool_use before it
                found = False
                for j in range(i - 1, -1, -1):
                    if _message_has_tool_use(trimmed[j]):
                        found = True
                        break
                assert found, f"Tool result at {i} has no corresponding tool_use"


def test_helper_functions_detect_tool_messages():
    """Test that helper functions correctly identify tool_use and tool_result messages."""
    from app.domain.queries.agent import _message_has_tool_use, _message_has_tool_result
    
    # Test tool_use detection
    tool_use_msg = create_tool_use_message()
    assert _message_has_tool_use(tool_use_msg), "Should detect tool_use in content"
    
    # Test tool_result detection
    tool_result_msg = create_tool_result_message()
    assert _message_has_tool_result(tool_result_msg), "Should detect tool_result in content"
    
    # Test regular message (should have neither)
    regular_msg = create_regular_message("Hello")
    assert not _message_has_tool_use(regular_msg), "Regular message should not have tool_use"
    assert not _message_has_tool_result(regular_msg), "Regular message should not have tool_result"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
