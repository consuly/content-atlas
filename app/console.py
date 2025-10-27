#!/usr/bin/env python3
"""
Console interface for natural language database queries.
Provides a REPL-like interface to query the database using natural language.
"""

import argparse
import sys
import os
import uuid
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.prompt import Prompt
from rich.live import Live
from rich.spinner import Spinner
import pandas as pd
from io import StringIO

# Add the app directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .query_agent import query_database_with_agent
from .config import settings


class DatabaseConsole:
    """Interactive console for database queries."""

    def __init__(self):
        self.console = Console()
        self.history = []
        # Generate unique thread_id for this console session to maintain conversation memory
        self.thread_id = str(uuid.uuid4())

    def print_welcome(self):
        """Print welcome message."""
        welcome_text = Text("🤖 Database Query Console", style="bold blue")
        welcome_panel = Panel.fit(
            "[green]Welcome to the interactive database query console![/green]\n\n"
            "• Type natural language queries (e.g., 'Show me all customers')\n"
            "• Type 'help' for commands\n"
            "• Type 'exit' or 'quit' to exit\n"
            "• Type 'history' to see previous queries\n\n"
            "[dim]Using Claude-powered natural language to SQL conversion[/dim]",
            title=welcome_text,
            border_style="blue"
        )
        self.console.print(welcome_panel)

    def print_help(self):
        """Print help information."""
        help_table = Table(title="Available Commands")
        help_table.add_column("Command", style="cyan", no_wrap=True)
        help_table.add_column("Description", style="white")

        help_table.add_row("help", "Show this help message")
        help_table.add_row("exit/quit", "Exit the console")
        help_table.add_row("history", "Show query history")
        help_table.add_row("clear", "Clear the screen")
        help_table.add_row("<query>", "Execute natural language query")

        self.console.print(help_table)
        self.console.print("\n[dim]Examples:[/dim]")
        self.console.print("  'Show me all customers'")
        self.console.print("  'What are the top 5 products by sales?'")
        self.console.print("  'How many orders were placed last month?'\n")

    def format_query_result(self, result: dict) -> None:
        """Format and display query results."""
        if not result.get("success"):
            error_panel = Panel(
                f"[red]❌ Query failed:[/red]\n{result.get('error', 'Unknown error')}",
                title="Error",
                border_style="red"
            )
            self.console.print(error_panel)
            return

        # Success panel
        success_panel = Panel(
            f"[green]✅ {result.get('response', '')}[/green]",
            title="Query Result",
            border_style="green"
        )
        self.console.print(success_panel)

        # Show executed SQL if available
        if result.get("executed_sql"):
            sql_panel = Panel(
                f"[blue]{result['executed_sql']}[/blue]",
                title="Executed SQL",
                border_style="blue"
            )
            self.console.print(sql_panel)

        # Show CSV data as formatted table if available
        if result.get("data_csv"):
            try:
                # Parse CSV data
                csv_data = StringIO(result["data_csv"])
                df = pd.read_csv(csv_data)

                # Create rich table
                table = Table(title="Query Results")
                table.add_column("#", style="dim", justify="right")

                # Add columns
                for col in df.columns:
                    table.add_column(col, style="white")

                # Add rows
                for idx, row in df.iterrows():
                    row_data = [str(idx + 1)]
                    for col in df.columns:
                        value = str(row[col]) if pd.notna(row[col]) else ""
                        row_data.append(value)
                    table.add_row(*row_data)

                self.console.print(table)

                # Show summary
                if result.get("rows_returned"):
                    summary = f"Rows returned: {result['rows_returned']}"
                    if result.get("execution_time_seconds"):
                        summary += f" | Execution time: {result['execution_time_seconds']:.2f}s"
                    self.console.print(f"[dim]{summary}[/dim]")

            except Exception as e:
                # Fallback to raw CSV display
                csv_panel = Panel(
                    result["data_csv"],
                    title="Raw CSV Data",
                    border_style="yellow"
                )
                self.console.print(csv_panel)

    def execute_query(self, query: str) -> dict:
        """Execute a natural language query with conversation memory."""
        try:
            # Show spinner while processing
            with self.console.status("[bold green]Processing query...", spinner="dots"):
                # Pass thread_id to maintain conversation memory across queries
                result = query_database_with_agent(query, thread_id=self.thread_id)

            return result

        except Exception as e:
            return {
                "success": False,
                "error": f"Console error: {str(e)}",
                "response": f"An error occurred: {str(e)}"
            }

    def show_history(self):
        """Show query history."""
        if not self.history:
            self.console.print("[yellow]No queries in history yet.[/yellow]")
            return

        history_table = Table(title="Query History")
        history_table.add_column("#", style="dim", justify="right")
        history_table.add_column("Query", style="white")
        history_table.add_column("Status", style="white")

        for idx, (query, success) in enumerate(self.history[-10:], 1):  # Show last 10
            status = "[green]✓[/green]" if success else "[red]✗[/red]"
            history_table.add_row(str(idx), query, status)

        self.console.print(history_table)

    def run_interactive(self):
        """Run the interactive console."""
        self.print_welcome()

        while True:
            try:
                # Get user input
                user_input = Prompt.ask("\n[bold cyan]Query[/bold cyan]").strip()

                if not user_input:
                    continue

                # Handle commands
                if user_input.lower() in ['exit', 'quit', 'q']:
                    self.console.print("[yellow]Goodbye! 👋[/yellow]")
                    break

                elif user_input.lower() == 'help':
                    self.print_help()

                elif user_input.lower() == 'history':
                    self.show_history()

                elif user_input.lower() == 'clear':
                    self.console.clear()
                    self.print_welcome()

                else:
                    # Execute query
                    result = self.execute_query(user_input)
                    self.format_query_result(result)

                    # Add to history
                    self.history.append((user_input, result.get("success", False)))

            except KeyboardInterrupt:
                self.console.print("\n[yellow]Interrupted. Type 'exit' to quit.[/yellow]")
            except EOFError:
                self.console.print("\n[yellow]Goodbye! 👋[/yellow]")
                break

    def run_single_query(self, query: str):
        """Run a single query and exit."""
        result = self.execute_query(query)
        self.format_query_result(result)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Database Query Console - Natural language database queries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Start interactive console
  %(prog)s "Show me all customers"           # Run single query
  %(prog)s --help                            # Show this help
        """
    )

    parser.add_argument(
        'query',
        nargs='?',
        help='Natural language query to execute (if not provided, starts interactive mode)'
    )

    parser.add_argument(
        '--format',
        choices=['table', 'csv', 'json'],
        default='table',
        help='Output format for single queries (default: table)'
    )

    args = parser.parse_args()

    # Check if we have required environment variables
    if not hasattr(settings, 'anthropic_api_key') or not settings.anthropic_api_key:
        console = Console()
        console.print("[red]❌ Error: ANTHROPIC_API_KEY environment variable not set[/red]")
        console.print("Please set your Anthropic API key:")
        console.print("  export ANTHROPIC_API_KEY='your-key-here'")
        sys.exit(1)

    # Create console instance
    db_console = DatabaseConsole()

    if args.query:
        # Single query mode
        db_console.run_single_query(args.query)
    else:
        # Interactive mode
        db_console.run_interactive()


if __name__ == "__main__":
    main()
