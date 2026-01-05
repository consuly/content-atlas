#!/usr/bin/env python3
"""
Generate OpenAPI schema with diff reporting.

This script generates the OpenAPI schema from the FastAPI application,
compares it with the existing schema, and creates a detailed diff report.

Usage:
    python scripts/generate_openapi_with_diff.py

Outputs:
    - openapi.json (updated schema)
    - docs/openapi-changes/YYYY-MM-DD.md (diff report)
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Set, Optional
import sys


def load_schema(path: Path) -> Optional[Dict[str, Any]]:
    """Load JSON schema from file."""
    if not path.exists():
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_schema(schema: Dict[str, Any], path: Path) -> None:
    """Save JSON schema to file with formatting."""
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)


def get_paths(schema: Dict[str, Any]) -> Dict[str, List[str]]:
    """Extract all paths and their HTTP methods from schema."""
    paths = schema.get('paths', {})
    result = {}
    for path, methods in paths.items():
        result[path] = [method for method in methods.keys()]
    return result


def get_schemas(schema: Dict[str, Any]) -> Set[str]:
    """Extract all schema names from components."""
    components = schema.get('components', {})
    schemas = components.get('schemas', {})
    return set(schemas.keys())


def get_schema_properties(schema: Dict[str, Any], schema_name: str) -> Dict[str, Any]:
    """Get properties of a specific schema."""
    components = schema.get('components', {})
    schemas = components.get('schemas', {})
    schema_def = schemas.get(schema_name, {})
    return schema_def.get('properties', {})


def get_schema_required_fields(schema: Dict[str, Any], schema_name: str) -> List[str]:
    """Get required fields of a specific schema."""
    components = schema.get('components', {})
    schemas = components.get('schemas', {})
    schema_def = schemas.get(schema_name, {})
    return schema_def.get('required', [])


def compare_schemas(old_schema: Optional[Dict[str, Any]], 
                   new_schema: Dict[str, Any]) -> Dict[str, Any]:
    """Compare old and new schemas and return structured diff."""
    
    if old_schema is None:
        # First time generation
        return {
            'summary': {
                'endpoints_added': len(get_paths(new_schema)),
                'endpoints_removed': 0,
                'endpoints_modified': 0,
                'schemas_added': len(get_schemas(new_schema)),
                'schemas_removed': 0,
                'schemas_modified': 0,
                'breaking_changes_count': 0
            },
            'breaking_changes': [],
            'non_breaking_changes': [],
            'endpoint_changes': [],
            'schema_changes': []
        }
    
    # Initialize result
    result = {
        'summary': {},
        'breaking_changes': [],
        'non_breaking_changes': [],
        'endpoint_changes': [],
        'schema_changes': []
    }
    
    # Compare paths (endpoints)
    old_paths = get_paths(old_schema)
    new_paths = get_paths(new_schema)
    
    all_paths = set(old_paths.keys()) | set(new_paths.keys())
    
    for path in sorted(all_paths):
        old_methods = old_paths.get(path, [])
        new_methods = new_paths.get(path, [])
        
        all_methods = set(old_methods) | set(new_methods)
        
        for method in sorted(all_methods):
            if method in new_methods and method not in old_methods:
                # Added endpoint
                result['endpoint_changes'].append({
                    'type': 'endpoint_added',
                    'path': path,
                    'method': method.upper(),
                    'summary': new_schema['paths'][path][method].get('summary', '')
                })
            elif method in old_methods and method not in new_methods:
                # Removed endpoint (BREAKING)
                result['breaking_changes'].append({
                    'type': 'endpoint_removed',
                    'path': path,
                    'method': method.upper()
                })
                result['endpoint_changes'].append({
                    'type': 'endpoint_removed',
                    'path': path,
                    'method': method.upper()
                })
            elif method in old_methods and method in new_methods:
                # Modified endpoint
                old_endpoint = old_schema['paths'][path][method]
                new_endpoint = new_schema['paths'][path][method]
                
                changes = compare_endpoints(path, method, old_endpoint, new_endpoint)
                if changes:
                    result['endpoint_changes'].append({
                        'type': 'endpoint_modified',
                        'path': path,
                        'method': method.upper(),
                        'changes': changes
                    })
    
    # Compare schemas
    old_schemas = get_schemas(old_schema)
    new_schemas = get_schemas(new_schema)
    
    for schema_name in sorted(new_schemas - old_schemas):
        result['schema_changes'].append({
            'type': 'schema_added',
            'schema_name': schema_name
        })
    
    for schema_name in sorted(old_schemas - new_schemas):
        result['breaking_changes'].append({
            'type': 'schema_removed',
            'schema_name': schema_name
        })
        result['schema_changes'].append({
            'type': 'schema_removed',
            'schema_name': schema_name
        })
    
    for schema_name in sorted(old_schemas & new_schemas):
        changes = compare_schema_fields(old_schema, new_schema, schema_name)
        if changes:
            result['schema_changes'].append({
                'type': 'schema_modified',
                'schema_name': schema_name,
                'changes': changes
            })
    
    # Calculate summary
    result['summary'] = {
        'endpoints_added': sum(1 for c in result['endpoint_changes'] if c['type'] == 'endpoint_added'),
        'endpoints_removed': sum(1 for c in result['endpoint_changes'] if c['type'] == 'endpoint_removed'),
        'endpoints_modified': sum(1 for c in result['endpoint_changes'] if c['type'] == 'endpoint_modified'),
        'schemas_added': sum(1 for c in result['schema_changes'] if c['type'] == 'schema_added'),
        'schemas_removed': sum(1 for c in result['schema_changes'] if c['type'] == 'schema_removed'),
        'schemas_modified': sum(1 for c in result['schema_changes'] if c['type'] == 'schema_modified'),
        'breaking_changes_count': len(result['breaking_changes'])
    }
    
    return result


def compare_endpoints(path: str, method: str, 
                    old_endpoint: Dict[str, Any], 
                    new_endpoint: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Compare two endpoint definitions."""
    changes = []
    
    # Compare parameters
    old_params = {p['name']: p for p in old_endpoint.get('parameters', [])}
    new_params = {p['name']: p for p in new_endpoint.get('parameters', [])}
    
    all_params = set(old_params.keys()) | set(new_params.keys())
    
    for param_name in all_params:
        if param_name in new_params and param_name not in old_params:
            changes.append({
                'type': 'parameter_added',
                'parameter': param_name,
                'location': new_params[param_name].get('in', 'unknown'),
                'required': new_params[param_name].get('required', False)
            })
        elif param_name in old_params and param_name not in new_params:
            changes.append({
                'type': 'parameter_removed',
                'parameter': param_name,
                'breaking': True
            })
            old_req = old_params[param_name].get('required', False)
            if old_req:
                # Breaking: removed required parameter
                changes[-1]['breaking_reason'] = 'Previously required parameter'
        elif param_name in old_params and param_name in new_params:
            if old_params[param_name] != new_params[param_name]:
                changes.append({
                    'type': 'parameter_changed',
                    'parameter': param_name,
                    'breaking': is_breaking_param_change(old_params[param_name], new_params[param_name])
                })
    
    # Compare request body
    if 'requestBody' in old_endpoint or 'requestBody' in new_endpoint:
        old_body = old_endpoint.get('requestBody', {}).get('content', {}).get('application/json', {}).get('schema', {}).get('$ref', '').replace('#/components/schemas/', '')
        new_body = new_endpoint.get('requestBody', {}).get('content', {}).get('application/json', {}).get('schema', {}).get('$ref', '').replace('#/components/schemas/', '')
        
        if old_body != new_body:
            changes.append({
                'type': 'request_body_changed',
                'old_schema': old_body or 'none',
                'new_schema': new_body or 'none',
                'breaking': bool(old_body) and old_body != new_body
            })
    
    # Compare responses
    old_responses = old_endpoint.get('responses', {})
    new_responses = new_endpoint.get('responses', {})
    
    for status_code in set(old_responses.keys()) | set(new_responses.keys()):
        if status_code in old_responses and status_code not in new_responses:
            changes.append({
                'type': 'response_removed',
                'status_code': status_code,
                'breaking': status_code in ['200', '201']
            })
        elif status_code in new_responses and status_code not in old_responses:
            changes.append({
                'type': 'response_added',
                'status_code': status_code
            })
        elif status_code in old_responses and status_code in new_responses:
            old_resp_schema = old_responses[status_code].get('content', {}).get('application/json', {}).get('schema', {}).get('$ref', '').replace('#/components/schemas/', '')
            new_resp_schema = new_responses[status_code].get('content', {}).get('application/json', {}).get('schema', {}).get('$ref', '').replace('#/components/schemas/', '')
            
            if old_resp_schema != new_resp_schema:
                changes.append({
                    'type': 'response_schema_changed',
                    'status_code': status_code,
                    'old_schema': old_resp_schema or 'none',
                    'new_schema': new_resp_schema or 'none',
                    'breaking': status_code in ['200', '201'] and bool(old_resp_schema)
                })
    
    return changes


def is_breaking_param_change(old_param: Dict[str, Any], new_param: Dict[str, Any]) -> bool:
    """Determine if a parameter change is breaking."""
    old_req = old_param.get('required', False)
    new_req = new_param.get('required', False)
    
    # Making optional parameter required is breaking
    if not old_req and new_req:
        return True
    
    # Type changes can be breaking
    old_type = old_param.get('type')
    new_type = new_param.get('type')
    if old_type != new_type:
        # Most type changes are breaking
        return True
    
    # Enum value removal is breaking
    old_enum = old_param.get('enum', [])
    new_enum = new_param.get('enum', [])
    if old_enum and new_enum and set(old_enum) - set(new_enum):
        return True
    
    return False


def compare_schema_fields(old_schema: Dict[str, Any], 
                       new_schema: Dict[str, Any],
                       schema_name: str) -> List[Dict[str, Any]]:
    """Compare fields within a schema definition."""
    changes = []
    
    old_props = get_schema_properties(old_schema, schema_name)
    new_props = get_schema_properties(new_schema, schema_name)
    old_required = set(get_schema_required_fields(old_schema, schema_name))
    new_required = set(get_schema_required_fields(new_schema, schema_name))
    
    # Get schema definitions for type information
    components = new_schema.get('components', {})
    schemas_dict = components.get('schemas', {})
    schema_def = schemas_dict.get(schema_name, {})
    
    all_fields = set(old_props.keys()) | set(new_props.keys())
    
    for field_name in sorted(all_fields):
        if field_name in new_props and field_name not in old_props:
            # Added field
            changes.append({
                'type': 'field_added',
                'field': field_name,
                'required': field_name in new_required,
                'field_type': new_props[field_name].get('type', 'unknown')
            })
        elif field_name in old_props and field_name not in new_props:
            # Removed field (potentially breaking)
            was_required = field_name in old_required
            changes.append({
                'type': 'field_removed',
                'field': field_name,
                'breaking': was_required,
                'field_type': old_props[field_name].get('type', 'unknown')
            })
        elif field_name in old_props and field_name in new_props:
            old_field = old_props[field_name]
            new_field = new_props[field_name]
            
            # Check type changes
            if old_field.get('type') != new_field.get('type'):
                changes.append({
                    'type': 'field_type_changed',
                    'field': field_name,
                    'old_type': old_field.get('type'),
                    'new_type': new_field.get('type'),
                    'breaking': is_breaking_type_change(old_field.get('type'), new_field.get('type'))
                })
            
            # Check required changes
            was_required = field_name in old_required
            is_required = field_name in new_required
            
            if was_required and not is_required:
                changes.append({
                    'type': 'field_made_optional',
                    'field': field_name,
                    'breaking': False
                })
            elif not was_required and is_required:
                changes.append({
                    'type': 'field_made_required',
                    'field': field_name,
                    'breaking': True
                })
            
            # Check description changes
            old_desc = old_field.get('description', '')
            new_desc = new_field.get('description', '')
            if old_desc != new_desc:
                changes.append({
                    'type': 'field_description_changed',
                    'field': field_name,
                    'breaking': False
                })
    
    return changes


def is_breaking_type_change(old_type: Optional[str], new_type: Optional[str]) -> bool:
    """Determine if a type change is breaking."""
    if old_type == new_type:
        return False
    
    # These type changes are generally not breaking
    non_breaking_changes = [
        ('integer', 'number'),
        ('string', 'string'),  # format changes
    ]
    
    return (old_type, new_type) not in non_breaking_changes


def generate_diff_report(diff: Dict[str, Any], timestamp: str) -> str:
    """Generate markdown report from diff object."""
    lines = []
    
    # Header
    lines.append(f"# OpenAPI Changes Report")
    lines.append(f"\n**Generated:** {timestamp}")
    lines.append(f"\n---")
    
    # Summary
    summary = diff['summary']
    lines.append(f"\n## Summary")
    lines.append(f"- **Endpoints Added:** {summary['endpoints_added']}")
    lines.append(f"- **Endpoints Removed:** {summary['endpoints_removed']}")
    lines.append(f"- **Endpoints Modified:** {summary['endpoints_modified']}")
    lines.append(f"- **Schemas Added:** {summary['schemas_added']}")
    lines.append(f"- **Schemas Removed:** {summary['schemas_removed']}")
    lines.append(f"- **Schemas Modified:** {summary['schemas_modified']}")
    lines.append(f"- **Breaking Changes:** {summary['breaking_changes_count']}")
    
    # Breaking Changes
    if diff['breaking_changes']:
        lines.append(f"\n## Breaking Changes")
        for change in diff['breaking_changes']:
            if change['type'] == 'endpoint_removed':
                lines.append(f"\n### Removed Endpoint")
                lines.append(f"- `{change['method']} {change['path']}` - This endpoint was removed")
            elif change['type'] == 'schema_removed':
                lines.append(f"\n### Removed Schema")
                lines.append(f"- `{change['schema_name']}` - This schema definition was removed")
            elif change['type'] == 'parameter_removed' and change.get('breaking_reason'):
                lines.append(f"\n### Removed Required Parameter")
                lines.append(f"- **Endpoint:** {change.get('endpoint', 'unknown')}")
                lines.append(f"- **Parameter:** `{change['parameter']}` - {change['breaking_reason']}")
            elif change['type'] == 'field_removed' and change['breaking']:
                lines.append(f"\n### Removed Required Field")
                lines.append(f"- **Schema:** `{change.get('schema_name', 'unknown')}`")
                lines.append(f"- **Field:** `{change['field']}` - Previously required field removed")
            elif change['type'] == 'field_made_required':
                lines.append(f"\n### Field Made Required")
                lines.append(f"- **Schema:** `{change.get('schema_name', 'unknown')}`")
                lines.append(f"- **Field:** `{change['field']}` - Now required (was optional)")
            elif change['type'] == 'field_type_changed' and change['breaking']:
                lines.append(f"\n### Field Type Changed (Breaking)")
                lines.append(f"- **Schema:** `{change.get('schema_name', 'unknown')}`")
                lines.append(f"- **Field:** `{change['field']}`")
                lines.append(f"  - **Old Type:** `{change['old_type']}`")
                lines.append(f"  - **New Type:** `{change['new_type']}`")
    
    # Endpoint Changes
    if diff['endpoint_changes']:
        lines.append(f"\n## Endpoint Changes")
        
        for change in diff['endpoint_changes']:
            if change['type'] == 'endpoint_added':
                lines.append(f"\n### Added Endpoint")
                lines.append(f"- `{change['method']} {change['path']}`")
                if change.get('summary'):
                    lines.append(f"  - **Summary:** {change['summary']}")
            
            elif change['type'] == 'endpoint_removed':
                lines.append(f"\n### Removed Endpoint")
                lines.append(f"- `{change['method']} {change['path']}`")
            
            elif change['type'] == 'endpoint_modified':
                lines.append(f"\n### Modified Endpoint: `{change['method']} {change['path']}`")
                for sub_change in change['changes']:
                    format_endpoint_change(lines, sub_change)
    
    # Schema Changes
    if diff['schema_changes']:
        lines.append(f"\n## Schema Changes")
        
        for change in diff['schema_changes']:
            if change['type'] == 'schema_added':
                lines.append(f"\n### Added Schema")
                lines.append(f"- `{change['schema_name']}` - New schema definition")
            
            elif change['type'] == 'schema_removed':
                lines.append(f"\n### Removed Schema")
                lines.append(f"- `{change['schema_name']}` - Schema definition removed")
            
            elif change['type'] == 'schema_modified':
                lines.append(f"\n### Modified Schema: `{change['schema_name']}`")
                for sub_change in change['changes']:
                    format_schema_change(lines, sub_change)
    
    # Non-breaking info
    if not diff['breaking_changes']:
        lines.append(f"\n## Breaking Changes")
        lines.append("\n✅ **No breaking changes detected**")
    
    return '\n'.join(lines)


def format_endpoint_change(lines: List[str], change: Dict[str, Any]) -> None:
    """Format an endpoint change for markdown output."""
    if change['type'] == 'parameter_added':
        lines.append(f"- **Added Parameter:** `{change['parameter']}`")
        lines.append(f"  - **Location:** {change['location']}")
        if change['required']:
            lines.append(f"  - **Required:** Yes")
    elif change['type'] == 'parameter_removed':
        lines.append(f"- **Removed Parameter:** `{change['parameter']}`")
        if change.get('breaking'):
            lines.append(f"  - ⚠️ **Breaking Change**")
    elif change['type'] == 'parameter_changed':
        lines.append(f"- **Changed Parameter:** `{change['parameter']}`")
        if change.get('breaking'):
            lines.append(f"  - ⚠️ **Breaking Change**")
    elif change['type'] == 'request_body_changed':
        lines.append(f"- **Request Body Changed:**")
        lines.append(f"  - **Old Schema:** `{change['old_schema']}`")
        lines.append(f"  - **New Schema:** `{change['new_schema']}`")
        if change.get('breaking'):
            lines.append(f"  - ⚠️ **Breaking Change**")
    elif change['type'] == 'response_added':
        lines.append(f"- **Added Response:** {change['status_code']}")
    elif change['type'] == 'response_removed':
        lines.append(f"- **Removed Response:** {change['status_code']}")
        if change.get('breaking'):
            lines.append(f"  - ⚠️ **Breaking Change**")
    elif change['type'] == 'response_schema_changed':
        lines.append(f"- **Response Schema Changed ({change['status_code']}):**")
        lines.append(f"  - **Old Schema:** `{change['old_schema']}`")
        lines.append(f"  - **New Schema:** `{change['new_schema']}`")
        if change.get('breaking'):
            lines.append(f"  - ⚠️ **Breaking Change**")


def format_schema_change(lines: List[str], change: Dict[str, Any]) -> None:
    """Format a schema change for markdown output."""
    if change['type'] == 'field_added':
        lines.append(f"- **Added Field:** `{change['field']}`")
        lines.append(f"  - **Type:** `{change['field_type']}`")
        if change['required']:
            lines.append(f"  - **Required:** Yes")
    elif change['type'] == 'field_removed':
        lines.append(f"- **Removed Field:** `{change['field']}`")
        lines.append(f"  - **Type:** `{change['field_type']}`")
        if change.get('breaking'):
            lines.append(f"  - ⚠️ **Breaking Change** (was required)")
    elif change['type'] == 'field_type_changed':
        lines.append(f"- **Changed Field Type:** `{change['field']}`")
        lines.append(f"  - **Old Type:** `{change['old_type']}`")
        lines.append(f"  - **New Type:** `{change['new_type']}`")
        if change.get('breaking'):
            lines.append(f"  - ⚠️ **Breaking Change**")
    elif change['type'] == 'field_made_optional':
        lines.append(f"- **Field Made Optional:** `{change['field']}` (was required)")
    elif change['type'] == 'field_made_required':
        lines.append(f"- **Field Made Required:** `{change['field']}`")
        lines.append(f"  - ⚠️ **Breaking Change** (was optional)")
    elif change['type'] == 'field_description_changed':
        lines.append(f"- **Field Description Updated:** `{change['field']}`")


def main() -> None:
    """Main execution flow."""
    # Setup paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    openapi_file = project_root / 'openapi.json'
    changes_dir = project_root / 'docs' / 'openapi-changes'
    
    # Import app to generate schema
    sys.path.insert(0, str(project_root))
    from app.main import app
    
    # Generate new schema
    print("Generating OpenAPI schema from FastAPI application...")
    new_schema = app.openapi()
    
    # Load existing schema
    print("Loading existing openapi.json...")
    old_schema = load_schema(openapi_file)
    
    # Compare schemas
    print("Comparing schemas...")
    diff = compare_schemas(old_schema, new_schema)
    
    # Save new schema
    print(f"Saving updated schema to {openapi_file}...")
    save_schema(new_schema, openapi_file)
    
    # Create changes directory
    changes_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate and save diff report
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    date_str = datetime.now().strftime('%Y-%m-%d')
    diff_file = changes_dir / f'{date_str}.md'
    
    print(f"Generating diff report to {diff_file}...")
    report = generate_diff_report(diff, timestamp)
    
    # Append to existing report or create new one
    if diff_file.exists():
        with open(diff_file, 'a', encoding='utf-8') as f:
            f.write('\n\n---\n\n')  # Separator
            f.write(report)
    else:
        with open(diff_file, 'w', encoding='utf-8') as f:
            f.write(report)
    
    # Print summary
    summary = diff['summary']
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Endpoints Added:    {summary['endpoints_added']}")
    print(f"Endpoints Removed:  {summary['endpoints_removed']}")
    print(f"Endpoints Modified: {summary['endpoints_modified']}")
    print(f"Schemas Added:     {summary['schemas_added']}")
    print(f"Schemas Removed:   {summary['schemas_removed']}")
    print(f"Schemas Modified:  {summary['schemas_modified']}")
    print(f"Breaking Changes:   {summary['breaking_changes_count']}")
    print("=" * 50)
    print(f"\n✓ Schema updated: {openapi_file}")
    print(f"✓ Diff report:  {diff_file}")


if __name__ == '__main__':
    main()
