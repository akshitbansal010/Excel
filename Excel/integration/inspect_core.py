#!/usr/bin/env python3
"""
DataEngine Pro - Core Module Inspector
=======================================

Diagnostic script to discover and report on available core module functions.
Generates a mapping report showing what was connected to the adapter.

Usage:
    python integration/inspect_core.py
    
Output:
    - List of discovered functions
    - Suggested adapter mappings
    - Missing operations that need implementation
"""

import sys
import os
import ast
import inspect
from pathlib import Path
from typing import Dict, List, Any, Optional, Set

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Try importing core modules
CORE_IMPORTS = {}
CORE_IMPORT_ERRORS = {}

# Try to import each potential core module
modules_to_try = [
    ("data_engine", ["operations", "session", "database", "helpers", "display"]),
    ("excelpy", ["core", "engine", "helpers"]),
]

for module_name, submodules in modules_to_try:
    CORE_IMPORTS[module_name] = {}
    for submod in submodules:
        full_name = f"{module_name}.{submod}"
        try:
            mod = __import__(full_name, fromlist=[submod])
            CORE_IMPORTS[module_name][submod] = mod
        except ImportError as e:
            CORE_IMPORTS[module_name][submod] = None
            CORE_IMPORT_ERRORS[full_name] = str(e)


def get_functions_from_module(mod: Any) -> List[Dict[str, Any]]:
    """Extract function information from a module."""
    if mod is None:
        return []
    
    functions = []
    for name, obj in inspect.getmembers(mod, inspect.isfunction):
        # Skip private functions
        if name.startswith('_'):
            continue
        
        try:
            sig = inspect.signature(obj)
            params = []
            for param_name, param in sig.parameters.items():
                param_info = {
                    "name": param_name,
                    "has_default": param.default != inspect.Parameter.empty,
                }
                if param.default != inspect.Parameter.empty:
                    param_info["default"] = str(param.default)
                params.append(param_info)
            
            functions.append({
                "name": name,
                "params": params,
                "doc": inspect.getdoc(obj)[:100] if inspect.getdoc(obj) else None
            })
        except Exception:
            functions.append({
                "name": name,
                "params": [],
                "doc": None
            })
    
    return functions


def get_classes_from_module(mod: Any) -> List[Dict[str, Any]]:
    """Extract class information from a module."""
    if mod is None:
        return []
    
    classes = []
    for name, obj in inspect.getmembers(mod, inspect.isclass):
        # Skip private classes
        if name.startswith('_'):
            continue
        
        methods = [m for m in dir(obj) if not m.startswith('_') and callable(getattr(obj, m))]
        
        classes.append({
            "name": name,
            "module": obj.__module__,
            "methods": methods[:10],  # Limit to first 10
            "doc": inspect.getdoc(obj)[:100] if inspect.getdoc(obj) else None
        })
    
    return classes


# Canonical adapter operations that we need to map
CANONICAL_OPERATIONS = {
    # Data loading
    "load_table": "Load data from CSV, Excel, or SQLite",
    "list_tables": "List available tables",
    
    # Preview & Schema
    "get_schema": "Get column names and types",
    "preview": "Preview rows (limited)",
    "sample": "Get random sample",
    "column_window": "Get column window",
    
    # Operations
    "op_filter": "Filter rows by condition",
    "op_sort": "Sort by columns",
    "op_aggregate": "Group and aggregate",
    "op_rank": "Add ranking column",
    "op_pivot": "Create pivot table",
    "op_sql": "Execute SQL query",
    
    # Data modification
    "op_add_column": "Add calculated column",
    "op_join": "Join tables",
    
    # Persistence
    "save": "Save to CSV/Excel/SQLite",
    "undo": "Undo last operation",
    "redo": "Redo undone operation",
}


# Known function mappings (manually curated)
KNOWN_MAPPINGS = {
    # From data_engine.operations
    "op_filter": ["op_filter", "apply_single_condition", "op_multi_filter"],
    "op_sort": ["op_sort"],
    "op_aggregate": ["op_aggregate"],
    "op_rank": ["op_rank"],
    "op_pivot": ["op_pivot", "op_pivot_table"],
    "op_add_column": ["op_add_column"],
    "op_handle_nulls": ["op_handle_nulls"],
    "op_rename_drop": ["op_rename_drop"],
    "op_dedupe": ["op_dedupe"],
    "op_change_type": ["op_change_type"],
    "op_join": ["op_join"],
    "op_save": ["op_save"],
    "op_export": ["op_export"],
    
    # From data_engine.database
    "db_load": ["db_load"],
    "db_save": ["db_save"],
    "db_tables": ["db_tables"],
    "db_schema": ["db_get_schema"],
}


def check_mapping(canonical_name: str) -> Dict[str, Any]:
    """Check if a canonical operation has a mapping to core functions."""
    possible_names = KNOWN_MAPPINGS.get(canonical_name, [])
    
    found_in = []
    for mod_name, submods in CORE_IMPORTS.items():
        for submod_name, mod in submods.items():
            if mod is not None:
                for func_name in possible_names:
                    if hasattr(mod, func_name):
                        found_in.append(f"{mod_name}.{submod_name}.{func_name}")
    
    return {
        "canonical": canonical_name,
        "description": CANONICAL_OPERATIONS.get(canonical_name, ""),
        "possible_core_names": possible_names,
        "found_in": found_in,
        "mapped": len(found_in) > 0
    }


def print_report():
    """Print the diagnostic report."""
    print("=" * 70)
    print("DataEngine Pro - Core Module Inspector")
    print("=" * 70)
    print()
    
    # Section 1: Module availability
    print("📦 MODULE AVAILABILITY")
    print("-" * 40)
    
    for mod_name, submods in CORE_IMPORTS.items():
        print(f"\n  {mod_name}:")
        for submod_name, mod in submods.items():
            status = "✓" if mod else "✗"
            error = f" ({CORE_IMPORT_ERRORS.get(f'{mod_name}.{submod_name}', 'unknown error')})" if not mod else ""
            print(f"    {status} {submod_name}{error}")
    
    print()
    
    # Section 2: Discovered functions
    print("\n📋 DISCOVERED FUNCTIONS")
    print("-" * 40)
    
    for mod_name, submods in CORE_IMPORTS.items():
        for submod_name, mod in submods.items():
            if mod is not None:
                funcs = get_functions_from_module(mod)
                classes = get_classes_from_module(mod)
                
                if funcs:
                    print(f"\n  {mod_name}.{submod_name} (functions):")
                    for func in funcs[:15]:  # Limit output
                        params = ", ".join(p["name"] for p in func["params"][:3])
                        print(f"    - {func['name']}({params})")
                    if len(funcs) > 15:
                        print(f"    ... and {len(funcs) - 15} more")
                
                if classes:
                    print(f"\n  {mod_name}.{submod_name} (classes):")
                    for cls in classes[:5]:
                        print(f"    - {cls['name']} ({len(cls['methods'])} methods)")
    
    print()
    
    # Section 3: Adapter mapping status
    print("\n🔌 ADAPTER MAPPING STATUS")
    print("-" * 40)
    
    mapped = []
    unmapped = []
    
    for canonical_name in CANONICAL_OPERATIONS:
        mapping = check_mapping(canonical_name)
        if mapping["mapped"]:
            mapped.append(mapping)
        else:
            unmapped.append(mapping)
    
    print("\n  ✓ MAPPED OPERATIONS:")
    for m in mapped:
        print(f"    {m['canonical']}:")
        for func in m["found_in"]:
            print(f"      → {func}")
    
    print("\n  ✗ UNMAPPED OPERATIONS:")
    for m in unmapped:
        print(f"    {m['canonical']}: {m['description']}")
        print(f"      Expected functions: {', '.join(m['possible_core_names'])}")
    
    print()
    
    # Section 4: Recommendations
    print("\n💡 RECOMMENDATIONS")
    print("-" * 40)
    
    if unmapped:
        print("\n  To enable full adapter functionality, consider implementing:")
        for m in unmapped[:5]:
            print(f"    - {m['canonical']} in data_engine.operations")
    else:
        print("\n  ✓ All canonical operations are mapped!")
    
    print()
    
    # Section 5: Quick test
    print("\n🧪 QUICK FUNCTIONALITY TEST")
    print("-" * 40)
    
    # Try to import and use the adapter
    try:
        from integration.adapter import Adapter, get_adapter
        
        print("\n  Testing adapter initialization...")
        adapter = get_adapter()
        
        # Try creating a session
        session = adapter.create_session()
        print(f"    ✓ Created session: {session.id[:8]}...")
        
        # Try loading test data
        import pandas as pd
        test_df = pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"]})
        
        # Manually add table
        if isinstance(adapter._sessions.get(session.id), dict):
            adapter._sessions[session.id]["tables"]["test"] = test_df
            adapter._sessions[session.id]["active_table"] = "test"
        
        # Try preview
        result = adapter.preview(session.id)
        print(f"    ✓ Preview: {len(result.rows)} rows")
        
        # Try schema
        schema = adapter.get_schema(session.id)
        print(f"    ✓ Schema: {len(schema['columns'])} columns")
        
        print("\n  ✓ Adapter basic functionality test passed!")
        
    except Exception as e:
        print(f"\n  ✗ Adapter test failed: {e}")
    
    print()
    print("=" * 70)
    print("End of Report")
    print("=" * 70)


def main():
    """Main entry point."""
    print_report()


if __name__ == "__main__":
    main()
