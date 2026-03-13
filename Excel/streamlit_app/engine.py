"""
DataEngine Pro - Streamlit Engine Wrapper
=========================================

This module bridges the Streamlit UI with the existing data_engine operations.
It provides a clean interface for the UI to use all the existing modules.

Usage:
    from streamlit_app.engine import Engine
    engine = Engine(st.session_state)
    engine.filter(...)
    engine.sort(...)
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, List, Tuple
import traceback


class Engine:
    """
    Engine wrapper that connects Streamlit session to data_engine operations.
    Provides a simple API for all data operations while using the existing modules.
    """
    
    def __init__(self, session_state):
        """
        Initialize the engine with Streamlit session state.
        
        Args:
            session_state: Streamlit session_state object containing session_tables
        """
        self._session = session_state
        
        # Initialize session tables if not exist
        if 'session_tables' not in self._session:
            self._session.session_tables = {}
        if 'active_table' not in self._session:
            self._session.active_table = ""
        if 'operation_history' not in self._session:
            self._session.operation_history = []
    
    # =========================================================================
    # DATA LOADING
    # =========================================================================
    
    def load_csv(self, file, chunk_size: int = 50000) -> Tuple[bool, str]:
