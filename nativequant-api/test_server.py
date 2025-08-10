#!/usr/bin/env python3
"""
Test script for the NativeQuant FastAPI server
"""

import sys
import os
sys.path.append('/Users/jwu/Desktop/nativequant-all/TAI')

# Test imports first
try:
    import fastapi
    import uvicorn
    import pandas as pd
    import numpy as np
    print("✅ Basic imports successful")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

# Test TAI imports
try:
    from TAI.source import Fred
    print("✅ TAI imports successful")
    TAI_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ TAI imports failed: {e}")
    TAI_AVAILABLE = False

# Test the main application
try:
    if TAI_AVAILABLE:
        print("Testing with TAI package...")
    else:
        print("Testing without TAI package (demo mode)...")
    
    from main import app
    print("✅ FastAPI app imported successfully")
    
    # Test a simple import
    import uvicorn
    print("✅ Ready to start server")
    
    # Start server
    print("🚀 Starting server on http://localhost:8000")
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
