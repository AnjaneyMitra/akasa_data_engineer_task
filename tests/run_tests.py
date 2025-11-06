#!/usr/bin/env python3
"""
Streamlined pytest runner for Akasa Data Engineering Pipeline.
Demonstrates comprehensive testing of production-ready data pipeline.
"""

import subprocess
import sys
from pathlib import Path

def main():
    """Run the comprehensive test suite using pytest."""
    
    print("🚀 AKASA DATA ENGINEERING PIPELINE - PYTEST VALIDATION")
    print("=" * 70)
    print("Testing production-ready data engineering pipeline:")
    print("• Dual Architecture (Table + In-Memory approaches)")
    print("• 4 Core KPIs with Business Logic Validation")
    print("• Data Quality & Security Compliance")
    print("• Performance & Error Handling")
    print("• Modular Architecture Validation")
    print("=" * 70)
    
    # Set up environment
    project_root = Path(__file__).parent.parent
    
    # Configure pytest command
    cmd = [
        sys.executable, '-m', 'pytest', 
        'tests/test_pipeline.py',
        '-v',                    # Verbose output
        '--tb=short',           # Shorter tracebacks
        '--disable-warnings',   # Cleaner output
        '-x',                   # Stop on first failure
        f'--rootdir={project_root}',
    ]
    
    # Set environment variables
    import os
    env = {
        'PYTHONPATH': f"{project_root}:{project_root}/src",
        **dict(os.environ)
    }
    
    print(f"\n🧪 Executing: {' '.join(cmd)}")
    print(f"📂 Working Directory: {project_root}")
    print(f"🐍 Python Path: {env['PYTHONPATH']}")
    print("\n" + "=" * 70 + "\n")
    
    try:
        # Run pytest
        result = subprocess.run(
            cmd, 
            cwd=project_root,
            env=env,
            text=True
        )
        
        # Summary
        print("\n" + "=" * 70)
        if result.returncode == 0:
            print("✅ ALL TESTS PASSED - Pipeline validation successful!")
            print("🎯 Production-ready data engineering pipeline verified")
            print("\n📊 Key validations completed:")
            print("   • KPI calculations accuracy")
            print("   • Data quality standards")  
            print("   • Business logic compliance")
            print("   • Security best practices")
            print("   • Performance benchmarks")
            print("   • Architecture patterns")
        else:
            print("❌ TESTS FAILED - Check output above for details")
            print(f"Exit code: {result.returncode}")
        print("=" * 70)
        
        return result.returncode
        
    except FileNotFoundError:
        print("❌ ERROR: pytest not found. Install with: pip install pytest")
        return 1
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return 1


if __name__ == '__main__':
    import os
    sys.exit(main())
