"""
Export Profit and Loss Statement
"""
import os
import json
from datetime import datetime
from new_int import get_profit_and_loss

def export_pnl():
    """Export Profit and Loss statement for Q1-Q3 2025"""
    # Create exports directory if it doesn't exist
    os.makedirs('xero_exports/financial_statements', exist_ok=True)
    
    # Define date ranges
    q1_start = '2025-01-01'
    q1_end = '2025-03-31'
    q2_start = '2025-04-01'
    q2_end = '2025-06-30'
    q3_start = '2025-07-01'
    q3_end = '2025-09-30'
    
    print("Exporting Profit and Loss statements for 2025...")
    
    try:
        # Export Quarterly Profit & Loss Statements
        for q, (start, end) in enumerate([(q1_start, q1_end), (q2_start, q2_end), (q3_start, q3_end)], 1):
            print(f"\nExporting Q{q} 2025 ({start} to {end})")
            pnl = get_profit_and_loss(start, end)
            filename = f"xero_exports/financial_statements/pnl_q{q}_2025.json"
            with open(filename, 'w') as f:
                json.dump(pnl, f, indent=2)
            print(f"✓ Saved to {filename}")
        
        print("\n✅ All Profit and Loss statements exported successfully!")
        
    except Exception as e:
        print(f"\n❌ Error exporting Profit and Loss statements: {str(e)}")
        raise

if __name__ == "__main__":
    export_pnl()
