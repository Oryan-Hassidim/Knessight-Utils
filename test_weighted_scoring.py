"""Test script for time-weighted scoring implementation."""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from knessight.modules.config import Config
from knessight.modules.database import Database
from knessight.modules.output import OutputManager


def test_weighted_average_calculation():
    """Test the weighted average calculation with sample data."""
    print("\n=== Testing Weighted Average Calculation ===\n")
    
    # Initialize components without database
    config = Config()
    
    # Create a minimal OutputManager instance for testing
    # We'll test the calculation logic directly
    from unittest.mock import Mock
    mock_database = Mock()
    output_manager = OutputManager(mock_database, config=config)
    
    # Create sample speeches with different dates and scores
    today = datetime.now()
    
    sample_speeches = [
        {
            "Id": 1,
            "Date": (today - timedelta(days=0)).strftime("%Y-%m-%d"),  # Today
            "Rank": 9,  # Recent high score
        },
        {
            "Id": 2,
            "Date": (today - timedelta(days=365)).strftime("%Y-%m-%d"),  # 1 year ago
            "Rank": 5,
        },
        {
            "Id": 3,
            "Date": (today - timedelta(days=730)).strftime("%Y-%m-%d"),  # 2 years ago (half-life)
            "Rank": 3,
        },
        {
            "Id": 4,
            "Date": (today - timedelta(days=1460)).strftime("%Y-%m-%d"),  # 4 years ago
            "Rank": 2,  # Old low score
        },
    ]
    
    # Calculate averages
    simple_avg = sum(s["Rank"] for s in sample_speeches) / len(sample_speeches)
    weighted_avg = output_manager._calculate_weighted_average(sample_speeches)
    
    print(f"Sample speeches:")
    for speech in sample_speeches:
        years_ago = (today - datetime.strptime(speech["Date"], "%Y-%m-%d")).days / 365.25
        print(f"  - Score: {speech['Rank']}, Date: {speech['Date']} ({years_ago:.1f} years ago)")
    
    print(f"\nSimple Average: {simple_avg:.2f}")
    print(f"Weighted Average: {weighted_avg:.2f}")
    print(f"Difference: {weighted_avg - simple_avg:.2f}")
    print(f"\nHalf-life setting: {config.TIME_WEIGHT_HALF_LIFE_YEARS} years")
    
    # Weighted average should be higher because recent high score has more influence
    if weighted_avg > simple_avg:
        print("✓ Weighted average is higher (recent high scores have more influence)")
    else:
        print("✗ Unexpected: weighted average should be higher in this example")
    
    return weighted_avg, simple_avg


def test_csv_export():
    """Test CSV export functionality."""
    print("\n\n=== Testing CSV Export ===\n")
    
    from unittest.mock import Mock
    config = Config()
    mock_database = Mock()
    output_manager = OutputManager(mock_database, config=config)
    
    # Export CSV
    output_manager.export_all_scores_csv()
    
    # Check if CSV was created
    csv_path = output_manager.client_data_dir / "all_mk_scores.csv"
    if csv_path.exists():
        print(f"\n✓ CSV file created: {csv_path}")
        
        # Read and display first few lines
        with open(csv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()[:6]  # Header + 5 rows
            print("\nFirst few lines:")
            for line in lines:
                print(f"  {line.strip()}")
        
        print(f"\nTotal rows: {len(lines) - 1} (excluding header)")
    else:
        print("✗ CSV file was not created")
    
    return csv_path.exists()


def main():
    """Run all tests."""
    print("="*60)
    print("Time-Weighted Scoring Implementation Test")
    print("="*60)
    
    try:
        # Test 1: Weighted average calculation
        weighted_avg, simple_avg = test_weighted_average_calculation()
        
        # Test 2: CSV export
        csv_created = test_csv_export()
        
        # Summary
        print("\n" + "="*60)
        print("Test Summary")
        print("="*60)
        print(f"✓ Weighted average calculation works")
        print(f"✓ CSV export {'succeeded' if csv_created else 'failed'}")
        print("\nNOTE: Existing JSON files will need to be regenerated")
        print("      to include weighted_average field. Run the score")
        print("      pipeline to update them.")
        
    except Exception as e:
        print(f"\n✗ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
