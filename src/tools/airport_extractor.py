#!/usr/bin/env python3
"""
Remove towered airports from a list of busy GA airports.
"""

import csv
import sys

def load_identifiers(filepath, id_column=None):
    """Load airport identifiers from a CSV file."""
    identifiers = set()
    with open(filepath, 'r', encoding='utf-8') as f:
        # Peek at first line to see if it's a header or just data
        first_line = f.readline().strip()
        f.seek(0)
        
        # Check if it looks like a simple one-column file (like your uploaded file)
        if ',' not in first_line and len(first_line) <= 5:
            # Simple one-identifier-per-line format
            for line in f:
                ident = line.strip()
                if ident:
                    identifiers.add(ident.upper())
        else:
            # CSV with headers
            reader = csv.DictReader(f)
            # Try common column names for airport ID
            for row in reader:
                if id_column and id_column in row:
                    identifiers.add(row[id_column].strip().upper())
                elif 'ARPT_ID' in row:
                    identifiers.add(row['ARPT_ID'].strip().upper())
                elif 'IDENT' in row:
                    identifiers.add(row['IDENT'].strip().upper())
                elif 'ID' in row:
                    identifiers.add(row['ID'].strip().upper())
                elif 'LOCATION_ID' in row:
                    identifiers.add(row['LOCATION_ID'].strip().upper())
                else:
                    # Fall back to first column
                    first_col = list(row.values())[0]
                    identifiers.add(first_col.strip().upper())
    return identifiers

def main():
    if len(sys.argv) < 3:
        print("Usage: python filter_untowered.py <ga_airports.csv> <towered_airports.csv> [output.csv]")
        print("  Removes towered airports from the GA airports list")
        sys.exit(1)
    
    ga_file = sys.argv[1]
    towered_file = sys.argv[2]
    output_file = sys.argv[3] if len(sys.argv) > 3 else 'untowered_ga_airports.csv'
    
    # Load both lists
    ga_airports = load_identifiers(ga_file)
    towered_airports = load_identifiers(towered_file)
    
    # Filter out towered
    untowered = ga_airports - towered_airports
    
    # Also preserve original order from GA file
    ordered_untowered = []
    with open(ga_file, 'r', encoding='utf-8') as f:
        for line in f:
            ident = line.strip().upper()
            if ident in untowered:
                ordered_untowered.append(ident)
    
    # Write output
    with open(output_file, 'w', encoding='utf-8') as f:
        for ident in ordered_untowered:
            f.write(f"{ident}\n")
    
    print(f"Input GA airports: {len(ga_airports)}")
    print(f"Towered airports: {len(towered_airports)}")
    print(f"Removed (towered): {len(ga_airports & towered_airports)}")
    print(f"Remaining (untowered): {len(ordered_untowered)}")
    print(f"Output written to: {output_file}")
    
    # Show which ones were removed
    removed = ga_airports & towered_airports
    if removed:
        print(f"\nRemoved airports: {', '.join(sorted(removed))}")

if __name__ == '__main__':
    main()
