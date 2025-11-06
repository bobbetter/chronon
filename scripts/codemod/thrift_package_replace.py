#!/usr/bin/env python3

import argparse
import os
import sys
from pathlib import Path


def replace_in_file(input_file: Path, output_file: Path) -> None:
    """Replace package names in a single file."""
    os.makedirs(output_file.parent, exist_ok=True)

    with open(input_file, 'r') as f:
        content = f.read()

    modified_content = content.replace('org.apache.thrift', 'ai.chronon.api.thrift')

    with open(output_file, 'w') as f:
        f.write(modified_content)


def process_directory(input_dir: Path, output_dir: Path, verbose: bool = False) -> None:
    """Process all Java files in the input directory and its subdirectories."""
    if verbose:
        print(f"Scanning directory: {input_dir}")
        print(f"Output directory: {output_dir}")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Find all Java files
    for java_file in input_dir.rglob('*.java'):
        if verbose:
            print(f"Processing file: {java_file}")

        # Calculate relative path to maintain directory structure
        rel_path = java_file.relative_to(input_dir)
        output_file = output_dir / rel_path

        if verbose:
            print(f"Writing to: {output_file}")

        replace_in_file(java_file, output_file)


def main():
    parser = argparse.ArgumentParser(description='Replace package names in Java files')
    parser.add_argument('input_dir', type=str, help='Input directory containing Java files')
    parser.add_argument('output_dir', type=str, help='Output directory for modified files')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')

    args = parser.parse_args()

    input_path = Path(args.input_dir)
    output_path = Path(args.output_dir)

    if not input_path.exists():
        print(f"Error: Input directory '{input_path}' does not exist", file=sys.stderr)
        sys.exit(1)

    if not input_path.is_dir():
        print(f"Error: '{input_path}' is not a directory", file=sys.stderr)
        sys.exit(1)

    try:
        process_directory(input_path, output_path, args.verbose)
        if args.verbose:
            print("Replacement complete!")
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()