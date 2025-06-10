import csv
import asyncio
import os
from typing import List, Dict, Any, Optional
from .base_storage import BaseStorage


class CsvStorage(BaseStorage):
    """
    CSV Storage implementation for cases when fieldnames are predetermined.
    """

    def __init__(self, file_path: str, fieldnames: Optional[List[str]] = None, mode: str = "a"):
        """
        Initialize CSV storage.

        Args:
            file_path: Path to the CSV file.
            fieldnames: List of column names for the CSV.
            mode: File open mode, "a" for append and "w" for overwrite.
        """
        self.file_path = file_path
        self.fieldnames = fieldnames
        self.mode = mode

    async def save(self, data: Any) -> bool:
        """
        Save data to a CSV file.

        Args:
            data: Data to be saved, can be a dict or a list of dicts.

        Returns:
            bool: True if save was successful, False otherwise.
        """
        if not data:
            return False

        # Ensure data is in list form
        if not isinstance(data, dict) and not isinstance(data, list):
            raise TypeError("Data should only be dict or list type.")

        if isinstance(data, dict):
            data = [item for lst in data.values() for item in lst]

        # If no fieldnames specified and data is a dict, use keys of the first item
        if not self.fieldnames and isinstance(data[0], dict):
            self.fieldnames = list(data[0].keys())

        if not self.fieldnames:
            raise ValueError(
                "Fieldnames are empty. Set it in the CSVStorage instance creation stage.")

        loop = asyncio.get_event_loop()
        try:
            # Check if the file exists; if not, prepare to create it with a header
            file_exists = os.path.exists(self.file_path)

            # Determine write mode
            write_mode = self.mode
            if not file_exists:
                # Ensure directory exists
                os.makedirs(os.path.dirname(
                    os.path.abspath(self.file_path)), exist_ok=True)
                write_mode = "w"  # Always create a new file if it doesn't exist

            # Use executor to avoid blocking the event loop
            await loop.run_in_executor(None, self._write_to_csv, data, write_mode, not file_exists)
            return True
        except Exception as e:
            print(f"Error saving to CSV: {e}")
            return False

    def _write_to_csv(self, data, write_mode, write_header):
        """Perform CSV writing in a background thread."""
        with open(self.file_path, write_mode, newline='', encoding='utf-8') as csvfile:
            if not data:
                return

            first = data[0]
            # list of dicts
            if isinstance(first, dict):
                writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                if write_header:
                    writer.writeheader()
                writer.writerows(data)
            # list of sequences
            else:
                seq_writer = csv.writer(csvfile)
                # if you want a header row when data is sequences
                if write_header:
                    seq_writer.writerow(self.fieldnames)
                seq_writer.writerows(data)

    async def load(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Load data from a CSV file.

        Returns:
            List[Dict[str, Any]]: List of rows as dictionaries.
        """
        if not os.path.exists(self.file_path):
            return []

        loop = asyncio.get_event_loop()
        try:
            # Use executor to avoid blocking the event loop
            return await loop.run_in_executor(None, self._read_from_csv)
        except Exception as e:
            print(f"Error loading from CSV: {e}")
            return []

    def _read_from_csv(self):
        """Perform CSV reading in a background thread."""
        data = []
        with open(self.file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            self.fieldnames = reader.fieldnames
            for row in reader:
                data.append(row)
        return data
