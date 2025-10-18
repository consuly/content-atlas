from lxml import etree
from typing import List, Dict, Any
import io


def process_xml(file_content: bytes) -> List[Dict[str, Any]]:
    """Process XML file and return list of dictionaries."""
    root = etree.parse(io.BytesIO(file_content)).getroot()

    records = []
    # Assume each child of root is a record
    for record_element in root:
        record = {}
        for child in record_element:
            record[child.tag] = child.text
        records.append(record)

    return records
