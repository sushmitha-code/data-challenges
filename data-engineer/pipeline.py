import logging
import os
import sqlite3
import xml.etree.ElementTree as ET
from typing import Dict
from typing import List

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_files_from_dir(dir: str) -> List[str]:
    files_xml = []
    try:
        logger.info(f"Reading files from `{dir}`")
        if not os.path.exists(dir):
            raise FileNotFoundError(f"Directory '{dir}' does not exist.")

        for filename in os.listdir(dir):
            if filename.endswith(".xml"):
                logger.info(f"Reading xml data from `{os.path.join(dir, filename)}`")
                with open(os.path.join(dir, filename), 'r') as file:
                    files_xml.append(file.read())

        if not files_xml:
            raise FileNotFoundError(f"No XML files found in the directory `{dir}`.")
    except (FileNotFoundError, Exception) as e:
        logger.error(f"An error occurred while reading files from directory `{dir}`: {e}")
        raise e
    return files_xml


def parse_xml(files: List[str]) -> pd.DataFrame:
    logger.info("Parsing the data from XML files")
    data = []
    try:
        for xml_file in files:
            try:
                root = ET.fromstring(xml_file)
            except ET.ParseError as parse_error:
                logger.error(f"XML parsing error in xml_file: {parse_error}")
                continue  # Skip this xml_file and move to the next

            for event in root.iter('event'):
                try:
                    order_id = event.find('order_id').text
                    date_time = event.find('date_time').text
                    status = event.find('status').text
                    cost = float(event.find('cost').text)
                    technician = event.find('repair_details/technician').text
                    parts = [(part.attrib['name'], int(part.attrib['quantity'])) for part in
                             event.findall('repair_details/repair_parts/part')]

                    if not order_id or not date_time or not status or cost is None or not technician:
                        logger.error("XML structure is invalid")
                        raise ValueError("Missing required fields in event.")

                    data.append({
                        'order_id': order_id,
                        'date_time': date_time,
                        'status': status,
                        'cost': cost,
                        'technician': technician,
                        'parts': parts
                    })
                except (AttributeError, ValueError) as e:
                    print(f"Error processing event: {e}")
                    continue  # Skip this event and move to the next

        if not data:
            logger.error("No valid event data found in XML files.")
            raise ValueError("No valid event data found in XML files.")
    except Exception as e:
        logger.error(f"An error occurred while parsing XML files: {e}")
        raise e
    logger.info("XML Data is parsed successfully")
    return pd.DataFrame(data)


def window_by_datetime(data: pd.DataFrame, window: str) -> Dict[str, pd.DataFrame]:
    logger.info(f"Windowing data by window: `{window}`")
    try:
        data['date_time'] = pd.to_datetime(data['date_time'])
        data.set_index('date_time', inplace=True)
        grouped = data.groupby('order_id').resample(window).last().reset_index(level=0, drop=True).reset_index()
        w_d = {}
        for name, group in grouped.groupby(pd.Grouper(key='date_time', freq=window)):
            w_d[name] = group
        if not w_d:
            raise ValueError("No data found for the specified window.")
    except (ValueError, Exception) as e:
        logger.error(f"Unable to window the data {e}")
        raise e
    return w_d


class RO:
    def __init__(self, order_id: str, date_time: str, status: str, cost: float, technician: str, parts: List[tuple]):
        self.order_id = order_id
        self.date_time = date_time
        self.status = status
        self.cost = cost
        self.technician = technician
        self.parts = parts

    def __repr__(self):
        return f"RO(order_id={self.order_id}, date_time={self.date_time}, status={self.status}, cost={self.cost}, technician={self.technician}, parts={self.parts})"


def process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RO]:
    logger.info("Translating data into RO items")
    records = []
    try:
        for window, df in data.items():
            for _, row in df.iterrows():
                ro = RO(
                    order_id=row['order_id'],
                    date_time=row['date_time'],
                    status=row['status'],
                    cost=row['cost'],
                    technician=row['technician'],
                    parts=row['parts']
                )
                records.append(ro)
        if not records:
            raise ValueError(f"No valid repair orders found for window `{window}`.")
    except (ValueError, Exception) as e:
        logger.error(f"An error occurred while processing to RO: {e}")
        raise e
    logger.info(f"Data translated into RO successfully")
    return records


def pipeline(dir: str, window: str):
    try:
        logger.info(f"Reading XML files from directory: `{dir}`")
        xml_files = read_files_from_dir(dir)

        if not xml_files:
            raise FileNotFoundError("No XML files found or directory does not exist.")

        logger.info(f"Parsing XML files from directory: `{dir}`")
        parsed_data = parse_xml(xml_files)

        if parsed_data.empty:
            raise ValueError("Parsed data is empty. No valid events found in XML files.")

        logger.info(f"Windowing data by window: `{window}`")
        windowed_data = window_by_datetime(parsed_data, window)

        if not windowed_data:
            raise ValueError("No data found for the specified window.")

        logger.info("Processing windowed data into structured RO format...")
        structured_ro = process_to_RO(windowed_data)

        if not structured_ro:
            raise ValueError("No valid repair orders found after processing.")

        logger.info("Storing data into SQLite database")
        conn = sqlite3.connect('repair_orders.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS repair_orders
                     (order_id TEXT, date_time TEXT, status TEXT, cost REAL, technician TEXT, parts TEXT)''')
        for ro in structured_ro:
            c.execute("INSERT INTO repair_orders VALUES (?, ?, ?, ?, ?, ?)",
                      (ro.order_id, ro.date_time, ro.status, ro.cost, ro.technician, str(ro.parts)))
        conn.commit()
        conn.close()
        logger.info("XML Data RO Pipeline completed successfully.")
    except FileNotFoundError as fnf_error:
        logger.error(f"File not found: {fnf_error}")
    except ValueError as e:
        logger.error(f"Value error: {e}")
    except sqlite3.DatabaseError as db_error:
        logger.error(f"Database error: {db_error}")
    except Exception as e:
        logger.error(f"An error occurred in the pipeline: {e}")
