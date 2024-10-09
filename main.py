import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
import time
import schedule
from datetime import datetime
import re
from dotenv import load_dotenv
import telebot
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Appwrite configuration
client = Client()
client.set_endpoint(os.getenv('APPWRITE_ENDPOINT'))
client.set_project(os.getenv('APPWRITE_PROJECT_ID'))
client.set_key(os.getenv('APPWRITE_API_KEY'))

databases = Databases(client)

# Configuration
MAIN_URL = 'https://m.rbi.org.in//scripts/bs_viewcontent.aspx?Id=2009'
BASE_URL = 'https://rbidocs.rbi.org.in/rdocs/Content/DOCs/'
DATABASE_ID = os.getenv('APPWRITE_DATABASE_ID')
COLLECTION_ID = os.getenv('APPWRITE_COLLECTION_ID')
STATUS_DOCUMENT_ID = os.getenv('STATUS_DOCUMENT_ID')

# Telegram Bot configuration
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
bot = telebot.TeleBot(BOT_TOKEN)

def send_telegram_message(message):
    try:
        bot.send_message(CHAT_ID, message)
        logger.info(f"Telegram message sent: {message}")
    except Exception as e:
        logger.error(f"Error sending Telegram message: {str(e)}")

def get_update_date(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        update_text = soup.find(string=re.compile(r'updated as on'))
        if update_text:
            date_str = re.search(r'(\w+ \d+, \d{4})', update_text).group(1)
            return datetime.strptime(date_str, '%B %d, %Y')
    except Exception as e:
        logger.error(f"Error getting update date: {str(e)}")
    return None

def get_excel_links(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a', href=lambda href: href and href.endswith('.xlsx'))
        return [BASE_URL + link['href'].split('/')[-1] for link in links]
    except Exception as e:
        logger.error(f"Error getting Excel links: {str(e)}")
        return []

def download_excel(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return BytesIO(response.content)
    except Exception as e:
        logger.error(f"Error downloading Excel file: {str(e)}")
        return None

def process_excel(excel_file):
    try:
        df = pd.read_excel(excel_file)
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error processing Excel file: {str(e)}")
        return []

def insert_into_appwrite(data):
    new_records = 0
    for record in data:
        try:
            document_data = {
                'BANK': str(record.get('BANK', '')),
                'IFSC': str(record.get('IFSC', '')),
                'BRANCH': str(record.get('BRANCH', '')),
                'ADDRESS': str(record.get('ADDRESS', '')),
                'CITY1': str(record.get('CITY1', '')),
                'CITY2': str(record.get('CITY2', '')),
                'STATE': str(record.get('STATE', '')),
                'STD_CODE': str(record.get('STD CODE', '')),
                'PHONE': str(record.get('PHONE', ''))
            }
            
            existing_records = databases.list_documents(
                database_id=DATABASE_ID,
                collection_id=COLLECTION_ID,
                queries=[Query.equal('IFSC', document_data['IFSC'])]
            )
            
            if existing_records['total'] == 0:
                databases.create_document(
                    database_id=DATABASE_ID,
                    collection_id=COLLECTION_ID,
                    document_id='unique()',
                    data=document_data
                )
                new_records += 1
        except Exception as e:
            logger.error(f"Error processing record {record.get('IFSC', 'Unknown')}: {str(e)}")
    return new_records

def update_status(status):
    try:
        databases.update_document(
            database_id=DATABASE_ID,
            collection_id=COLLECTION_ID,
            document_id=STATUS_DOCUMENT_ID,
            data={'status': status, 'last_updated': datetime.now().isoformat()}
        )
        logger.info(f"Status updated: {status}")
    except Exception as e:
        logger.error(f"Error updating status: {str(e)}")

def get_last_update_date():
    try:
        status_doc = databases.get_document(
            database_id=DATABASE_ID,
            collection_id=COLLECTION_ID,
            document_id=STATUS_DOCUMENT_ID
        )
        return datetime.fromisoformat(status_doc['last_update_date'])
    except Exception as e:
        logger.error(f"Error getting last update date: {str(e)}")
        return None

def set_last_update_date(date):
    try:
        databases.update_document(
            database_id=DATABASE_ID,
            collection_id=COLLECTION_ID,
            document_id=STATUS_DOCUMENT_ID,
            data={'last_update_date': date.isoformat()}
        )
        logger.info(f"Last update date set: {date}")
    except Exception as e:
        logger.error(f"Error setting last update date: {str(e)}")

def main():
    update_status('running')
    send_telegram_message("Script started running.")
    
    current_update_date = get_update_date(MAIN_URL)
    last_update_date = get_last_update_date()
    
    if current_update_date and (not last_update_date or current_update_date > last_update_date):
        send_telegram_message(f"New update found: {current_update_date}")
        set_last_update_date(current_update_date)
        
        excel_links = get_excel_links(MAIN_URL)
        
        for link in excel_links:
            send_telegram_message(f"Processing file: {link}")
            excel_file = download_excel(link)
            
            if excel_file:
                data = process_excel(excel_file)
                
                new_records = insert_into_appwrite(data)
                
                send_telegram_message(f"Completed processing {link}. New records added: {new_records}")
            else:
                send_telegram_message(f"Failed to download file: {link}")
            
            time.sleep(2)
        
        send_telegram_message("All files processed.")
    else:
        send_telegram_message(f"No new updates as of {datetime.now()}")
    
    update_status('idle')
    send_telegram_message("Script finished running.")

def run_scheduled_task():
    send_telegram_message(f"Running scheduled task at {datetime.now()}")
    main()

# Schedule the task to run monthly
schedule.every(30).days.do(run_scheduled_task)

if __name__ == "__main__":
    logger.info("Starting the script...")
    main()  # Run once immediately
    while True:
        schedule.run_pending()
        time.sleep(1)