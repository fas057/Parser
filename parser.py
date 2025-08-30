import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import logging
import boto3
from botocore.exceptions import NoCredentialsError, ClientError, NoRegionError
import io
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# КОНФИГУРАЦИЯ SELECTEL CLOUD STORAGE
# =============================================================================
class SelectelConfig:
    """Конфигурация для Selectel Cloud Storage."""
    
    # Данные из переменных окружения
    ACCESS_KEY = os.getenv('SELECTEL_ACCESS_KEY')
    SECRET_KEY = os.getenv('SELECTEL_SECRET_KEY')
    BUCKET_NAME = os.getenv('SELECTEL_BUCKET_NAME', 'quotes-parser-bucket')
    ENDPOINT_URL = os.getenv('SELECTEL_ENDPOINT', 'https://s3.storage.selcloud.ru')
    
    # Настройки парсера
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    }
    
    MIN_DELAY = 1.0
    MAX_DELAY = 2.0
    MAX_RETRIES = 3
    TIMEOUT = 10
    BASE_URL = "http://quotes.toscrape.com/"

# =============================================================================
# SELECTEL CLOUD STORAGE MANAGER
# =============================================================================
class SelectelStorageManager:
    """Управление Selectel Cloud Storage."""
    
    def __init__(self, config: SelectelConfig):
        self.config = config
        self.s3_client = None
        self.is_connected = False
        self.connect()
    
    def connect(self):
        """Подключается к Selectel Cloud Storage."""
        try:
            logger.info(f"Подключение к Selectel: {self.config.ENDPOINT_URL}")
            
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.config.ENDPOINT_URL,
                aws_access_key_id=self.config.ACCESS_KEY,
                aws_secret_access_key=self.config.SECRET_KEY,
                region_name='ru-7'  # Важно указать регион для Selectel
            )
            
            # Простая проверка подключения - список bucket'ов
            self.s3_client.list_buckets()
            self.is_connected = True
            logger.info("✅ Успешное подключение к Selectel Cloud Storage")
            
        except NoCredentialsError:
            logger.error("❌ Не найдены учетные данные для Selectel")
            raise
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"❌ Ошибка подключения к Selectel: {error_code} - {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Неизвестная ошибка подключения: {e}")
            raise
    
    def create_bucket(self):
        """Создает bucket в Selectel."""
        try:
            logger.info(f"Попытка создания bucket: {self.config.BUCKET_NAME}")
            
            # Для Selectel нужно указывать LocationConstraint
            self.s3_client.create_bucket(
                Bucket=self.config.BUCKET_NAME,
                CreateBucketConfiguration={
                    'LocationConstraint': 'ru-7'
                }
            )
            
            logger.info(f"✅ Bucket {self.config.BUCKET_NAME} успешно создан")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'BucketAlreadyExists':
                logger.info(f"ℹ️ Bucket {self.config.BUCKET_NAME} уже существует")
                return True
            elif error_code == 'BucketAlreadyOwnedByYou':
                logger.info(f"ℹ️ Bucket {self.config.BUCKET_NAME} уже создан вами")
                return True
            else:
                logger.error(f"❌ Ошибка создания bucket: {error_code} - {e}")
                return False
        except Exception as e:
            logger.error(f"❌ Неизвестная ошибка при создании bucket: {e}")
            return False
    
    def check_bucket_exists(self):
        """Проверяет существование bucket."""
        try:
            self.s3_client.head_bucket(Bucket=self.config.BUCKET_NAME)
            logger.info(f"✅ Bucket {self.config.BUCKET_NAME} существует")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info(f"ℹ️ Bucket {self.config.BUCKET_NAME} не существует")
                return False
            else:
                logger.error(f"❌ Ошибка проверки bucket: {error_code} - {e}")
                return False
    
    def ensure_bucket(self):
        """Гарантирует, что bucket существует."""
        if self.check_bucket_exists():
            return True
        return self.create_bucket()
    
    def upload_dataframe(self, df, object_name, format='csv'):
        """Загружает DataFrame в хранилище."""
        if not self.is_connected:
            logger.error("❌ Нет подключения к Selectel")
            return False
        
        if not self.ensure_bucket():
            logger.error("❌ Не удалось обеспечить существование bucket")
            return False
        
        try:
            if format == 'csv':
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, encoding='utf-8')
                self.s3_client.put_object(
                    Bucket=self.config.BUCKET_NAME,
                    Key=object_name,
                    Body=csv_buffer.getvalue().encode('utf-8'),
                    ContentType='text/csv; charset=utf-8'
                )
            
            logger.info(f"✅ Файл {object_name} успешно загружен")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"❌ Ошибка загрузки {object_name}: {error_code} - {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Неизвестная ошибка при загрузке {object_name}: {e}")
            return False
    
    def list_files(self):
        """Списывает файлы в bucket."""
        if not self.is_connected:
            return []
        
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.config.BUCKET_NAME)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                logger.error(f"❌ Bucket {self.config.BUCKET_NAME} не существует")
            return []
        except Exception as e:
            logger.error(f"❌ Ошибка получения списка файлов: {e}")
            return []

# =============================================================================
# ПАРСЕР ЦИТАТ
# =============================================================================
class QuotesScraper:
    """Парсер для сайта quotes.toscrape.com."""
    
    def __init__(self, config: SelectelConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(config.HEADERS)
    
    def get_random_delay(self):
        return random.uniform(self.config.MIN_DELAY, self.config.MAX_DELAY)
    
    def make_request(self, url: str):
        for attempt in range(self.config.MAX_RETRIES):
            try:
                logger.info(f"🌐 Запрос: {url} (попытка {attempt + 1})")
                response = self.session.get(url, timeout=self.config.TIMEOUT)
                
                if response.status_code == 200:
                    time.sleep(self.get_random_delay())
                    return BeautifulSoup(response.content, 'html.parser')
                else:
                    logger.warning(f"⚠️ Статус код {response.status_code}")
                    time.sleep(self.get_random_delay() * 2)
                    
            except Exception as e:
                logger.error(f"❌ Ошибка запроса: {e}")
                time.sleep(self.get_random_delay() * 2)
        
        return None
    
    def parse_quote(self, quote_element):
        try:
            text = quote_element.find('span', class_='text').text.strip()
            author = quote_element.find('small', class_='author').text.strip()
            tags = [tag.text for tag in quote_element.find_all('a', class_='tag')]
            
            return {
                'quote': text,
                'author': author,
                'tags': ', '.join(tags),
                'tags_list': tags,
                'quote_length': len(text)
            }
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга цитаты: {e}")
            return None
    
    def scrape_all_quotes(self):
        logger.info("🚀 Начало парсинга цитат")
        all_quotes = []
        page_num = 1
        
        while True:
            if page_num == 1:
                url = self.config.BASE_URL
            else:
                url = f"{self.config.BASE_URL}page/{page_num}/"
            
            soup = self.make_request(url)
            if not soup:
                break
            
            quotes = soup.find_all('div', class_='quote')
            page_quotes = []
            
            for quote_element in quotes:
                quote_data = self.parse_quote(quote_element)
                if quote_data:
                    page_quotes.append(quote_data)
            
            all_quotes.extend(page_quotes)
            logger.info(f"📄 Страница {page_num}: {len(page_quotes)} цитат")
            
            if not soup.find('li', class_='next'):
                break
            
            page_num += 1
            time.sleep(self.get_random_delay())
        
        logger.info(f"✅ Парсинг завершен! Всего: {len(all_quotes)} цитат")
        return all_quotes

# =============================================================================
# ФИЛЬТРАЦИЯ И АГРЕГАЦИЯ ДАННЫХ
# =============================================================================
def filter_and_analyze_data(quotes_data):
    """Фильтрует и анализирует данные."""
    df = pd.DataFrame(quotes_data)
    
    logger.info("🔍 Фильтрация и агрегация данных...")
    
    # 1. ФИЛЬТРОВАННЫЕ ДАННЫЕ
    # Топ авторы (более 1 цитаты)
    author_counts = df['author'].value_counts()
    top_authors = author_counts[author_counts >= 1].index
    filtered_by_author = df[df['author'].isin(top_authors)]
    
    # Цитаты определенной длины
    filtered_by_length = df[df['quote_length'] > 20]
    
    # Цитаты с тегами
    filtered_with_tags = df[df['tags'].str.len() > 0]
    
    # 2. АГРЕГИРОВАННЫЕ ДАННЫЕ
    # Статистика по авторам
    author_stats = df.groupby('author').agg({
        'quote': 'count',
        'quote_length': 'mean'
    }).rename(columns={'quote': 'quotes_count', 'quote_length': 'avg_length'})
    author_stats = author_stats.round(2)
    
    # Статистика по тегам
    all_tags = []
    for tags in df['tags_list']:
        all_tags.extend(tags)
    
    tag_stats = pd.Series(all_tags).value_counts().reset_index()
    tag_stats.columns = ['tag', 'count']
    
    # 3. ПОДГОТОВКА ДАТАСЕТОВ ДЛЯ ЗАГРУЗКИ
    datasets = {
        # Полные данные
        'all_quotes.csv': df[['quote', 'author', 'tags', 'quote_length']],
        
        # Фильтрованные данные
        'filtered/top_authors.csv': filtered_by_author[['quote', 'author', 'tags']],
        'filtered/long_quotes.csv': filtered_by_length[['quote', 'author', 'tags']],
        'filtered/with_tags.csv': filtered_with_tags[['quote', 'author', 'tags']],
        
        # Агрегированные данные
        'analytics/author_statistics.csv': author_stats.reset_index(),
        'analytics/tag_statistics.csv': tag_stats,
        'analytics/top_10_tags.csv': tag_stats.head(10),
    }
    
    return datasets, df

# =============================================================================
# ОСНОВНАЯ ФУНКЦИЯ
# =============================================================================
def main():
    """Основная функция."""
    try:
        config = SelectelConfig()
        
        print("=" * 60)
        print("ПАРСЕР ЦИТАТ С СОХРАНЕНИЕМ В SELECTEL CLOUD STORAGE")
        print("=" * 60)
        
        # Проверяем наличие учетных данных
        if not config.ACCESS_KEY or not config.SECRET_KEY:
            print("❌ ERROR: Не найдены учетные данные Selectel")
            print("Создайте файл .env со следующими переменными:")
            print("SELECTEL_ACCESS_KEY=ваш_access_key")
            print("SELECTEL_SECRET_KEY=ваш_secret_key")
            print("SELECTEL_BUCKET_NAME=quotes-parser-bucket (опционально)")
            return
        
        print(f"🔑 Access Key: {config.ACCESS_KEY[:10]}...")
        print(f"📦 Bucket: {config.BUCKET_NAME}")
        print("=" * 60)
        
        # Инициализируем менеджер хранилища
        storage_manager = SelectelStorageManager(config)
        
        if not storage_manager.is_connected:
            print("❌ Не удалось подключиться к Selectel")
            return
        
        # Запускаем парсер
        scraper = QuotesScraper(config)
        quotes_data = scraper.scrape_all_quotes()
        
        if not quotes_data:
            logger.error("❌ Не удалось собрать данные")
            return
        
        # Фильтруем и агрегируем данные
        datasets, original_df = filter_and_analyze_data(quotes_data)
        
        # Сохраняем локально для проверки
        original_df.to_csv('all_quotes_local.csv', index=False)
        logger.info("💾 Локальная копия сохранена: all_quotes_local.csv")
        
        # Загружаем данные в Selectel Cloud Storage
        logger.info("☁️ Загрузка данных в Selectel Cloud Storage...")
        
        uploaded_files = []
        for filename, dataset in datasets.items():
            logger.info(f"⬆️ Попытка загрузки: {filename}")
            if storage_manager.upload_dataframe(dataset, filename, format='csv'):
                uploaded_files.append(filename)
                print(f"✅ Успешно: {filename}")
            else:
                print(f"❌ Ошибка: {filename}")
        
        # Выводим статистику
        print("\n" + "=" * 50)
        print("СТАТИСТИКА ПАРСИНГА")
        print("=" * 50)
        print(f"📊 Всего собрано цитат: {len(original_df)}")
        print(f"👤 Уникальных авторов: {original_df['author'].nunique()}")
        
        # Подсчет тегов
        all_tags = []
        for tags in original_df['tags_list']:
            all_tags.extend(tags)
        print(f"🏷️ Уникальных тегов: {len(set(all_tags))}")
        
        print(f"\n📤 Загружено файлов в Selectel: {len(uploaded_files)}")
        for file in uploaded_files:
            print(f"  ✓ {file}")
        
        if uploaded_files:
            print("\n" + "=" * 50)
            print("✅ ВСЕ ЗАДАЧИ ВЫПОЛНЕНЫ УСПЕШНО!")
            print("=" * 50)
        else:
            print("\n" + "=" * 50)
            print("⚠️  Данные собраны, но не загружены в Selectel")
            print("Проверьте права доступа и настройки bucket")
            print("=" * 50)
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        print(f"\nОшибка: {e}")

if __name__ == "__main__":
    main()