import logging
from daemon import runner
import time
from datetime import date, datetime, timedelta
import requests
from bs4 import BeautifulSoup
from sqlite import SQLite, SQLiteTable
from IPython.core.debugger import Tracer

LOG_FILE = '/Users/deedy/Dev/Python/times-of-india-scraper/toidaemon.log'
PID_FILE_PATH = '/Users/deedy/Dev/Python/times-of-india-scraper/toi.pid'
DB_PATH = '/Users/deedy/Dropbox/toi_2000_2015/toi.db'

class ToiScraper():
  TABLE_NAME = 'articles'
  TABLE_SCHEMA = [(u'ds', u'text(10)'), (u'title', u'text'), (u'url', u'text')]
  # Manually observed minimum date on TOI
  INIT_DATE = (2000, 1, 18)
  MIN_ENTRIES = 600
  MAX_SLEEP = 3600

  def __init__(self):
    # ====  Required vars ===== #
    self.stdin_path = '/dev/null'
    self.stdout_path = '/dev/null'
    self.stderr_path = '/dev/null'
    # self.pidfile_path =  '/var/run/toidaemon/toidaemon.pid'
    self.pidfile_path = PID_FILE_PATH
    self.pidfile_timeout = 5
    # ========================= #

    self.db_name = DB_PATH
    self.db = SQLite(self.db_name)
    self.table = self.db.get(ToiScraper.TABLE_NAME)
    logger.info("Initializing...")
    if not self.table:
      logger.info("No table found with name {0}. Creating it.".format(ToiScraper.TABLE_NAME))
      self.table = self.db.create(ToiScraper.TABLE_NAME, ToiScraper.TABLE_SCHEMA)
    else:
      if not self.table.get_info() == ToiScraper.TABLE_SCHEMA:
        error_str = "Table {0} exists but with incorrect schema".format(ToiScraper.TABLE_NAME)
        logger.error(error_str)
        raise Exception(error_str)
    self.iter_date = self._get_init_date_full()

  # Get the last date in the database with at least 600 entries in it (enough to tell that it's full)
  def _get_init_date_full(self):
    logger.info("Retrieving last retrieved date from database with at least {0} in it".format(ToiScraper.MIN_ENTRIES))
    first_date = self.db.execute("""
        SELECT
          a.ds,
          a.count
        FROM (
          SELECT
            ds,
            count(1) AS count
          FROM {0}
          GROUP BY ds
          ORDER BY DATE(ds) DESC
        ) a
        WHERE a.count > {1}
        LIMIT 1;
      """.format(ToiScraper.TABLE_NAME, ToiScraper.MIN_ENTRIES),
      get=True
    )
    if len(first_date) == 0:
      logger.info("No last date with given minimum entries found in DB, starting from beginning.")
      return ToiScraper.INIT_DATE
    logger.info("Last date with entries {0} found. {1} entries total.".format(first_date[0][0], first_date[0][1]))
    return self.get_next_day(*tuple(map(int, first_date[0][0].split('-'))))


  # Get the last date in the database with entries in it
  def _get_init_date(self):
    logger.info("Retrieving last retrieved date from database")
    first_date = self.db.execute('SELECT ds FROM {0} ORDER BY DATE(ds) DESC LIMIT 1'.format(ToiScraper.TABLE_NAME), get=True)
    if len(first_date) == 0:
      logger.info("No last date found in DB, starting from beginning.")
      return ToiScraper.INIT_DATE
    logger.info("Last date {0} found.".format(first_date[0]['ds']))
    return self.get_next_day(*tuple(map(int, first_date[0]['ds'].split('-'))))

  def get_last_valid_date(self):
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

  # Check if the date is strictly before today in IST
  def is_valid_date(self, year, month, day):
    try:
      datetime(year, month, day)
    except ValueError:
      return False
    cur_time = datetime(year, month, day)
    india_time = self.get_last_valid_date()
    return cur_time + timedelta(days = 1) < india_time and cur_time >= datetime(*ToiScraper.INIT_DATE)

  def compute_url_for_day(self, year, month, day):
    if not self.is_valid_date(year, month, day):
      return None
    # Day count used in TOI URL (1st October, 2015 == 42278)
    day_count = (date(year, month, day) - date(1900, 1, 1)).days + 2
    return "http://timesofindia.indiatimes.com/{year}/{month}/{day}/archivelist/year-{year},month-{month},starttime-{daycount}.cms".format(
        year = year,
        month = month,
        day = day,
        daycount = day_count
      )

  def get_next_day(self, year, month, day):
    next_day = datetime(year, month, day) + timedelta(days = 1)
    return (next_day.year, next_day.month, next_day.day)

  def _retrieve_url_contents(self, url, datetuple):
    logger.debug("Request sent to url {0}".format(url))
    req = requests.get(url)
    logger.debug("Response retrieved, parsing")
    soup = BeautifulSoup(req.text, 'lxml')
    # Signature of the element we're interested in. We rely on the TOI webpage
    # not to change
    divs = soup.find_all('div', style='font-family:arial ;font-size:12;font-weight:bold; color: #006699')
    if not len(divs) == 1:
      error_str = "Found {0} divs matching signature. Aborting.".format(len(divs))
      self.error(error_str)
      raise Exception(error_str)
    articles = divs[0].find_all('a')
    logger.debug("Found {0} hyperlinks in the archive.".format(len(articles)))
    articles = [a for a in articles if len(a.text) > 0]
    res = []
    titles = set({})
    for art in articles:
      corr_url = self.validate_url(art['href'])
      if corr_url:
        if art.text in titles:
          continue
        titles.add(art.text)
        res.append([
          datetime(*datetuple).strftime('%Y-%m-%d'),
          art.text,
          corr_url,
        ])
    logger.debug("Finished parsing, {0} rows remain".format(len(res)))
    return res

  # TOI specific article URL validation and correction
  def validate_url(self, url):
    URL_CORRECT = 'http://timesofindia.indiatimes.com/'
    URL_STANDARD = 'http://'
    URL_INSIDE = '.indiatimes.com/'
    if not url.startswith(URL_STANDARD) or not URL_INSIDE in url:
      if not url.endswith('.cms') or 'http' in url or ' ' in url:
        return None
      else:
        return URL_CORRECT + url
    return url

  def dedup_insert(self, data, ds):
    date_str = '-'.join(map(str, ds))
    logger.debug("Asking to insert {0} articles in {1}".format(len(data), date_str))
    rows = self.table.where({'ds': date_str})
    logger.debug("Already {0} rows exist in {1}".format(len(rows), date_str))
    titles = set({})
    res = []
    for a in rows:
      if not a['title'] in titles:
        titles.add(a['title'])
        res.append((a['ds'], a['title'], a['url']))
    for r in data:
      if not r[1] in titles:
        titles.add(r[1])
        res.append(r)
    logger.debug("{0} rows left after deduplicating".format(len(res)))
    if len(rows) > 0:
      logger.info("Deleting {0} rows from {1}".format(len(rows), date_str))
      self.table.del_where({'ds': date_str})
    if len(res) > 0:
      logger.info("Inserting {0} rows from {1}".format(len(res), date_str))
      self.table.insert(res)

  def get_articles_for_day(self, year, month, day):
    logger.debug("Getting articles for the day")
    url = self.compute_url_for_day(year, month, day)
    if not url:
      return 0
    data = self._retrieve_url_contents(url, (year, month, day))
    self.dedup_insert(data, (year, month, day))
    return len(data)

  def run(self):
    while True:
      while not self.is_valid_date(*self.iter_date):
        next_date = datetime(*self.iter_date) + timedelta(days=1)
        sec_to_next_date = (next_date - self.get_last_valid_date()).seconds
        logger.info("Reached the end, {0} seconds until {1}".format(sec_to_next_date, datetime(*self.iter_date).strftime('%Y-%m-%d')))
        if sec_to_next_date <= ToiScraper.MAX_SLEEP:
          time.sleep(sec_to_next_date)
        else:
          logger.info('Seconds till next day {0} greater than {1}, so only sleeping for {1}'.format(sec_to_next_date, ToiScraper.MAX_SLEEP))
          time.sleep(ToiScraper.MAX_SLEEP)
        logger.info('Woken up, getting init date again')
        self.iter_date = self._get_init_date_full()
        logger.info('New date set to {0}'.format(self.iter_date))
      logger.info("Retrieving articles for date {0}".format(self.iter_date))
      num_rows = self.get_articles_for_day(*self.iter_date)
      logger.info("Retrieved {0} rows from TOI".format(num_rows))
      if num_rows == 0:
        logger.debug("Sleeping for 10 seconds, no rows retrieved")
        time.sleep(10)
      else:
        self.iter_date = self.get_next_day(*self.iter_date)
        logger.debug("Iterated to next day - {0}".format(datetime(*self.iter_date)))



# Set up Logging
logger = logging.getLogger("DaemonLog")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler = logging.FileHandler(LOG_FILE)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Start Daemon with logging
app = ToiScraper()
daemon_runner = runner.DaemonRunner(app)
daemon_runner.daemon_context.files_preserve=[handler.stream]
daemon_runner.do_action()





