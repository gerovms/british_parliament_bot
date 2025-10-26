PERSON = 'https://api.parliament.uk/historic-hansard/people'
ITEMS_PER_PAGE = 5
MAIN_URL = 'https://api.parliament.uk'
START_DATE = 1803
FINISH_DATE = 2005
BASE_NO_PESON_URL = 'https://api.parliament.uk/historic-hansard/sittings'
MONTHS = ['jan', 'feb',
          'mar', 'apr',
          'may', 'jun',
          'jul', 'aug',
          'sep', 'oct',
          'nov', 'dec']
FIRST_MONTH_DAY = 1
LAST_MONTH_DAY = 31
DATE_RANGE = 11
DELAY_TIME = 5
CELERY_QUEUE_TABLE_NAME = 'celery_user_queue'
PERSON_PATTERN = r'https?://api\.parliament\.uk/historic-hansard/people/'
