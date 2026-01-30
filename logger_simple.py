# -*- coding: utf-8 -*-
"""
독립 실행형 타거래소 수집용 간단 로거.
logs/ 폴더에 일별 로그 파일을 기록합니다.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler

LOG_DIR = "logs"
LOG_FILE_PREFIX = "exchange_collector"
MAX_LOG_FILE_SIZE = 10 * 1024 * 1024  # 10MB
MAX_BACKUP_COUNT = 5


def _get_log_filepath():
    current_date = datetime.now().strftime("%Y%m%d")
    return os.path.join(LOG_DIR, f"{current_date}_{LOG_FILE_PREFIX}.log")


def setup_logger(name=None, level=logging.INFO):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(level)
    Path(LOG_DIR).mkdir(exist_ok=True)
    log_filepath = _get_log_filepath()
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)8s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = RotatingFileHandler(
        log_filepath,
        maxBytes=MAX_LOG_FILE_SIZE,
        backupCount=MAX_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    console.setFormatter(formatter)
    logger.addHandler(console)
    return logger


def get_logger(name=None):
    logger = logging.getLogger(name)
    if not logger.handlers:
        return setup_logger(name)
    return logger
