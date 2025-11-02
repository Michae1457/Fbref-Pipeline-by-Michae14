import logging
import os
from datetime import datetime
from pathlib import Path

# Global logger instance
_logger = None

def get_logger(name: str = "fbref_pipeline") -> logging.Logger:
    """
    Get or create a universal logger instance with file path tracking.
    
    Args:
        name: Logger name (default: fbref_pipeline)
    
    Returns:
        Configured logger instance
    """
    global _logger
    
    if _logger is None:
        _logger = logging.getLogger(name)
        _logger.setLevel(logging.INFO)
        
        # Avoid duplicate handlers
        if not _logger.handlers:
            # Create formatter with pathname
            # %(pathname)s gives full path, we'll extract relative path in the format
            formatter = logging.Formatter(
                '%(asctime)s - %(relative_path)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            _logger.addHandler(console_handler)
            
            # File handler
            log_dir = Path("log")
            log_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = f"fbref_pipeline_{timestamp}.log"
            log_path = log_dir / log_file
            
            file_handler = logging.FileHandler(log_path)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            _logger.addHandler(file_handler)
            
            # Add filter to inject relative path
            _logger.addFilter(RelativePathFilter())
            
            _logger.info(f"Universal logger initialized")
    
    return _logger


class RelativePathFilter(logging.Filter):
    """Filter to add relative file path to log records."""
    
    def filter(self, record):
        """
        Add relative_path attribute to the log record.
        
        Args:
            record: LogRecord instance
            
        Returns:
            True to allow the record to be logged
        """
        try:
            # Get the absolute path of the file
            abs_path = Path(record.pathname).resolve()
            
            # Get the project root (where pipeline/ directory is)
            # Go up from the current file until we find the pipeline directory
            current = abs_path
            project_root = None
            
            # Try to find project root by looking for 'pipeline' directory
            for parent in abs_path.parents:
                if (parent / 'pipeline').exists():
                    project_root = parent
                    break
            
            if project_root:
                # Get relative path from project root
                try:
                    rel_path = abs_path.relative_to(project_root)
                    record.relative_path = str(rel_path)
                except ValueError:
                    # If relative_to fails, use the filename
                    record.relative_path = record.filename
            else:
                # Fallback to just filename if we can't find project root
                record.relative_path = record.filename
                
        except Exception:
            # If anything goes wrong, just use the filename
            record.relative_path = record.filename
        
        return True

def setup_logger(name: str, log_file: str = None, level: int = logging.INFO) -> logging.Logger:
    """
    Legacy function for backward compatibility.
    Now returns the universal logger.
    
    Args:
        name: Logger name (ignored, uses universal logger)
        log_file: Log file name (ignored)
        level: Logging level (ignored)
    
    Returns:
        Universal logger instance
    """
    return get_logger()
