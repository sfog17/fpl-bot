import logging

# Create the Logger
loggers = logging.getLogger(__name__)
loggers.setLevel(logging.DEBUG)

# Create the Handler for logging data to the console and a file
file_handler = logging.FileHandler(filename='fpl.log')
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a Formatter for formatting the log messages
logger_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add the Formatter to the Handlers
file_handler.setFormatter(logger_formatter)
console_handler.setFormatter(logger_formatter)

# Add the Handler to the Logger
loggers.addHandler(file_handler)
loggers.addHandler(console_handler)
loggers.info('Completed configuring logger()!')
