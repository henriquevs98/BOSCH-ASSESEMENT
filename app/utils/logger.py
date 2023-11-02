import logging

def logger():
    # Configure logging
    logging.basicConfig(level=logging.DEBUG)
    # Create a StreamHandler to print log messages to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.getLogger().getEffectiveLevel())
    # Create a Formatter to format log messages
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # Add the Formatter to the StreamHandler
    console_handler.setFormatter(formatter)
    # Add the StreamHandler to the logger
    logging.getLogger().handlers.pop()
    logging.getLogger().addHandler(console_handler)
