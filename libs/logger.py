import logging
from colorama import Fore, Style, init

# Inicializar o colorama para permitir cores em terminais
init(autoreset=True)


class Logger:
    """_summary_

    Classe responsável por emitir mensagens personalizadas no log durante o processo
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def sucess(message: str) -> None:
        """_summary_

        Args:
            message (str): mensagem de sucesso a ser emitida
        """
        logging.basicConfig(
            format="%(asctime)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            level=logging.INFO,
        )

        colored_message = f"{Fore.GREEN}[SUCESS]{Style.NORMAL} - {message}"
        logging.info(colored_message)

    @staticmethod
    def info(message: str) -> None:
        """_summary_

        Args:
            message (str): mensagem de informação a ser emitida
        """
        logging.basicConfig(
            format="%(asctime)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            level=logging.INFO,
        )

        logging.info(message)

    @staticmethod
    def warning(message: str) -> None:
        """_summary_

        Args:
            message (str): mensagem de aviso a ser emitida
        """
        logging.basicConfig(
            format="%(asctime)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            level=logging.WARNING,
        )

        colored_message = f"{Fore.YELLOW}[WARNING]{Style.NORMAL} {message}"
        logging.warning(colored_message)

    @staticmethod
    def error(message: str) -> None:
        """_summary_

        Args:
            message (str): mensagem de erro a ser emitida
        """
        logging.basicConfig(
            format="%(asctime)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            level=logging.ERROR,
        )

        colored_message = f"{Fore.RED}[ERROR]{Style.NORMAL} - {message}"
        logging.error(colored_message)
