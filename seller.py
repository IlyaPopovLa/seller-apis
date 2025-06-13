import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Отправляет запрос api Ozon и получает список товаров на сайте Ozon..

    Args:
        last_id: id последнего товара из предыдущего запроса.
        client_id: id клиента для аутентификации.
        seller_token: Токен продавца для аутентификации.

    Returns:
        Вернет результат JSON-ответа в виде словаря, содержащего список товаров.

    Raises:
        requests.exceptions.RequestException:
    """

    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает артикулы товаров на сайте Ozon.

    Args:
        client_id: id клиента для аутентификации.
        seller_token: Токен продавца для аутентификации.

    Returns:
        Возвращает cписок артикулов товаров.

    Raises:
        requests.exceptions.RequestException:
    """

    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров на сайте Ozon.

    Args:
        prices: Список цен для обновления.
        client_id: id клиента для аутентификации.
        seller_token: Токен продавца для аутентификации.

    Returns:
        Возвращяет результат в виде словаря.

    Raises:
        requests.exceptions.RequestException:
    """

    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров на сайте Ozon.

    Args:
        stocks: Список остатков для обновления.
        client_id: id клиента для аутентификации.
        seller_token: Токен продавца для аутентификации.

    Returns:
        Возвращает результат в виде словаря.

    Raises:
        requests.exceptions.RequestException: Если запрос к API не удался.
    """

    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает файл с остатками товаров с сайта Casio.

    Returns:
        Возвращает список остатков товаров в виде словаря.

    Raises:
        requests.exceptions.RequestException:
        FileNotFoundError:
    """

    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создает список остатков товаров для обновления.

    Args:
        watch_remnants: Список остатков товаров.
        offer_ids: Список артикулов товаров.

    Returns:
        Возвращает список остатков товаров для обновления.

    Raises:
        ValueError: Если артикул товара не найден.
    """

    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен товаров для обновления.

    Args:
        watch_remnants: Список остатков товаров.
        offer_ids: Список артикулов товаров.

    Returns:
        Возвращает список цен товаров для обновления.

    Raises:
        ValueError: Если артикул товара не найден.
    """

    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует значение цена из строки в числа до точки.

    Args:
        price (str): Строка, представляющая цену, с символами и десятичными числами.

    Returns:
        str: Возвращает значение только цифры до точки.

    Examples:
        price_conversion("19'990.00 руб.") >>> '5990'

    Raises:
        requests.exceptions.RequestException:
    """

    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на части по n элементов.

    Args:
        lst: Список, который необходимо разделить.
        n: Количество элементов в каждой части.

    Examples:
        Возвращает разделенный список [[1, 2], [3, 4], [5, 6]]
    """

    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Получает список артикулов товаров и обновляет цены на сайте Ozon.

    Args:
        watch_remnants: Список остатков товаров.
        client_id: id клиента для аутентификации.
        seller_token: Токен продавца для аутентификации.

    Returns:
        Возвращает список обновленных цен товаров.

    Raises:
       requests.exceptions.RequestException:
    """

    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Получает список артикулов товаров и обновляет их остатки на сайте Ozon.

    Args:
        watch_remnants: Список остатков товаров.
        client_id: id клиента для аутентификации.
        seller_token: Токен продавца для аутентификации.

    Returns:
        not_empty: Список товаров с ненулевыми остатками.
        stocks: Список всех товаров с остатками.

    Raises:
        requests.exceptions.RequestException:
    """

    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция для обновления остатков и цен товаров.

   Использует переменные окружения для получения токена продавца и идентификатора клиента.

    Raises:
        requests.exceptions.RequestException: Если запрос к API не удался.
        Exception: Если произошла ошибка при обработке данных.
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
