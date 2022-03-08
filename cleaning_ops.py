import re


def old_price_clean(data_price: str) -> float | None:
    if isinstance(data_price, int):
        return float(data_price)

    elif data_price is None or data_price == '':
        return None

    elif re.search(r"\d", data_price) is None:
        return None

    elif isinstance(data_price, str):
        # data_price = data_price + 'er'
        try:
            index_last_num = re.match('.+([0-9])[^0-9]*$', data_price)  # find the index of the last number
            if index_last_num is not None:
                data_price = data_price[:index_last_num.start(1) + 1]  # deleting all characters down to this index

            index_first_num = re.search(r"\d", data_price)  # find the index of the first number
            data_price = data_price[index_first_num.start():]  # deleting all characters up to this index

            data_price_dots_list = data_price
            dots_list = []
            add_index = 0

            # creating a list with indexes of all dots
            while True:
                i = data_price_dots_list.find(".")
                if i == -1:
                    break
                else:
                    dots_list.append(i + add_index)
                    add_index += i + 1
                    data_price_dots_list = data_price_dots_list[i + 1:]
            for i in dots_list:
                big_float_control = data_price[i + 1: i + 4]
                if len(big_float_control) == 3 and re.search(r"\d", data_price) is not None:
                    data_price = data_price[:i] + " " + data_price[i + 1:]

            data_price = data_price.replace(" ", "")
            try:
                return float(data_price)
            except:
                data_price = data_price.replace(",", ".")
            try:
                return float(data_price)
            except:
                return None
        except:
            return None
    else:
        return None


def immowelt_id_clean(data_immowelt_id: str) -> str | None:
    if isinstance(data_immowelt_id, str):
        if len(data_immowelt_id) >= 7:
            try:
                return str(data_immowelt_id[:7])
            except:
                return None
        else:
            return None
    else:
        return None


def address_clean(data_address: str) -> str | None:     # string because PLZ "09232" can not be integer
    data_address = str(data_address)
    if data_address is None or data_address == '':
        return None
    else:
        index_first_num = re.search(r"\d", data_address)  # find the index of the first number
        if index_first_num is not None:
            data_address = data_address[index_first_num.start():index_first_num.start()+5]
            try:
                return str(data_address)
            except:
                return None
        else:
            return None


def price_clean(data_price: str) -> float | None:

    if isinstance(data_price, int):
        return float(data_price)

    elif data_price is None or data_price == '':
        return None

    elif re.search(r"\d", data_price) is None:
        return None

    elif isinstance(data_price, str):
        return float(re.sub(r'[,.](\d{3})', r'\1', re.sub('[^0-9,.]', '', data_price)).replace(',', '.'))
