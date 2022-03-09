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
        data_immowelt_id = data_immowelt_id.replace(' ', '')
        if len(data_immowelt_id) != 7:
            return None
        else:
            return str(data_immowelt_id)
    else:
        return None


def address_clean(data_address: str) -> int | None:     # string because PLZ "09232" can not be integer
    if isinstance(data_address, int):
        return data_address
    elif isinstance(data_address, str):
        if data_address == '':
            return None
        else:
            index_first_num = re.search(r"\d", data_address)  # find the index of the first number
            if index_first_num is not None:
                data_address = data_address[index_first_num.start():index_first_num.start()+5]
                try:
                    return int(data_address)
                except:
                    return None
            else:
                return None
    else:
        return None


def price_clean(data_price: str) -> float | None:
    if isinstance(data_price, int):
        return float(data_price)

    elif isinstance(data_price, str):
        if (data_price == '') or (re.search(r"\d", data_price) is None):
            return None
        else:
            return float(re.sub(r'[,.](\d{3})', r'\1', re.sub('[^0-9,.]', '', data_price)).replace(',', '.'))
    else:
        return None


def immonet_id_clean(data_immonet_id: str) -> int | None:
    if isinstance(data_immonet_id, int):
        return int(data_immonet_id)
    elif isinstance(data_immonet_id, str):
        if (data_immonet_id == '') or (re.search(r"\d", data_immonet_id) is None):
            return None
        else:
            return int(re.sub(r'[,.](\d{3})', r'\1', re.sub('[^0-9,.]', '', data_immonet_id)).replace(',', '.'))
    else:
        return None


def seller_id_clean(data_seller_id: str) -> str | None:
    if isinstance(data_seller_id, str):
        if data_seller_id == '' or len(data_seller_id) <= 20:
            return None
        else:
            return str(data_seller_id)[20:]
    else:
        return None