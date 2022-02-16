import re


def old_str_to_float(data_price: str) -> float | None:
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


def test_price_to_float():
    assert old_str_to_float("1.344") == 1344
