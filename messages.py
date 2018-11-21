import pickle


def client_message(user_input):
    return pickle.dumps((0, user_input))


def _unpack_message(data):
    data_tuple = pickle.loads(data)
    return data_tuple
