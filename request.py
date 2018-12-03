import time


class Request(object):

    def __init__(self, rtype, args, sendBackTo, previous_request=None):

        # timestamp the request, need this to reference it later
        self.time_created = time.time()
        self.type = rtype
        self.sendBackTo = sendBackTo
        self.responses = {}
        self.previous_request = previous_request
        self.responded=False

        if rtype == 'put':
            self.forwardedTo = None
            self.hash = args[0]
            self.value = args[1]
            self.context = args[2]
        elif rtype == 'get':
            self.forwardedTo = None
            self.hash = args
            self.value = None
            self.context = None
        elif rtype == 'for_put':
            self.forwardedTo = args[0]
            self.hash = args[1]
            self.value = args[2]
            self.context = args[3]
        else:  # rtype== 'for_get'
            self.forwardedTo = args[0]
            self.hash = args[1]
            self.value = None
            self.context = None
