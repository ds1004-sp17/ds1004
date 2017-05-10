from datetime import date

class HalfTrip(object):
    def __init__(self, mode, columns, d, loc):
        super(HalfTrip, self).__init__()

        SATURDAY = 5
        passenger_count_index = 2
        distance_index = 3
        total_amt_index = 5

        # such and such is in this location at this moment
        # PU = departure
        # DO = arrival
        self.mode = mode
        self.year = d['year']
        self.month = d['month']
        self.day = d['day']
        self.hour = d['hour']
        self.minute = d['minute']
        self.is_weekend = \
                date(d['year'], d['month'], d['day']).weekday() >= SATURDAY
        self.loc = loc

        self.passengers = int(columns[passenger_count_index])
        self.distance = float(columns[distance_index])
        self.total = float(columns[total_amt_index])
        self.count = 1

    def get_time_key(self):
        return (self.loc,
                self.mode,
                self.hour,
                self.minute,
                self.is_weekend)

    def get_daily_key(self):
        return (self.loc,
                self.mode,
                self.year,
                self.month,
                self.day)

    def get_val(self):
        return (self.passengers,
                self.distance,
                self.total,
                self.count)


