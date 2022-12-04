from Object.I3 import I3
from Object.GBQ import GBQ


def main():
    i3 = I3()
    i3.get_dividend()
    gbq = GBQ()
    gbq.initiate_connection(service_account_file='service-account.json')
    gbq.update_dividend_data(data=i3.dividend_data)

if __name__ == "__main__":
    main()