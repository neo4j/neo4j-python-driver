from timeit import default_timer as timer
from fast_packstream import read as fpsr

def main():
    data = [0xA1, 0x81, 0x41, 0xF8]
    start = timer()
    v = fpsr(data)
    print(v)
    end = timer()
    print_time(start, end)


def print_time(start, end):
    ms = int((end - start) * 1000) 
    print('Time elapsed: {}ms'.format(ms))


if __name__ == '__main__':
    main()