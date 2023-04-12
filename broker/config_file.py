import time
def log_file_dump(port, log_message):
    f = open('config.txt', 'a')
    f_1 = open('../broker1/config.txt', 'a')
    f_2 = open('../broker2/config.txt', 'a')

    f.write('\n')
    f.write(f' {time.strftime("%H:%M:%S", time.localtime())}')
    f.write(f'\t{port} : {log_message}')

    f_1.write('\n')
    f_1.write(f' {time.strftime("%H:%M:%S", time.localtime())}')
    f_1.write(f'\t{port} : {log_message}')

    f_2.write('\n')
    f_2.write(f' {time.strftime("%H:%M:%S", time.localtime())}')
    f_2.write(f'\t{port} : {log_message}')

    f.close()
    f_1.close()
    f_2.close()