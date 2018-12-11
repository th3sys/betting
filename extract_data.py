import os
import random
import tempfile
import zipfile
import time
import concurrent.futures


def timer(f):
    def inner(*args, **kwargs):
        try:
            t0 = time.time()
            return f(*args, **kwargs)
        finally:
            t1 = time.time()
            print(f.__name__, 'TOOK', t1 - t0)
    return inner


@timer
def f1(fn, dest):
    # t0=time.time()
    with open(fn, 'rb') as f:
        zf = zipfile.ZipFile(f)
        zf.extractall(dest)
    # t1=time.time()

    total = 0
    for root, dirs, files in os.walk(dest):
        for file_ in files:
            fn = os.path.join(root, file_)
            total += _count_file(fn)
    # t2=time.time()
    # print(t1-t0)
    # print(t2-t1)
    return total


@timer
def f1b(fn, dest):  # slight optimizations
    with open(fn, 'rb') as f:
        zf = zipfile.ZipFile(f)
        zf.extractall(dest, members=zf.infolist())
        namelist = zf.namelist()

    total = 0
    for name in namelist:
        fn = os.path.join(dest, name)
        total += _count_file(fn)
    return total


def _count_file(fn):
    with open(fn, 'rb') as f:
        return _count_file_object(f)


def _count_file_object(f):
    total = 0
    for line in f:
        total += len(line)
    return total


@timer
def f2(fn, dest):

    def unzip_member(zf, member, dest):
        zf.extract(member, dest)
        fn = os.path.join(dest, member.filename)
        return _count_file(fn)

    with open(fn, 'rb') as f:
        zf = zipfile.ZipFile(f)
        futures = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for member in zf.infolist():
                futures.append(
                    executor.submit(
                        unzip_member,
                        zf,
                        member,
                        dest,
                    )
                )
            total = 0
            for future in concurrent.futures.as_completed(futures):
                total += future.result()
    return total


def unzip_member_f3(zip_filepath, filename, dest):
    with open(zip_filepath, 'rb') as f:
        zf = zipfile.ZipFile(f)
        zf.extract(filename, dest)
    fn = os.path.join(dest, filename)
    return _count_file(fn)


@timer
def f3(fn, dest):
    with open(fn, 'rb') as f:
        zf = zipfile.ZipFile(f)
        futures = []
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for member in zf.infolist():
                futures.append(
                    executor.submit(
                        unzip_member_f3,
                        fn,
                        member.filename,
                        dest,
                    )
                )
            total = 0
            for future in concurrent.futures.as_completed(futures):
                total += future.result()
    return total



# @timer
# def f4(fn, dest):  # extract some to disk only
#     total = 0
#     with open(fn, 'rb') as f:
#         zf = zipfile.ZipFile(f)
#         for member in zf.infolist():
#             if random.random() > 0.5:
#                 zf.extract(member, dest)
#                 fn = os.path.join(dest, member.filename)
#                 total += _count_file(fn)
#     return total


@timer
def f5(fn, *_):

    total = 0

    def unzip_member(zf, name):
        with zf.open(name) as member_bytes:
            return _count_file_object(member_bytes)

    with open(fn, 'rb') as f:
        zf = zipfile.ZipFile(f)
        futures = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for member in zf.infolist():
                futures.append(
                    executor.submit(
                        unzip_member,
                        zf,
                        member.filename,
                    )
                )
            total = 0
            for future in concurrent.futures.as_completed(futures):
                total += future.result()
    return total



def unzip_member_f6(zip_filepath, name):
    with open(zip_filepath, 'rb') as f:
        zf = zipfile.ZipFile(f)
        with zf.open(name) as member_bytes:
            return _count_file_object(member_bytes)

@timer
def f6(fn, *_):

    with open(fn, 'rb') as f:
        zf = zipfile.ZipFile(f)
        futures = []
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for member in zf.infolist():
                futures.append(
                    executor.submit(
                        unzip_member_f6,
                        fn,
                        member.filename,
                    )
                )
            total = 0
            for future in concurrent.futures.as_completed(futures):
                total += future.result()
    return total


@timer
def f7(fn, *_):
    with open(fn, 'rb') as f:
        zf = zipfile.ZipFile(f)
        total = 0
        for member in zf.infolist():
            with zf.open(member.filename) as f:
                total += _count_file_object(f)
    return total


#
# def run_multiple(fn):
#     functions = [f1, f7]
#     times = {x.__name__:[] for x in functions}
#     for i in range(5):
#         for f in functions:
#             with tempfile.TemporaryDirectory() as tmpdir:
#                 t0 = time.time()
#                 assert f(fn, tmpdir) == 193566145
#                 t1 = time.time()
#                 print(f.__name__, t1 - t0)
#                 times[f.__name__].append(t1 - t0)
#
#     for f_name in times:
#         print(f_name, min(times[f_name]))
#
#         # print(f1(fn, tmpdir))


def run(fn):
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f1(fn, tmpdir))
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f1b(fn, tmpdir))
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f2(fn, tmpdir))
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f3(fn, tmpdir))
    print(f5(fn))
    print(f6(fn))
    print(f7(fn))


if __name__ == '__main__':
    # run('/var/folders/1x/2hf5hbs902q54g3bgby5bzt40000gn/T/massive-symbol-zips/symbols-2017-11-27T14_15_30.zip')
    run('files/data.tar')
    # run_multiple('/var/folders/1x/2hf5hbs902q54g3bgby5bzt40000gn/T/massive-symbol-zips/symbols-2017-11-27T14_15_30.zip')
